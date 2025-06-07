"""
Ensure idempotent runs of your DLT pipeline in DEV:

1. **Development Mode**: In the Databricks UI toggle your pipeline to Development Mode. This will drop and re-create managed tables on every pipeline update.

2. **Alternate Schema**: Use separate `dev_bronze` and `prod_bronze` schemas to avoid conflicts.

3. **Manual Cleanup (Once)**: If needed, run the following SQL commands just one time to clear stale managed tables:
   DROP TABLE IF EXISTS principal_lab_db.dev_bronze.agents;
   DROP TABLE IF EXISTS principal_lab_db.dev_bronze.customers;
   DROP TABLE IF EXISTS principal_lab_db.dev_bronze.policies;
   DROP TABLE IF EXISTS principal_lab_db.dev_bronze.claims;
   DROP TABLE IF EXISTS principal_lab_db.dev_bronze.products;
"""
import dlt
from pyspark.sql import functions as F

# --- Environment detection ---
# Set via pipeline configuration: 'pipeline.schema' must be 'dev_bronze' or 'prod_bronze'
SCHEMA = spark.conf.get("pipeline.schema", "dev_bronze")
print(f"🏷️ Running DLT pipeline in schema: {SCHEMA}")

# --- Configuration ---
RAW_PATH = "/Volumes/principal_lab_db/landing/operational_data"
CATALOG  = "principal_lab_db"

# Helper to form full table identifiers
def full_name(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"

# Enrichment: add audit columns and optional snapshot_date
def enrich(df, snapshot: bool):
    df2 = (
        df
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    if snapshot:
        df2 = df2.withColumn(
            "snapshot_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"), r"/(\d{4}/\d{2}/\d{2})/", 1
                ),
                "yyyy/MM/dd"
            )
        )
    return df2

# --- DLT Table Definitions ---

@dlt.table(name=full_name("agents"), comment="Bronze: agents snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def agents_bronze():
    return enrich(
        spark.read.option("header", True)
            .csv(f"{RAW_PATH}/agents/*/*/*/*.csv"),
        snapshot=True
    )

@dlt.table(name=full_name("customers"), comment="Bronze: customers snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def customers_bronze():
    return enrich(
        spark.read.option("header", True)
            .csv(f"{RAW_PATH}/customers/*/*/*/*.csv"),
        snapshot=True
    )

@dlt.table(name=full_name("policies"), comment="Bronze: policies snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def policies_bronze():
    return enrich(
        spark.read.option("header", True)
            .csv(f"{RAW_PATH}/policies/*/*/*/*.csv"),
        snapshot=True
    )

@dlt.table(name=full_name("claims"), comment="Bronze: claims reference")
def claims_bronze():
    return enrich(
        spark.read.option("header", True)
            .csv(f"{RAW_PATH}/claims/*.csv"),
        snapshot=False
    )

@dlt.table(name=full_name("products"), comment="Bronze: products reference")
def products_bronze():
    return enrich(
        spark.read.option("header", True)
            .csv(f"{RAW_PATH}/products/*.csv"),
        snapshot=False
    )
