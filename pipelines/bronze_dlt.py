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
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DateType

# --- ENVIRONMENT CONFIGURATION ---
SCHEMA = spark.conf.get("pipeline.schema", "dev_bronze")
CATALOG = "principal_lab_db"
RAW_PATH = "/Volumes/principal_lab_db/landing/operational_data"  # raw input path  # path to Bronze CSVs

print(f"🏷️ Running DLT pipeline in schema: {SCHEMA}")

def full_name(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"

# Enrichment: add audit columns and snapshot_date
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
                F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4}/\d{2}/\d{2})/", 1),
                "yyyy/MM/dd"
            )
        )
    return df2

# JSON schemas
metadata_schema = StructType([
    StructField("languages", ArrayType(StringType()), True),
    StructField("certifications", ArrayType(StringType()), True)
])
preferences_schema = StructType([
    StructField("contact_methods", ArrayType(StringType()), True),
    StructField("preferred_language", StringType(), True),
    StructField("newsletter_opt_in", BooleanType(), True)
])
coverages_schema = ArrayType(StringType())

# --- BRONZE TABLES ---

@dlt.table(name=full_name("agents"), comment="Bronze: agents snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def bronze_agents():
    df = spark.read \
        .option("header", True) \
        .option("multiLine", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(f"{RAW_PATH}/agents/*/*/*/*.csv")
    df = enrich(df, snapshot=True)
    df = df.withColumn("metadata", F.from_json("metadata", metadata_schema))
    df = df.withColumn("languages", F.col("metadata.languages")) \
           .withColumn("certifications", F.col("metadata.certifications"))
    return df.drop("metadata")

@dlt.table(name=full_name("customers"), comment="Bronze: customers snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def bronze_customers():
    df = spark.read \
        .option("header", True) \
        .option("multiLine", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(f"{RAW_PATH}/customers/*/*/*/*.csv")
    df = enrich(df, snapshot=True)
    df = df.withColumn("preferences", F.from_json("preferences", preferences_schema))
    df = df.withColumn("contact_methods", F.col("preferences.contact_methods")) \
           .withColumn("preferred_language", F.col("preferences.preferred_language")) \
           .withColumn("newsletter_opt_in", F.col("preferences.newsletter_opt_in")) \
           .drop("preferences")
    return df

@dlt.table(name=full_name("policies"), comment="Bronze: policies snapshot")
@dlt.expect_or_drop("valid_snapshot", F.col("snapshot_date").isNotNull())
def bronze_policies():
    df = spark.read \
        .option("header", True) \
        .option("multiLine", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(f"{RAW_PATH}/policies/*/*/*/*.csv")
    df = enrich(df, snapshot=True)
    df = df.withColumn("coverages", F.from_json(F.col("coverages"), coverages_schema))
    return df

@dlt.table(name=full_name("claims"), comment="Bronze: claims reference")
def bronze_claims():
    df = spark.read.option("header", True).csv(f"{RAW_PATH}/claims/*.csv")
    return enrich(df, snapshot=False)

@dlt.table(name=full_name("products"), comment="Bronze: products reference")
def bronze_products():
    df = spark.read.option("header", True).csv(f"{RAW_PATH}/products/*.csv")
    return enrich(df, snapshot=False)

@dlt.table(name=full_name("premium_transactions"), comment="Bronze: premium transactions")
def bronze_premiums():
    df = spark.read.option("header", True).csv(f"{RAW_PATH}/premium/*.csv")
    return enrich(df, snapshot=False)
