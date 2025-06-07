import dlt
from pyspark.sql import functions as F

# Default location of the CSV files on Databricks Volumes. The path can be
# overridden with the `raw_path` pipeline configuration when creating the
# pipeline.
RAW_PATH = spark.conf.get(
    "raw_path",
    "/Volumes/principal_lab_dbx/landing/external_data_volume",
)

# Tables are written to the DEV catalog inside the `bronze` schema. Update the
# catalog or schema by changing these constants if needed.
CATALOG = spark.conf.get("catalog", "DEV")
SCHEMA = spark.conf.get("schema", "bronze")


def full_name(table: str) -> str:
    """Return the fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{table}"


def ingest(pattern: str, snapshot: bool = False):
    """Load CSV files and enrich with metadata."""
    df = (
        spark.read.format("csv")
        .option("header", True)
        .load(f"{RAW_PATH}/{pattern}")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    if snapshot:
        df = df.withColumn(
            "snapshot_date",
            F.to_date(
                F.regexp_extract(F.input_file_name(), r"/(\d{4}/\d{2}/\d{2})/", 1),
                "yyyy/MM/dd",
            ),
        )
    return df

@dlt.table(name=full_name("agents"), comment="Raw agent snapshots")
def agents_bronze():
    return ingest("agents/*/*/*/*.csv", snapshot=True)

@dlt.table(name=full_name("customers"), comment="Raw customer snapshots")
def customers_bronze():
    return ingest("customers/*/*/*/*.csv", snapshot=True)

@dlt.table(name=full_name("policies"), comment="Raw policy snapshots")
def policies_bronze():
    return ingest("policies/*/*/*/*.csv", snapshot=True)

@dlt.table(name=full_name("products"), comment="Reference products table")
def products_bronze():
    return ingest("products/*.csv", snapshot=False)

@dlt.table(name=full_name("claims"), comment="Claims table")
def claims_bronze():
    return ingest("claims/*.csv", snapshot=False)
