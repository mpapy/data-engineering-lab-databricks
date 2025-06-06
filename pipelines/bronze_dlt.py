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
CATALOG = "DEV"
SCHEMA = "bronze"


def full_name(table: str) -> str:
    """Return the fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{table}"

@dlt.table(name=full_name("agents"), comment="Raw agent snapshots")
def agents_bronze():
    return (
        spark.read.format("csv")
            .option("header", True)
            .load(f"{RAW_PATH}/agents/*.csv")
            .withColumn("ingest_file", F.input_file_name())
    )

@dlt.table(name=full_name("customers"), comment="Raw customer snapshots")
def customers_bronze():
    return (
        spark.read.format("csv")
            .option("header", True)
            .load(f"{RAW_PATH}/customers/*.csv")
            .withColumn("ingest_file", F.input_file_name())
    )

@dlt.table(name=full_name("policies"), comment="Raw policy snapshots")
def policies_bronze():
    return (
        spark.read.format("csv")
            .option("header", True)
            .load(f"{RAW_PATH}/policies/*.csv")
            .withColumn("ingest_file", F.input_file_name())
    )

@dlt.table(name=full_name("products"), comment="Reference products table")
def products_bronze():
    return (
        spark.read.format("csv")
            .option("header", True)
            .load(f"{RAW_PATH}/products/products.csv")
            .withColumn("ingest_file", F.input_file_name())
    )

@dlt.table(name=full_name("claims"), comment="Claims table")
def claims_bronze():
    return (
        spark.read.format("csv")
            .option("header", True)
            .load(f"{RAW_PATH}/claims/claims.csv")
            .withColumn("ingest_file", F.input_file_name())
    )
