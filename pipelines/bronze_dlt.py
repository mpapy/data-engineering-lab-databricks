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

import dlt
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Get environment and define source/target schema names
env = spark.conf.get("pipeline.env")
catalog = "principal_lab_db"
bronze_schema = f"{env}_bronze"
silver_schema = f"{env}_silver"

# (Optionally read lookup table for config – here we manually specify the config for clarity)
keys_map = {
    "customers": ["customer_id"],   # primary key for each table
    "agents":    ["agent_id"],
    "products":  ["product_id"],
    "policies":  ["policy_id"],
    "claims":    ["claim_id"]
}
scd_type_map = {
    "customers": "SCD2",   # dimension table with Type 2 changes
    "agents":    "SCD1",   # dimension table with Type 1 changes
    "products":  "SCD1",   # dimension (assume Type 1)
    "policies":  None,     # fact table (no SCD)
    "claims":    None      # fact table (no SCD)
}

# SCD2 Dimension Example: Customers (Dimension with historical tracking)
@dlt.view(name="customers_cleaned", comment="Streaming cleaned view of Customers data")
def customers_cleaned():
    # Read bronze as a stream
    return spark.readStream.table(f"{catalog}.{bronze_schema}.customers_bronze")  # :contentReference[oaicite:8]{index=8}

# Create empty target table for SCD2 output (customers_scd2), with schema managed by DLT
dlt.create_streaming_table(
    name="customers_scd2",
    comment="Silver SCD2 dimension table for Customers (historical changes)",
    table_properties={"quality": "silver"}  # example property, Unity Catalog will store in silver schema
)
# Apply CDC flow to capture Type 2 changes into customers_scd2
dlt.create_auto_cdc_flow(
    target="customers_scd2",
    source="customers_cleaned",
    keys=keys_map["customers"],            # e.g. ["customer_id"]
    sequence_by="snapshot_date",           # use snapshot_date as the sequence for ordering changes
    stored_as_scd_type=2                   # SCD Type 2 (keeps history)
)
 
# SCD1 Dimension Example: Agents (Dimension with no history, latest state only)
@dlt.view(name="agents_cleaned", comment="Streaming cleaned view of Agents data")
def agents_cleaned():
    return spark.readStream.table(f"{catalog}.{bronze_schema}.agents_bronze")

@dlt.table(name="agents_silver", comment="Silver table for latest Agents (SCD1)")
def agents_silver():
    df = dlt.read("agents_cleaned")  # read the cleaned stream (as a DataFrame)
    # For each agent_id, keep only the record with max snapshot_date (latest snapshot)
    window = Window.partitionBy("agent_id").orderBy(col("snapshot_date").desc())
    latest_df = df.withColumn("rn", row_number().over(window)) \
                  .filter(col("rn") == 1) \
                  .drop("rn")
    return latest_df

# SCD1 Dimension Example: Products (assume Products behave as Type 1 dimension)
@dlt.view(name="products_cleaned", comment="Streaming cleaned view of Products data")
def products_cleaned():
    return spark.readStream.table(f"{catalog}.{bronze_schema}.products_bronze")

@dlt.table(name="products_silver", comment="Silver table for latest Products (SCD1)")
def products_silver():
    df = dlt.read("products_cleaned")
    window = Window.partitionBy("product_id").orderBy(col("snapshot_date").desc())
    latest_df = df.withColumn("rn", row_number().over(window)) \
                  .filter(col("rn") == 1) \
                  .drop("rn")
    return latest_df

# Fact Table Example: Policies (Fact data with no SCD – append only)
@dlt.table(name="policies_silver", comment="Silver table for Policies fact data")
def policies_silver():
    return spark.readStream.table(f"{catalog}.{bronze_schema}.policies_bronze")

# Fact Table Example: Claims (Fact data with no SCD – append only)
@dlt.table(name="claims_silver", comment="Silver table for Claims fact data")
def claims_silver():
    return spark.readStream.table(f"{catalog}.{bronze_schema}.claims_bronze")

@dlt.table(name="premium_transactions_bronze", comment="Bronze table for raw Premium Transactions data")
def premium_transactions_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/premium"
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(path)
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("snapshot_date",
                        to_date(regexp_extract(col("source_file"),
                                               r'/operational_data/[^/]+/(\d{4}/\d{2}/\d{2})/', 1),
                                "yyyy/MM/dd")))

