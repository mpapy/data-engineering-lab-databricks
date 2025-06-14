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
from pyspark.sql.functions import current_timestamp, col, to_date, regexp_extract

# Dostanu DEV nebo PROD
env = spark.conf.get("pipeline.env") 
catalog = "principal_lab_db"
bronze_schema = f"{env}_bronze"

@dlt.table(name="customers_bronze", comment="Bronze table for raw Customers data")
def customers_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/customers"
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
                               "yyyy/MM/dd"))
           )

@dlt.table(name="agents_bronze", comment="Bronze table for raw Agents data")
def agents_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/agents"
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
            .option("header", "true").load(path)
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("snapshot_date",
                        to_date(regexp_extract(col("source_file"),
                                               r'/operational_data/[^/]+/(\d{4}/\d{2}/\d{2})/', 1),
                               "yyyy/MM/dd"))
           )

@dlt.table(name="products_bronze", comment="Bronze table for raw Products data")
def products_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/products"
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
            .option("header", "true").load(path)
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("snapshot_date",
                        to_date(regexp_extract(col("source_file"),
                                               r'/operational_data/[^/]+/(\d{4}/\d{2}/\d{2})/', 1),
                               "yyyy/MM/dd"))
           )

@dlt.table(name="policies_bronze", comment="Bronze table for raw Policies data")
def policies_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/policies"
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
            .option("header", "true").load(path)
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("snapshot_date",
                        to_date(regexp_extract(col("source_file"),
                                               r'/operational_data/[^/]+/(\d{4}/\d{2}/\d{2})/', 1),
                               "yyyy/MM/dd"))
           )

@dlt.table(name="claims_bronze", comment="Bronze table for raw Claims data")
def claims_bronze():
    path = f"/Volumes/principal_lab_db/landing/operational_data/claims"
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
            .option("header", "true").load(path)
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn("snapshot_date",
                        to_date(regexp_extract(col("source_file"),
                                               r'/operational_data/[^/]+/(\d{4}/\d{2}/\d{2})/', 1),
                               "yyyy/MM/dd"))
           )

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

