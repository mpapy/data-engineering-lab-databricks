# data-engineering-lab-databricks

This repository contains sample data and scripts for building data pipelines on Databricks using the Bronze-Silver-Gold architecture.  
The CSV files in the root directory represent snapshots of insurance-related tables.

## Bronze ingestion with Delta Live Tables
The file [`pipelines/bronze_dlt.py`](pipelines/bronze_dlt.py) defines a Delta Live Tables (DLT) pipeline that ingests the raw CSV files from a Databricks volume into Bronze tables.
By default it reads from `/Volumes/principal_lab_dbx/landing/external_data_volume`. The location can be overridden using the `raw_path` pipeline configuration when the pipeline is created.
The target catalog and schema can also be set using the `catalog` and `schema` configurations, making it easy to run the same pipeline against different environments (for example `DEV` vs. `PROD`).
Each table is loaded using simple `spark.read` operations and is augmented with the source filename, ingestion timestamp and optional snapshot date for lineage.

### Expected primary keys
- **Agents** – `agent_id`
- **Customers** – `customer_id`
- **Policies** – `policy_id`
- **Products** – `product_id`
- **Claims** – `claim_id`

These keys uniquely identify records within their respective tables and can be used for downstream merges or upserts.
