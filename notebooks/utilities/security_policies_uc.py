# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# ——————————————————————————————
#  ENV SETUP
# ——————————————————————————————
#env = spark.conf.get("pipeline.env") 
catalog = "principal_lab_db"
schema = f"{env}_silver"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
qualified = lambda name: f"{catalog}.{schema}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vytváření funkcí pro maskování

# COMMAND ----------

# ——————————————————————————————
#  CREATE POLICIES (email, income)
# ——————————————————————————————
# Masking policy využívá dříve vytvořenou funkci a říká, jak se má daný sloupec maskovat
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_email(email STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('viewers') THEN email
    ELSE '*** REDACTED ***'
END;
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_income(income INTEGER)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('viewers') THEN CAST(income AS STRING)
    ELSE '-1'
END;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Maskovávání sloupců

# COMMAND ----------

# ——————————————————————————————
#  APPLY MASKING POLICIES TO TABLES
# ——————————————————————————————
# Platí policy na konkrétní sloupce tabulek
masking_targets = [
    ("dim_agents_mask", "email", "mask_email"),
    ("dim_customers_mask", "income", "mask_income")
]

for table_name, column_name, function_name in masking_targets:
    full_table = f"{catalog}.{schema}.{table_name}"
    full_function = f"{catalog}.{schema}.{function_name}"
    sql_stmt = f"""
    ALTER TABLE {full_table}
    ALTER COLUMN {column_name}
    SET MASK {full_function}
    """
    spark.sql(sql_stmt)
