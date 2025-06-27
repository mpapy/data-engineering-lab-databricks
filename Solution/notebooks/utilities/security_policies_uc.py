# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# ——————————————————————————————
#  ENV SETUP
# ——————————————————————————————
#dbutils.widgets.text("pipeline_env", "test_marek")

env = dbutils.widgets.get("pipeline.env")

catalog = "principal_lab_db"
schema = f"{env}_silver"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vytvareni funkci pro row filtering

# COMMAND ----------

spark.sql(f""" 
          CREATE OR REPLACE FUNCTION {catalog}.{schema}.rf_west_region(region STRING)
          RETURN IF(IS_ACCOUNT_GROUP_MEMBER('viewers'), true, region='West');
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplikace row filteru na tabulky

# COMMAND ----------

# ——————————————————————————————
#  APPLY ROW FILTER POLICIES TO TABLES
# ——————————————————————————————
# Platí policy na konkrétní sloupce tabulek
row_filter_target = [
    ("dim_agents_mask","region","rf_west_region"),
]

for table_name, column_name, function_name in row_filter_target:
    full_table = f"{catalog}.{schema}.{table_name}"
    full_function = f"{catalog}.{schema}.{function_name}"
    sql_stmt = f"""
    ALTER TABLE {full_table}
    SET ROW FILTER {full_function}
    ON ({column_name})
    """
    spark.sql(sql_stmt)
