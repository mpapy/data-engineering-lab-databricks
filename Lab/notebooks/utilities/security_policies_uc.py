# Databricks notebook source
# ===========================================================
# LAB ÚKOL: Maskování a Row Filtering
# Cíl: Vytvořit vlastní maskovací funkce a aplikovat je na tabulky
# ===========================================================

# COMMAND ----------

# KROK 1: ENVIRONMENT SETUP
# ÚKOL:
# - Získej aktuální prostředí pomocí dbutils.widgets.get("pipeline.env")
# - Nastav proměnné `catalog` a `schema`
# - Použij spark.sql pro přepnutí na správný katalog a schéma

# env = ...
# catalog = ...
# schema = ...
# spark.sql(...)
# spark.sql(...)

# COMMAND ----------

# KROK 2: VYTVOŘENÍ MASKOVACÍCH FUNKCÍ
# ÚKOL:
# - Pomocí `CREATE FUNCTION` vytvoř dvě funkce:
#   1. mask_email(email STRING) → zobrazí email jen skupině 'viewers'
#   2. mask_income(income INT) → zobrazí příjem jen skupině 'viewers'

# spark.sql(f""" ... """)

# COMMAND ----------

# KROK 3: APLIKACE MASKOVACÍCH FUNKCÍ
# ÚKOL:
# - Pro každou tabulku a sloupec z níže uvedeného seznamu
#   použij ALTER TABLE a APPLY MASK

# masking_targets = [
#     ("dim_agents_mask", "email", "mask_email"),
#     ("dim_customers_mask", "income", "mask_income")
# ]

# for table_name, column_name, function_name in masking_targets:
#     ...
#     spark.sql(sql_stmt)

# COMMAND ----------

# KROK 4: VYTVOŘENÍ ROW FILTER FUNKCE
# ÚKOL:
# - Vytvoř funkci `rf_west_region(region STRING)`
# - Vrací `TRUE`, pokud je uživatel členem 'data_admins', jinak jen pokud je region 'West'

# spark.sql(f""" ... """)

# COMMAND ----------

# KROK 5: APLIKACE ROW FILTERU
# ÚKOL:
# - Aplikuj vytvořený row filter na tabulku `dim_agents_mask` a sloupec `region`

# row_filter_target = [
#     ("dim_agents_mask", "region", "rf_west_region")
# ]

# for table_name, column_name, function_name in row_filter_target:
#     ...
#     spark.sql(sql_stmt)
