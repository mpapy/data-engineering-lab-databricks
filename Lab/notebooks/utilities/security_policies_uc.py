# Databricks notebook source
# MAGIC %md
# MAGIC ## Úvod
# MAGIC V tomto cvičení si vyzkoušíte, jak pracovat s bezpečnostními politikami v
# MAGIC Unity Catalogu. Každý krok je popsán níže a váš úkol je doplnit chybějící
# MAGIC kód.

# COMMAND ----------

# ——————————————————————————————
#  Nastavení prostředí
# ——————————————————————————————
# TODO: vytvořte widget `pipeline_env` a získejte hodnotu prostředí
env = None  # dbutils.widgets.get("pipeline_env")

catalog = "principal_lab_db"
schema = f"{env}_silver"

# TODO: nastavte aktivní katalog a schéma pomocí spark.sql


# COMMAND ----------

# MAGIC %md
# MAGIC ### Krok 1: Vytvoření funkcí pro maskování
# MAGIC Pomocí SQL vytvořte dvě funkce `mask_email` a `mask_income` v katalogu
# MAGIC a schématu nastaveném výše. Funkce budou později použity v maskovacích
# MAGIC politikách.

# COMMAND ----------

# ——————————————————————————————
#  CREATE POLICIES (email, income)
# ——————————————————————————————
# TODO: definujte SQL příkazy pro vytvoření funkcí mask_email a mask_income


# COMMAND ----------

# MAGIC %md
# MAGIC ### Krok 2: Maskování sloupců
# MAGIC Připravte seznam tabulek a sloupců, na které bude aplikována maskovací
# MAGIC politika. Následně pro každý záznam proveďte příslušný SQL příkaz.

# COMMAND ----------

# ——————————————————————————————
#  APPLY MASKING POLICIES TO TABLES
# ——————————————————————————————
# Platí policy na konkrétní sloupce tabulek
masking_targets = [
    ("dim_agents_mask", "email", "mask_email"),
    ("dim_customers_mask", "income", "mask_income")
]

# TODO: pomocí cyklu aplikujte maskovací funkce na uvedené tabulky a sloupce

# COMMAND ----------

# MAGIC %md
# MAGIC ### Krok 3: Vytvoření funkce pro row filtering
# MAGIC Vytvořte SQL funkci, která bude sloužit k filtrování řádků podle regionu.

# COMMAND ----------

# TODO: definujte funkci `rf_west_region` pomocí příkazu CREATE OR REPLACE FUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Krok 4: Aplikace row filteru na tabulky
# MAGIC Připravte seznam tabulek, pro které chcete nastavit row filter, a pomocí
# MAGIC SQL příkazu je aplikujte.

# COMMAND ----------

# ——————————————————————————————
#  APPLY ROW FILTER POLICIES TO TABLES
# ——————————————————————————————
# Platí policy na konkrétní sloupce tabulek
row_filter_target = [
    ("dim_agents_mask", "region", "rf_west_region"),
]

# TODO: pomocí cyklu nastavte row filter pro uvedené tabulky

