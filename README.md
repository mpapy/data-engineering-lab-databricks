# Data Engineering Lab – Databricks

Tento repozitář slouží jako praktické prostředí pro budování moderní datové platformy pomocí **Databricks**, **Delta Live Tables**, **Unity Catalogu**, **streamingu** a **CI/CD principů**. Cílem je vytvořit Lab pro naučení se Databricks.

---

## Jak postupovat (High-Level Workflow)

### 1. Příprava prostředí - všechno popsáno v OneNote
- Ujisti se, že máš přístup do **Databricks workspace** s aktivovaným **Unity Catalogem**
- Povol přístup do příslušného **katalogu** (`principal_lab_db`), kde se pipeline spouštějí
---

### 2. Vývoj transformací - https://www.databricks.com/product/data-engineering/delta-live-tables, https://www.databricks.com/discover/pages/getting-started-with-delta-live-tables, https://docs.databricks.com/gcp/en/delta-live-tables/load
- Novou DLT transformaci napiš v adresáři (./pipelines)
  - Každá tabulka = samostatná funkce s `@dlt.table`
  - Pro SCD použij `dlt.apply_changes()`
- Používej `spark.conf.get("pipeline.env")` pro odlišení prostředí (`dev`, `prod`, ...)

```python
@dlt.table(name="dim_customers")
def dim_customers():
    df = spark.readStream.table("landing.customers")
    return df.select("id", "name", "email")
```

---

### 3. Vytvoření pipeline - vytvoření ručně v Databricks - https://www.databricks.com/glossary/data-pipelines, https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started
- Přidej novou konfiguraci do složky [`pipelines/`](./pipelines), nebo přímo vytvářej v databricks
- Použij YAML nebo JSON specifikaci:
  ```json
  {
    "name": "silver_layer_pipeline",
    "target": "dev_silver",
    "notebook_path": "/Repos/<user>/data-engineering-lab-databricks/dlt_definitions/silver_layer",
    "libraries": [],
    "development": true
  }
  ```

---

### 4. Spuštění pipeline

#### Možnost A – přes UI
- Otevři Databricks UI → Workflows → Delta Live Tables → +Create pipeline
- Zadej cestu k notebooku (např. `/Repos/<user>/...`)
- Zvol katalog, target schéma a cluster

#### Možnost B – přes CLI
```bash
databricks pipelines deploy --spec pipelines/dev_pipeline.json
databricks pipelines start --name silver_layer_pipeline
```

---

### 5. Automatizace (Workflows)
- Vytvářej orchestraci v [`workflows_definition/`](./workflows_definition)
- Použij `databricks jobs create` pro automatizované joby
- Např. JSON definice pipeline s triggerem:

```json
{
  "name": "daily_refresh",
  "tasks": [...],
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "Europe/Prague"
  }
}
```

---

### 6. Governance a Masking
- Masking policies a funkce definuj v SQL nebo Pythonu
- Používej Unity Catalog maskovací funkce:
```sql
CREATE OR REPLACE FUNCTION principal_lab_db.dev_silver.mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('viewers') THEN email ELSE '***' END;
```

---

## Struktura repozitáře

```
data-engineering-lab-databricks/
├── dlt_definitions/           # Transformace: DLT notebooky nebo skripty
├── pipelines/                 # Konfigurace pipeline (.json/.yml)
├── workflows_definition/      # Orchestrace jobů (Databricks Jobs JSON)
├── infrastructure/            # Katalogy, volumes, schémata (Terraform / SQL)
├── notebooks/                 # Vývojové nebo testovací notebooky
├── Solution/                  # Výstupní reporty, prezentace, PBIX
└── README.md
```

---

## Požadavky

- ✅ Databricks workspace (s Unity Catalog)
- ✅ Git + GitHub (verzování notebooků)
- ✅ Databricks CLI (volitelně pro CI/CD)
- ✅ Volumes nebo přístup k externímu storage (ADLS Gen2)

---

![image](https://github.com/user-attachments/assets/1c18a3d4-0376-42c5-a581-a60863c7c648)

Nejdůležitější odkazy:
- Základní stránka: https://www.databricks.com/
- Partner: https://partner-academy.databricks.com/
- Udemy: https://principal.udemy.com/organization/search/?src=ukw&q=databricks
- OneNote: Certification, RoadMap, Procedure
- Medium: https://medium.com/the-data-therapy/how-i-scored-95-on-the-databricks-data-engineer-associate-certification-a-comprehensive-guide-c4ea47485a05
- Databricks training: https://www.databricks.com/learn/training/home
- Databricks Certification: https://www.databricks.com/learn/certification/data-engineer-associate
- Přihlášení na nákup certifikace: https://www.webassessor.com/
- Příprava na zkoušku: https://www.examtopics.com/exams/databricks/certified-data-engineer-associate/
- ![image](https://github.com/user-attachments/assets/dc252d8a-9108-45bf-a1dc-1b88bb859d0f)

