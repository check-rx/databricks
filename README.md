# Databricks Lakehouse – PBP Data Pipeline

Used for **Databricks resources**.  
Will hold **data warehousing, lakehouse, data lake, ETL**, and other scripts related to working with them within the Databricks ecosystem.  

⚠️ **Note**: This repo could be replaced by another environment if Databricks is not used.  
All **data cleaning** and **ETL scripts** are portable and runnable in plain **Python/Spark**.

---

# Lakehouse PBP Data Pipeline

A production-ready **Bronze → Silver → (Gold soon)** lakehouse for **PBP plan data**.  

### 🎯 Design Principles
- 🔄 **Rerunnable** – idempotent overwrites with schema evolution
- 📝 **Audited** – every load and validation logged
- ✅ **Validated** – SQL unit tests for downstream quality
- 📦 **Portable** – runnable inside Databricks *and* via `spark-submit` / `spark-sql`

---

## 📚 Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Repository Layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Environment & Schemas](#environment--schemas)
- [Audit Tables](#audit-tables)
- [Run the Pipeline](#run-the-pipeline)
- [Configuration Knobs](#configuration-knobs)
- [Validation](#validation)
- [Operational Monitoring](#operational-monitoring)
- [Unit Tests (CI/CD)](#unit-tests-optional-cicd)
- [Troubleshooting](#troubleshooting--gotchas)
- [Roadmap](#roadmap-gold--rag)

---

## 🧩 Architecture

**Bronze (Raw)**  
- Raw copy of tab-delimited files from Unity Catalog Volumes.  
- Delta tables in `auto.bronze`.  
- No enrichment, no renames, NULLs preserved.  

**Silver (Enriched)**  
- Reads Bronze tables + joins **Benefits Dictionary**.  
- Adds context with configurable prefix (default: `bendict_`).  
- Normalizes values (`trim/upper/lpad`).  
- Outputs **long format records** `(bid_id, column_name)`.

**Gold (Planned)**  
- BI & RAG-ready.  
- `plan_facts_long` for **RAG/RIG embeddings**.  
- `plan_master` wide for **dashboards & APIs**.

---

## 🔄 Data Flow

```text
/Volumes/auto/landing/plans/*.txt
   │
   ▼
auto.bronze.<file>   (Bronze ingestion)
   │
   ▼
auto.silver.<file>_enriched   (Silver enrichment w/ dictionary)
   │
   ▼
auto.gold.*   (coming soon)


lakehouse-pbp/
├── README.md
├── audit/
│   └── audit_schema.sql        # CREATE TABLEs for audit logs
├── bronze/
│   └── bronze_ingest.py        # Bronze loader
├── silver/
│   └── silver_enrich.py        # Silver enricher
├── validation/
│   └── silver_validation.sql   # SQL unit tests
├── run_pipeline.py             # Orchestrator (bronze → silver → validation)
└── tests/
    ├── test_bronze.py
    └── test_silver.py



⚙️ Prerequisites

Databricks workspace with Unity Catalog enabled

Catalog: auto (default, configurable)

Input: /Volumes/auto/landing/plans/*.txt

Tab-delimited (\t)

With headers (else adjust scripts)

Benefits dictionary in auto.landing.pbp_dictionary

Columns: file, name, title, field_title, json_question, codes, code_values


🏗️ Environment & Schemas
CREATE SCHEMA IF NOT EXISTS auto.bronze;
CREATE SCHEMA IF NOT EXISTS auto.silver;
CREATE SCHEMA IF NOT EXISTS auto.audit;


📜 Audit Tables
-- Load log
CREATE TABLE IF NOT EXISTS auto.audit.load_log (
  timestamp     TIMESTAMP,
  step          STRING,
  source_file   STRING,
  output_table  STRING,
  action        STRING,
  status        STRING,
  row_count     BIGINT,
  error_message STRING
);

-- Validation log
CREATE TABLE IF NOT EXISTS auto.audit.validation_log (
  timestamp   TIMESTAMP,
  layer       STRING,
  table_name  STRING,
  test_name   STRING,
  status      STRING,
  details     STRING
);


▶️ Run the Pipeline
🟢 Databricks (Interactive)

Run bronze/bronze_ingest.py → writes to auto.bronze.*

Run silver/silver_enrich.py → writes to auto.silver.*_enriched

Run validation/silver_validation.sql → logs to auto.audit.validation_log

🔵 Databricks Workflow

Job with 3 tasks:

bronze_ingest.py

silver_enrich.py

silver_validation.sql


🟠 Outside Databricks (Spark CLI)
spark-submit bronze/bronze_ingest.py
spark-submit silver/silver_enrich.py
spark-sql -f validation/silver_validation.sql


Or run the orchestrator:
python run_pipeline.py



