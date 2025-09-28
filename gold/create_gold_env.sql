-- Plan metadata (Section A)
CREATE SCHEMA IF NOT EXISTS auto.gold;

CREATE TABLE IF NOT EXISTS auto.gold.plan_master (
  plan_id STRING,
  source_table STRING,
  col_name STRING,
  raw_value STRING,
  bendict_json_question STRING,
  last_updated TIMESTAMP
);

-- Structured cost sharing (copay, coinsurance, deductible, max_plan, limit)
CREATE TABLE IF NOT EXISTS auto.gold.plan_costsharing (
  plan_id STRING,
  service_category STRING,
  col_name STRING,
  coverage_type STRING,   -- copay | coinsurance | deductible | max_plan | limit | other
  metric STRING,          -- min | max | value | yn
  unit STRING,            -- amt | pct | per | per_day | flag | other
  raw_value STRING,
  last_updated TIMESTAMP
);

-- Frequency/quantity limits (visit caps, screenings per year, etc.)
CREATE TABLE IF NOT EXISTS auto.gold.plan_limits (
  plan_id STRING,
  service_category STRING,
  col_name STRING,
  limit_type STRING,
  raw_value STRING,
  last_updated TIMESTAMP
);

-- Narrative/unstructured benefits (for RAG embeddings)
CREATE TABLE IF NOT EXISTS auto.gold.plan_services_long (
  plan_id STRING,
  source_table STRING,
  col_name STRING,
  bendict_json_question STRING,
  raw_value STRING,
  last_updated TIMESTAMP
);
