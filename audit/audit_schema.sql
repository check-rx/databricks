-- Operational log (Bronze/Silver/Gold loads)
CREATE TABLE IF NOT EXISTS auto.audit.load_log (
  timestamp TIMESTAMP,
  step STRING,           -- bronze_ingest | silver_enrich | gold_build
  source_file STRING,    -- file path (bronze) or table name (silver/gold)
  output_table STRING,
  action STRING,         -- overwrite | validate | etc.
  status STRING,         -- SUCCESS | FAILED
  row_count BIGINT,
  error_message STRING
);

-- Validation log (data quality checks)
CREATE TABLE IF NOT EXISTS auto.audit.validation_log (
  timestamp TIMESTAMP,
  layer STRING,          -- bronze | silver | gold
  table_name STRING,
  test_name STRING,
  status STRING,         -- PASS | FAIL
  details STRING
);
