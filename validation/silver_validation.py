# Silver Validation Script (Dynamic & Schema-Aware)
# -------------------------------------------------
# Validates all *_enriched tables in auto.silver
# Inserts PASS/FAIL results into auto.audit.validation_log

from pyspark.sql import functions as F

CATALOG = "auto"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
AUDIT  = f"{CATALOG}.audit.validation_log"

# Get list of all Silver enriched tables
tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {SILVER}").collect()
    if r["tableName"].endswith("_enriched")
]

for tbl in tables:
    fq = f"{SILVER}.{tbl}"
    try:
        df = spark.table(fq)
        cols = [c.lower() for c in df.columns]
    except Exception as e:
        print(f"⚠️ Skipping {fq} — not a valid table ({str(e)[:120]})")
        continue

    print(f"🔍 Validating {fq}")

    # --- 1. Table not empty ---
    row = df.count()
    status = "PASS" if row > 0 else "FAIL"
    spark.sql(f"""
        INSERT INTO {AUDIT}
        SELECT current_timestamp(), 'silver', '{tbl}', 'table_not_empty',
               '{status}', 'Row count = {row}'
    """)

    # --- 2. Enrichment coverage (at least some dictionary matches) ---
    if "bendict_title" in cols:
        enriched = df.filter("bendict_title IS NOT NULL").count()
        status = "PASS" if enriched > 0 else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'silver', '{tbl}', 'enrichment_coverage',
                   '{status}', 'Rows with enrichment = {enriched}'
        """)

    # --- 3. Decoded values match codes (if both cols exist) ---
    if "bendict_code" in cols and "bendict_value_decoded" in cols:
        bad_decodes = df.filter("bendict_code IS NOT NULL AND bendict_value_decoded IS NULL").count()
        status = "PASS" if bad_decodes == 0 else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'silver', '{tbl}', 'decoded_values_match',
                   '{status}', 'Rows with missing decode = {bad_decodes}'
        """)

    # --- 4. Placeholder handling (if raw_value exists) ---
    if "raw_value" in cols:
        null_raw = df.filter("raw_value IS NULL").count()
        status = "PASS" if null_raw == 0 else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'silver', '{tbl}', 'null_placeholder',
                   '{status}', 'Null raw_value rows = {null_raw}'
        """)

    # --- 5. Row parity vs Bronze (sanity check) ---
    # Only if matching Bronze table exists
    base_tbl = tbl.replace("_enriched", "")
    try:
        bronze_count = spark.table(f"{BRONZE}.{base_tbl}").count()
        silver_count = row
        status = "PASS" if silver_count >= bronze_count else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'silver', '{tbl}', 'row_parity_check',
                   '{status}', 'Silver rows = {silver_count}, Bronze rows = {bronze_count}'
        """)
    except:
        print(f"ℹ️ Skipping row parity check — no Bronze table found for {tbl}")

print("✅ Silver validation completed. Results logged to auto.audit.validation_log")
