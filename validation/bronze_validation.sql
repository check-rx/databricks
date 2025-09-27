# Bronze Validation Script (Safe with missing/ghost tables)
# ---------------------------------------------------------

from pyspark.sql import functions as F

CATALOG = "auto"
BRONZE = f"{CATALOG}.bronze"
AUDIT  = f"{CATALOG}.audit.validation_log"

tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()]

for tbl in tables:
    fq = f"{BRONZE}.{tbl}"
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
        SELECT current_timestamp(), 'bronze', '{tbl}', 'table_not_empty',
               '{status}', 'Row count = {row}'
    """)

    # --- 2. bid_id not null (if present) ---
    if "bid_id" in cols:
        null_bid = df.filter("bid_id IS NULL").count()
        status = "PASS" if null_bid == 0 else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'bronze', '{tbl}', 'bid_id_not_null',
                   '{status}', 'Null bid_id rows = {null_bid}'
        """)

    # --- 3. filename not null (if present) ---
    if "filename" in cols:
        null_file = df.filter("filename IS NULL").count()
        status = "PASS" if null_file == 0 else "FAIL"
        spark.sql(f"""
            INSERT INTO {AUDIT}
            SELECT current_timestamp(), 'bronze', '{tbl}', 'filename_not_null',
                   '{status}', 'Null filename rows = {null_file}'
        """)

        # --- 4. no duplicate bid_id + filename ---
        if "bid_id" in cols:
            dupes = (df.groupBy("bid_id", "filename")
                       .count()
                       .filter("count > 1")
                       .count())
            status = "PASS" if dupes == 0 else "FAIL"
            spark.sql(f"""
                INSERT INTO {AUDIT}
                SELECT current_timestamp(), 'bronze', '{tbl}', 'no_duplicate_keys',
                       '{status}', 'Duplicate key rows = {dupes}'
            """)

print("✅ Bronze validation completed. Results logged to auto.audit.validation_log")
