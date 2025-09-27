# Databricks Notebook / Python Script
# Bronze ingestion pipeline for PBP files
# ---------------------------------------
# - Reads raw .txt files from Volumes
# - Writes to Delta tables in auto.bronze
# - Logs status to auto.audit.load_log
# - Designed to be rerunnable & production-safe

from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

# --- Config ---
CATALOG = "auto"
BRONZE = f"{CATALOG}.bronze"
AUDIT = f"{CATALOG}.audit.load_log"
RAW_PATH = "/Volumes/auto/landing/plans"   # adjust if path changes

# Ensure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")

# List files from the raw path
files = [f.path for f in dbutils.fs.ls(RAW_PATH) if f.name.endswith(".txt")]
print("📄 Files detected:", files)

audit_entries = []

for file_path in files:
    file_name = file_path.split("/")[-1]          # e.g., pbp_section_a.txt
    table_name = file_name.replace(".txt", "").lower()  # e.g., pbp_section_a
    out_tbl = f"{BRONZE}.{table_name}"

    started_at = datetime.now().isoformat()
    status = "SUCCESS"
    row_count = 0
    error_message = None

    print(f"\n➡️  Ingesting {file_name} → {out_tbl}")

    try:
        # Read tab-delimited file
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "\t")
            .load(file_path)
        )

        # Write to Bronze (overwrite, schema evolution)
        (
            df.write
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(out_tbl)
        )

        row_count = spark.table(out_tbl).count()
        print(f"✅ Loaded {row_count:,} rows into {out_tbl}")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)[:4000]
        print(f"❌ FAILED to load {file_name}: {error_message}")

    # Collect audit log entry
    audit_entries.append(Row(
        timestamp=started_at,
        step="bronze_ingest",
        source_file=file_name,
        output_table=out_tbl,
        action="overwrite",
        status=status,
        row_count=row_count,
        error_message=error_message
    ))

# Append audit log
audit_df = spark.createDataFrame(audit_entries).withColumn("timestamp", F.to_timestamp("timestamp"))
audit_df.write.mode("append").saveAsTable(AUDIT)

print(f"\n📊 Audit log updated in {AUDIT}")
