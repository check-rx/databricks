# Databricks Notebook / Python Script
# Silver enrichment pipeline for PBP files
# ----------------------------------------
# - Reads Bronze tables
# - Joins with benefits dictionary
# - Preserves raw values + adds enriched dictionary context
# - Writes Silver enriched tables
# - Logs results to auto.audit.load_log (shared schema with Bronze)

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime

# --- Config ---
CATALOG = "auto"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
DICT   = f"{CATALOG}.landing.pbp_dictionary"
AUDIT  = f"{CATALOG}.audit.load_log"

# Configurable dictionary field prefix
BENDICT_PREFIX = "bendict_"

# Configurable null handling
USE_PLACEHOLDER = True
PLACEHOLDER = "novalue"

# Ensure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")

# Helper for canonicalization
def canon(col):
    c = F.upper(F.trim(col.cast("string")))
    return F.when(c.rlike(r'^[0-9]$'), F.lpad(c, 2, "0")).otherwise(c)

# Helper for null/placeholder handling
def safe_col(col):
    return F.coalesce(col, F.lit(PLACEHOLDER)) if USE_PLACEHOLDER else col

# Load dictionary
dict_all = spark.table(DICT)

d_meta = dict_all.select(
    F.lower(F.col("file")).alias("dict_file"),
    "name",
    F.col("title").alias(f"{BENDICT_PREFIX}title"),
    F.col("field_title").alias(f"{BENDICT_PREFIX}field_title"),
    F.col("json_question").alias(f"{BENDICT_PREFIX}json_question")
).dropDuplicates(["dict_file","name"])

d_enum = dict_all.select(
    F.lower(F.col("file")).alias("dict_file"),
    "name",
    F.col("codes").cast("string").alias(f"{BENDICT_PREFIX}code"),
    F.col("code_values").alias(f"{BENDICT_PREFIX}code_values")
).withColumn(f"{BENDICT_PREFIX}code_canon", canon(F.col(f"{BENDICT_PREFIX}code")))

# Candidate Bronze tables
tables_df = spark.sql(f"SHOW TABLES IN {BRONZE}")
candidate_tables = [
    r["tableName"] for r in tables_df.collect()
    if r["tableName"].lower().startswith("pbp_")
    and "plan_area" not in r["tableName"].lower()
]

print("📄 Candidate tables:", candidate_tables)

audit_entries = []

for tbl in candidate_tables:
    src_fq = f"{BRONZE}.{tbl}"
    out_tbl = f"{SILVER}.{tbl}_enriched"
    raw_file = f"{tbl}.txt"

    started_at = datetime.now().isoformat()
    status = "SUCCESS"
    row_count = 0
    error_message = None

    print(f"\n➡️  Processing {src_fq} → {out_tbl}")

    try:
        src = spark.table(src_fq)

        # Unpivot wide → long
        cols = src.columns
        cols_keep = [c for c in cols if c.lower()=="bid_id"]
        cols_val  = [c for c in cols if c.lower() not in {"bid_id","filename"}]

        kv = F.array(*[
            F.struct(F.lit(c).alias("col_name"), F.col(c).cast("string").alias("raw_value"))
            for c in cols_val
        ])

        long0 = (src.select(*cols_keep, kv.alias("kv"))
                    .withColumn("kv", F.explode("kv"))
                    .select("bid_id", F.lit(raw_file).alias("filename"), "kv.*"))

        # Canonicalize values
        long1 = long0.withColumn(f"{BENDICT_PREFIX}value_canon", canon(F.col("raw_value")))

        # Join metadata + enums
        long2 = (long1
            # metadata join still uses filename + col_name
            .join(d_meta,
                  (F.lower("filename")==F.col("dict_file")) &
                  (F.col("col_name")==F.col("name")),
                  "left")
            # enum join only needs col_name + code
            .join(d_enum.alias("de"),
                  (F.col("col_name")==F.col("de.name")) &
                  (F.col(f"{BENDICT_PREFIX}value_canon")==F.col(f"de.{BENDICT_PREFIX}code_canon")),
                  "left")
            .withColumn(f"{BENDICT_PREFIX}value_decoded", F.col(f"de.{BENDICT_PREFIX}code_values"))
        )

        # Select with dictionary fields first
        final = long2.select(
            safe_col(F.col(f"{BENDICT_PREFIX}title")).alias(f"{BENDICT_PREFIX}title"),
            safe_col(F.col(f"{BENDICT_PREFIX}field_title")).alias(f"{BENDICT_PREFIX}field_title"),
            safe_col(F.col(f"{BENDICT_PREFIX}json_question")).alias(f"{BENDICT_PREFIX}json_question"),
            safe_col(F.col(f"{BENDICT_PREFIX}code")).alias(f"{BENDICT_PREFIX}code"),
            safe_col(F.col(f"{BENDICT_PREFIX}code_values")).alias(f"{BENDICT_PREFIX}code_values"),
            safe_col(F.col(f"{BENDICT_PREFIX}value_canon")).alias(f"{BENDICT_PREFIX}value_canon"),
            safe_col(F.col(f"{BENDICT_PREFIX}value_decoded")).alias(f"{BENDICT_PREFIX}value_decoded"),

            # Identifiers
            "bid_id", "filename", "col_name",

            # Raw value
            safe_col(F.col("raw_value")).alias("raw_value")
        )

        # Write Silver table
        (final.write.mode("overwrite")
             .option("overwriteSchema","true")
             .saveAsTable(out_tbl))

        row_count = spark.table(out_tbl).count()
        print(f"✅ Wrote {row_count:,} rows to {out_tbl}")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)[:4000]
        print(f"❌ FAILED ({src_fq}): {error_message}")

    # Audit log entry
    audit_entries.append(Row(
        timestamp=started_at,
        step="silver_enrich",
        source_file=src_fq,
        output_table=out_tbl,
        action="overwrite",
        status=status,
        row_count=row_count,
        error_message=error_message
    ))

# --- Audit log write ---
audit_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("step", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("output_table", StringType(), True),
    StructField("action", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("error_message", StringType(), True)
])

audit_df = spark.createDataFrame(audit_entries, schema=audit_schema) \
                 .withColumn("timestamp", F.to_timestamp("timestamp"))

audit_df.write.mode("append").saveAsTable(AUDIT)

print(f"\n📊 Audit log appended to {AUDIT}")
