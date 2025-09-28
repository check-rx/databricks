from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "auto"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

now = datetime.now()

# Function to split bid_id into normalized IDs
def normalize_ids(df):
    return (df.withColumn("contract_id", F.split(F.col("bid_id"), "_")[0])
              .withColumn("plan_number", F.split(F.col("bid_id"), "_")[1])
              .withColumn("segment_id", F.split(F.col("bid_id"), "_")[2])
              .withColumn("full_bid_id", F.col("bid_id")))

# Function to classify coverage type
def classify_col(colname: str):
    col = colname.lower()
    if "copay" in col: return "copay"
    if "coins" in col: return "coinsurance"
    if "ded" in col: return "deductible"
    if "maxplan" in col or "maxenr" in col: return "max_plan"
    if "limit" in col: return "limit"
    return "other"

tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {SILVER}").collect()
    if r["tableName"].endswith("_enriched")
]

for tbl in tables:
    df = spark.table(f"{SILVER}.{tbl}")
    df = normalize_ids(df) \
        .withColumn("coverage_type", F.udf(classify_col, "string")(F.col("col_name"))) \
        .withColumn("last_updated", F.lit(now)) \
        .withColumn("service_category", F.lit(tbl))

    # Plan master (Section A only)
    if tbl == "pbp_section_a_enriched":
        (df.select("contract_id","plan_number","segment_id","full_bid_id",
                   F.lit(tbl).alias("source_table"),
                   "col_name","raw_value","bendict_json_question","last_updated")
           .write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{GOLD}.plan_master"))

    # Plan services long (narrative/unstructured)
    (df.select("contract_id","plan_number","segment_id","full_bid_id",
               F.lit(tbl).alias("source_table"),
               "col_name","bendict_json_question","raw_value","last_updated")
    .write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{GOLD}.plan_services_long"))

    # Costsharing (structured)
    (df.select("contract_id","plan_number","segment_id","full_bid_id",
               "service_category","col_name","coverage_type",
               F.when(F.col("col_name").like("%_min%"), "min")
                .when(F.col("col_name").like("%_max%"), "max")
                .when(F.col("col_name").like("%_yn"), "yn")
                .otherwise("value").alias("metric"),
               F.when(F.col("col_name").like("%_amt"), "amount")
                .when(F.col("col_name").like("%_pct"), "percent")
                .when(F.col("col_name").like("%_per_d"), "per_day")
                .when(F.col("col_name").like("%_per"), "per")
                .when(F.col("col_name").like("%_yn"), "flag")
                .otherwise("other").alias("unit"),
               "raw_value","last_updated")
      
    .write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{GOLD}.plan_costsharing"))


    # Limits (subset where coverage_type = limit)
    (df.filter(F.col("coverage_type")=="limit")
       .select("contract_id","plan_number","segment_id","full_bid_id",
               "service_category","col_name",
               F.lit("frequency").alias("limit_type"),
               "raw_value","last_updated")
    .write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{GOLD}.plan_limits"))

print("✅ Gold tables populated with normalized IDs")
