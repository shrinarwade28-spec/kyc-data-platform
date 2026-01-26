import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# -------------------------
# Read Glue Job Arguments
# -------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_bucket",
        "raw_prefix",
        "curated_prefix",
        "rejected_prefix"
    ]
)

RAW_BUCKET = args["raw_bucket"]
RAW_PREFIX = args["raw_prefix"]
CURATED_PREFIX = args["curated_prefix"]
REJECTED_PREFIX = args["rejected_prefix"]

# -------------------------
# Spark / Glue Context
# -------------------------
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Read RAW Data
# -------------------------
raw_path = f"s3://{RAW_BUCKET}/{RAW_PREFIX}"

print(f"[INFO] Reading RAW data from: {raw_path}")

df = spark.read.parquet(raw_path)

if df.rdd.isEmpty():
    print("[INFO] No data found in RAW zone. Exiting.")
    spark.stop()
    sys.exit(0)

# -------------------------
# Data Quality Rules
# -------------------------
df = df.withColumn(
    "rejection_reason",
    when(~col("pan_number").rlike("^[A-Z]{5}[0-9]{4}[A-Z]$"), lit("INVALID_PAN"))
    .when(~col("aadhaar_number").rlike("^[0-9]{12}$"), lit("INVALID_AADHAAR"))
    .when(col("email").isNotNull() & ~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
          lit("INVALID_EMAIL"))
    .when(col("mobile_number").isNotNull() & ~col("mobile_number").rlike("^[0-9]{10}$"),
          lit("INVALID_MOBILE"))
)

# -------------------------
# Split Data
# -------------------------
curated_df = df.filter(col("rejection_reason").isNull()) \
               .drop("rejection_reason")

rejected_df = df.filter(col("rejection_reason").isNotNull())

# -------------------------
# Write Outputs
# -------------------------
load_date = datetime.utcnow().date()

curated_path = (
    f"s3://{RAW_BUCKET}/{CURATED_PREFIX}"
    f"load_date={load_date}/"
)

rejected_path = (
    f"s3://{RAW_BUCKET}/{REJECTED_PREFIX}"
    f"load_date={load_date}/"
)

print(f"[INFO] Writing CURATED data to: {curated_path}")
curated_df.write.mode("append").parquet(curated_path)

print(f"[INFO] Writing REJECTED data to: {rejected_path}")
rejected_df.write.mode("append").parquet(rejected_path)

print("[INFO] Data quality job completed successfully.")

spark.stop()
