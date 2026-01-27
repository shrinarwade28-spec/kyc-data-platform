import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, length, regexp_replace,
    substring, concat, year
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# -------------------------
# Resolve Glue Arguments
# -------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "s3_bucket", "table_name"]
)

JOB_NAME = args["JOB_NAME"]
S3_BUCKET = args["s3_bucket"]
TABLE_NAME = args["table_name"]

# -------------------------
# Spark / Glue Context
# -------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.app.name", JOB_NAME)

# -------------------------
# Input / Output Paths
# -------------------------
load_date = datetime.utcnow().date()

curated_path = (
    f"s3://{S3_BUCKET}/curated/{TABLE_NAME}/"
    f"load_date={load_date}/"
)

masked_path = (
    f"s3://{S3_BUCKET}/masked/{TABLE_NAME}/"
    f"load_date={load_date}/"
)

print(f"[INFO] Reading curated data from {curated_path}")

df = spark.read.parquet(curated_path)

if df.rdd.isEmpty():
    print("[INFO] No curated data found. Exiting.")
    spark.stop()
    sys.exit(0)

# -------------------------
# PII Masking Logic
# -------------------------

# Aadhaar: XXXXXXXX1234
df = df.withColumn(
    "aadhaar_number",
    when(
        col("aadhaar_number").isNotNull(),
        concat(lit("XXXXXXXX"), substring(col("aadhaar_number"), -4, 4))
    ).otherwise(None)
)

# PAN: ABC****Z
df = df.withColumn(
    "pan_number",
    when(
        col("pan_number").isNotNull(),
        concat(
            substring(col("pan_number"), 1, 3),
            lit("****"),
            substring(col("pan_number"), -1, 1)
        )
    ).otherwise(None)
)

# Mobile: 98****3210
df = df.withColumn(
    "mobile_number",
    when(
        col("mobile_number").isNotNull(),
        concat(
            substring(col("mobile_number"), 1, 2),
            lit("****"),
            substring(col("mobile_number"), -4, 4)
        )
    ).otherwise(None)
)

# Email: j***@domain.com
df = df.withColumn(
    "email",
    when(
        col("email").isNotNull(),
        concat(
            substring(col("email"), 1, 1),
            lit("***"),
            regexp_replace(col("email"), "^[^@]+", "")
        )
    ).otherwise(None)
)

# DOB: keep year only
df = df.withColumn(
    "dob",
    when(col("dob").isNotNull(), year(col("dob"))).otherwise(None)
)

# -------------------------
# Write Masked Data
# -------------------------
print(f"[INFO] Writing masked data to {masked_path}")

(
    df.write
    .mode("overwrite")
    .parquet(masked_path)
)

print("[SUCCESS] PII masking completed successfully")
spark.stop()

