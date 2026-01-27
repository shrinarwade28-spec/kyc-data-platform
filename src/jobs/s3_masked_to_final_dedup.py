import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, lit, current_date
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
# Paths
# -------------------------
load_date = datetime.utcnow().date()

masked_path = (
    f"s3://{S3_BUCKET}/masked/{TABLE_NAME}/"
    f"load_date={load_date}/"
)

final_path = (
    f"s3://{S3_BUCKET}/final/{TABLE_NAME}/"
    f"ingestion_date={load_date}/"
)

print(f"[INFO] Reading masked data from {masked_path}")

df = spark.read.parquet(masked_path)

if df.rdd.isEmpty():
    print("[INFO] No masked data found. Exiting.")
    spark.stop()
    sys.exit(0)

# -------------------------
# Deduplication Logic
# -------------------------
window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

df_dedup = (
    df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# -------------------------
# Add Metadata Columns
# -------------------------
df_final = (
    df_dedup
    .withColumn("ingestion_date", current_date())
    .withColumn("record_source", lit("mysql_rds"))
    .withColumn("is_current", lit(True))
)

# -------------------------
# Write Final Data
# -------------------------
print(f"[INFO] Writing final data to {final_path}")

(
    df_final
    .write
    .mode("overwrite")
    .parquet(final_path)
)

print("[SUCCESS] Final deduplicated dataset created")
spark.stop()
