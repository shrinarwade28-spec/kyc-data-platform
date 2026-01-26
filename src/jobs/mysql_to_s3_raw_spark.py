import sys
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "mysql_host",
        "mysql_db",
        "mysql_table",
        "mysql_user",
        "mysql_password",
        "s3_bucket",
        "ddb_table",
        "num_partitions",
        "fetch_size"
    ]
)
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.app.name", "mysql_to_s3_raw_spark")

# -------------------------
# Read Job Arguments
# -------------------------
args = dict(arg.split("=", 1) for arg in sys.argv if "=" in arg)

JOB_NAME    = args["--JOB_NAME"]
MYSQL_HOST = args["--mysql_host"]
MYSQL_DB   = args["--mysql_db"]
MYSQL_TABLE= args["--mysql_table"]
MYSQL_USER = args["--mysql_user"]
MYSQL_PASS = args["--mysql_password"]
S3_BUCKET  = args["--s3_bucket"]
DDB_TABLE  = args["--ddb_table"]

NUM_PARTS  = int(args.get("--num_partitions", "4"))
FETCH_SIZE = int(args.get("--fetch_size", "10000"))

# -------------------------
# Spark Session
# -------------------------
spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# DynamoDB – Read Watermark
# -------------------------
ddb = boto3.resource("dynamodb")
state_table = ddb.Table(DDB_TABLE)

resp = state_table.get_item(Key={"pipeline_name": JOB_NAME})
last_ts = resp.get("Item", {}).get("last_processed_ts")

print(f"[INFO] Last processed timestamp: {last_ts}")

# -------------------------
# JDBC Configuration
# -------------------------
jdbc_url = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

base_query = f"""
SELECT *
FROM {MYSQL_TABLE}
"""

if last_ts:
    base_query += f" WHERE updated_at > '{last_ts}'"

jdbc_query = f"({base_query}) AS src"

# -------------------------
# Parallel JDBC Read
# -------------------------
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", jdbc_query)
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASS)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("fetchsize", FETCH_SIZE)
    .option("numPartitions", NUM_PARTS)
    .load()
)

if df.rdd.isEmpty():
    print("[INFO] No new records found. Exiting.")
    spark.stop()
    sys.exit(0)

# -------------------------
# Write to S3 (Raw Zone)
# -------------------------
load_date = datetime.utcnow().date()

target_path = (
    f"s3://{S3_BUCKET}/raw/{MYSQL_TABLE}/"
    f"load_date={load_date}/"
)

(
    df
    .write
    .mode("append")
    .parquet(target_path)
)

print(f"[INFO] Written data to {target_path}")

# -------------------------
# Update Watermark in DynamoDB
# -------------------------
max_ts = df.select(spark_max("updated_at")).collect()[0][0]

state_table.put_item(
    Item={
        "pipeline_name": JOB_NAME,
        "last_processed_ts": str(max_ts)
    }
)

print(f"[INFO] Updated watermark to {max_ts}")

spark.stop()

