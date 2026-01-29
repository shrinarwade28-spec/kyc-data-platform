import sys
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

# -------------------------
# Resolve Glue Arguments
# -------------------------
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

JOB_NAME     = args["JOB_NAME"]
MYSQL_HOST  = args["mysql_host"]
MYSQL_DB    = args["mysql_db"]
MYSQL_TABLE = args["mysql_table"]
MYSQL_USER  = args["mysql_user"]
MYSQL_PASS  = args["mysql_password"]
S3_BUCKET   = args["s3_bucket"]
DDB_TABLE   = args["ddb_table"]

NUM_PARTS   = int(args["num_partitions"])
FETCH_SIZE  = int(args["fetch_size"])

# -------------------------
# Spark / Glue Context
# -------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.app.name", JOB_NAME)

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

base_query = f"SELECT * FROM {MYSQL_TABLE}"
if last_ts:
    base_query += f" WHERE updated_at > '{last_ts}'"

jdbc_query = f"({base_query}) AS src"

# -------------------------
# Read from MySQL via JDBC
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

# -------------------------
# NO DATA HANDLING
# -------------------------
if df.rdd.isEmpty():
    print("[INFO] No new records found. Graceful completion.")

    result = {
        "status": "NO_DATA",
        "record_count": 0
    }

    print(f"RESULT={result}")
    spark.stop()
    # Script ends naturally

else:
    # -------------------------
    # Write to S3 (Raw Zone)
    # -------------------------
    load_date = datetime.utcnow().date()
    target_path = (
        f"s3://{S3_BUCKET}/raw/{MYSQL_TABLE}/"
        f"load_date={load_date}/"
    )

    df.write.mode("append").parquet(target_path)
    print(f"[INFO] Written data to {target_path}")

    # -------------------------
    # Update DynamoDB Watermark
    # -------------------------
    max_ts = df.select(spark_max("updated_at")).collect()[0][0]

    state_table.put_item(
        Item={
            "pipeline_name": JOB_NAME,
            "last_processed_ts": str(max_ts)
        }
    )

    record_count = df.count()

    result = {
        "status": "SUCCESS",
        "record_count": record_count
    }

    print(f"RESULT={result}")
    spark.stop()

