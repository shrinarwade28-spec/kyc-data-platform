import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

# -------------------------
# Read Glue Job Arguments
# -------------------------
args = dict(arg.split("=", 1) for arg in sys.argv if "=" in arg)

JOB_NAME     = args.get("--JOB_NAME")
MYSQL_HOST   = args.get("--mysql_host")
MYSQL_DB     = args.get("--mysql_db")
MYSQL_TABLE  = args.get("--mysql_table")
MYSQL_USER   = args.get("--mysql_user")
MYSQL_PASS   = args.get("--mysql_password")
S3_BUCKET    = args.get("--s3_bucket")

# Optional (default safe)
FETCH_SIZE   = int(args.get("--fetch_size", "10000"))

# -------------------------
# Spark Session
# -------------------------
spark = (
    SparkSession.builder
    .appName(JOB_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# JDBC URL
# -------------------------
jdbc_url = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

connection_properties = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": str(FETCH_SIZE)
}

# -------------------------
# Incremental Query
# (Initial full load fallback)
# -------------------------
incremental_query = f"""
(
  SELECT *
  FROM {MYSQL_TABLE}
) AS src
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

# -------------------------
# Read Glue Job Arguments
# -------------------------
args = dict(arg.split("=", 1) for arg in sys.argv if "=" in arg)

JOB_NAME     = args.get("--JOB_NAME")
MYSQL_HOST   = args.get("--mysql_host")
MYSQL_DB     = args.get("--mysql_db")
MYSQL_TABLE  = args.get("--mysql_table")
MYSQL_USER   = args.get("--mysql_user")
MYSQL_PASS   = args.get("--mysql_password")
S3_BUCKET    = args.get("--s3_bucket")

# Optional (default safe)
FETCH_SIZE   = int(args.get("--fetch_size", "10000"))

# -------------------------
# Spark Session
# -------------------------
spark = (
    SparkSession.builder
    .appName(JOB_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# JDBC URL
# -------------------------
jdbc_url = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

connection_properties = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": str(FETCH_SIZE)
}

# -------------------------
# Incremental Query
# (Initial full load fallback)
# -------------------------
incremental_query = f"""
(
  SELECT *
  FROM {MYSQL_TABLE}
) AS src
"""

# -------------------------
# Read from MySQL using JDBC
# -------------------------
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", incremental_query)
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASS)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("fetchsize", FETCH_SIZE)
    .load()
)

if df.rdd.isEmpty():
    print("[INFO] No data found. Exiting job.")
    spark.stop()
    sys.exit(0)

# -------------------------
# S3 Write Path
# -------------------------
load_date = datetime.utcnow().date()

s3_target_path = (
    f"s3://{S3_BUCKET}/raw/{MYSQL_TABLE}/"
    f"load_date={load_date}/"
)

# -------------------------
# Write to S3 (Parquet)
# -------------------------
(
    df
    .write
    .mode("append")
    .parquet(s3_target_path)
)

print(f"[INFO] Data successfully written to {s3_target_path}")

spark.stop()
# -------------------------
# Read from MySQL using JDBC
# -------------------------
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", incremental_query)
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASS)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("fetchsize", FETCH_SIZE)
    .load()
)

if df.rdd.isEmpty():
    print("[INFO] No data found. Exiting job.")
    spark.stop()
    sys.exit(0)

# -------------------------
# S3 Write Path
# -------------------------
load_date = datetime.utcnow().date()

s3_target_path = (
    f"s3://{S3_BUCKET}/raw/{MYSQL_TABLE}/"
    f"load_date={load_date}/"
)

# -------------------------
# Write to S3 (Parquet)
# -------------------------
(
    df
    .write
    .mode("append")
    .parquet(s3_target_path)
)

print(f"[INFO] Data successfully written to {s3_target_path}")

spark.stop()

