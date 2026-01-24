import sys
import boto3
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from io import StringIO
from awsglue.utils import getResolvedOptions

# ------------------------------------------------------
# Glue Arguments (MANDATORY way)
# ------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "mysql_host",
        "mysql_user",
        "mysql_password",
        "mysql_db",
        "mysql_table",
        "s3_bucket",
        "ddb_table",
        "chunk_size"
    ]
)

JOB_NAME = args["JOB_NAME"]
MYSQL_HOST = args["mysql_host"]
MYSQL_USER = args["mysql_user"]
MYSQL_PASSWORD = args["mysql_password"]
MYSQL_DB = args["mysql_db"]
MYSQL_TABLE = args["mysql_table"]
S3_BUCKET = args["s3_bucket"]
DDB_TABLE_NAME = args["ddb_table"]
CHUNK_SIZE = int(args["chunk_size"])

# ------------------------------------------------------
# AWS Clients
# ------------------------------------------------------
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table(DDB_TABLE_NAME)

# ------------------------------------------------------
# DynamoDB Helper Functions
# ------------------------------------------------------
def get_pipeline_item(table, pipeline_name):
    response = table.get_item(
        Key={"pipeline_name": pipeline_name}
    )
    return response.get("Item")


def get_pipeline_state(table, pipeline_name):
    item = get_pipeline_item(table, pipeline_name)
    if not item:
        print(f"[INFO] First run detected for job: {pipeline_name}")
        return "INIT"
    return item.get("pipeline_execution_state", "INIT")


def get_last_watermark(table, pipeline_name):
    item = get_pipeline_item(table, pipeline_name)
    if not item:
        return None
    return item.get("last_watermark")


def update_pipeline_state(
    table,
    pipeline_name,
    state,
    watermark=None
):
    item = {
        "pipeline_name": pipeline_name,
        "pipeline_execution_state": state,
        "last_updated_ts": datetime.utcnow().isoformat()
    }

    if watermark:
        item["last_watermark"] = str(watermark)

    table.put_item(Item=item)


# ------------------------------------------------------
# MySQL Read Logic (CDC + Incremental)
# ------------------------------------------------------
def read_mysql_cdc(engine, table, last_watermark, chunk_size):
    if last_watermark:
        query = f"""
            SELECT *,
                   CASE
                       WHEN is_deleted = 1 THEN 'D'
                       WHEN created_at = updated_at THEN 'I'
                       ELSE 'U'
                   END AS cdc_op
            FROM {table}
            WHERE updated_at > '{last_watermark}'
            ORDER BY updated_at
        """
    else:
        # First run – full load
        query = f"""
            SELECT *,
                   'I' AS cdc_op
            FROM {table}
            ORDER BY updated_at
        """

    return pd.read_sql(query, engine, chunksize=chunk_size)


# ------------------------------------------------------
# S3 Write Logic
# ------------------------------------------------------
def write_to_s3(df, bucket, table):
    execution_time = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"raw/{table}/load_ts={execution_time}/{table}.csv"

    buffer = StringIO()
    df.to_csv(buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )

    print(f"[INFO] Written {len(df)} records to s3://{bucket}/{key}")


# ------------------------------------------------------
# Main
# ------------------------------------------------------
def main():
    print(f"[INFO] Starting Glue job: {JOB_NAME}")

    # 1. Read pipeline state
    pipeline_state = get_pipeline_state(ddb_table, JOB_NAME)
    print(f"[INFO] Current pipeline state: {pipeline_state}")

    # 2. Read last watermark
    last_watermark = get_last_watermark(ddb_table, JOB_NAME)
    print(f"[INFO] Last watermark: {last_watermark}")

    # 3. Mark job as RUNNING
    update_pipeline_state(
        ddb_table,
        JOB_NAME,
        state="RUNNING",
        watermark=last_watermark
    )

    # 4. MySQL connection
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:3306/{MYSQL_DB}",
        pool_pre_ping=True,
        pool_recycle=300
    )

    max_watermark = last_watermark

    # 5. CDC Processing
    for chunk_df in read_mysql_cdc(
        engine,
        MYSQL_TABLE,
        last_watermark,
        CHUNK_SIZE
    ):
        if chunk_df.empty:
            continue

        write_to_s3(chunk_df, S3_BUCKET, MYSQL_TABLE)

        chunk_max = chunk_df["updated_at"].max()
        if not max_watermark or chunk_max > max_watermark:
            max_watermark = chunk_max

    # 6. Mark SUCCESS (only after full completion)
    update_pipeline_state(
        ddb_table,
        JOB_NAME,
        state="SUCCESS",
        watermark=max_watermark
    )

    print("[INFO] Job completed successfully")


# ------------------------------------------------------
# Entry Point with Failure Recovery
# ------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERROR] Job failed")
        print(str(e))

        # Mark job as FAILED without corrupting watermark
        update_pipeline_state(
            ddb_table,
            JOB_NAME,
            state="FAILED"
        )
        raise

