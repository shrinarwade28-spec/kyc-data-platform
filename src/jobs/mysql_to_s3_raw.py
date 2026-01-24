import argparse
import sys
import boto3
import pymysql
import pandas as pd
from datetime import datetime
from io import StringIO


# -------------------------------------------------------------------
# ARGUMENT PARSING (Glue-compatible)
# -------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--mysql_host", required=True)
    parser.add_argument("--mysql_user", required=True)
    parser.add_argument("--mysql_password", required=True)
    parser.add_argument("--mysql_db", required=True)
    parser.add_argument("--mysql_table", required=True)

    parser.add_argument("--s3_bucket", required=True)
    parser.add_argument("--ddb_table", required=True)

    parser.add_argument("--chunk_size", type=int, default=10000)

    return parser.parse_args()


# -------------------------------------------------------------------
# DYNAMODB HELPERS (SAFE FOR FIRST RUN)
# -------------------------------------------------------------------
def get_pipeline_state(dynamodb_table, job_name):
    """
    Safely fetch pipeline execution state from DynamoDB.
    Handles first-run scenario where no item exists.
    """
    response = dynamodb_table.get_item(
        Key={"pipeline_name": job_name}
    )

    item = response.get("Item")

    if not item:
        print(f"[INFO] First run detected for job: {job_name}")
        return {
            "state": "INIT",
            "last_watermark": None
        }

    return {
        "state": item.get("pipeline_execution_state", "INIT"),
        "last_watermark": item.get("last_watermark")
    }


def update_pipeline_state(dynamodb_table, job_name, state, last_watermark=None):
    update_expr = "SET pipeline_execution_state = :s"
    expr_vals = {":s": state}

    if last_watermark is not None:
        update_expr += ", last_watermark = :w"
        expr_vals[":w"] = last_watermark

    dynamodb_table.update_item(
        Key={"pipeline_name": job_name},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_vals
    )

    print(f"[INFO] DynamoDB state updated → {state}")


# -------------------------------------------------------------------
# MYSQL CONNECTION
# -------------------------------------------------------------------
def get_mysql_connection(args):
    return pymysql.connect(
        host=args.mysql_host,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_db,
        cursorclass=pymysql.cursors.DictCursor
    )


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    args = parse_args()

    print(f"[INFO] Job started: {args.JOB_NAME}")

    # AWS clients
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.client("s3")

    ddb_table = dynamodb.Table(args.ddb_table)

    # Read pipeline state
    pipeline_meta = get_pipeline_state(ddb_table, args.JOB_NAME)
    current_state = pipeline_meta["state"]
    last_watermark = pipeline_meta["last_watermark"]

    print(f"[INFO] Current state: {current_state}")
    print(f"[INFO] Last watermark: {last_watermark}")

    # Decide FULL vs CDC
    if current_state == "INIT" or last_watermark is None:
        print("[INFO] Running FULL load")
        sql_query = f"SELECT * FROM {args.mysql_table}"
    else:
        print("[INFO] Running CDC load")
        sql_query = f"""
            SELECT *
            FROM {args.mysql_table}
            WHERE updated_at > '{last_watermark}'
        """

    conn = get_mysql_connection(args)

    total_rows = 0
    chunk_index = 0

    for chunk_df in pd.read_sql(sql_query, conn, chunksize=args.chunk_size):
        if chunk_df.empty:
            continue

        chunk_index += 1
        total_rows += len(chunk_df)

        s3_key = (
            f"raw/{args.mysql_table}/"
            f"load_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
            f"part_{chunk_index}.csv"
        )

        csv_buffer = StringIO()
        chunk_df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=args.s3_bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )

        print(f"[INFO] Uploaded chunk {chunk_index} → s3://{args.s3_bucket}/{s3_key}")

    conn.close()

    print(f"[INFO] Total rows processed: {total_rows}")

    # Update DynamoDB state after success
    new_watermark = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    update_pipeline_state(
        ddb_table,
        args.JOB_NAME,
        state="SUCCESS",
        last_watermark=new_watermark
    )

    print("[INFO] Job completed successfully")


# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Job failed: {str(e)}", file=sys.stderr)
        raise
