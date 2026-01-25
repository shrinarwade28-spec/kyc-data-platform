import sys
import argparse
import traceback
from datetime import datetime
import pandas as pd
import boto3
from sqlalchemy import create_engine

# -------------------------
# Argument Parsing (Glue-safe)
# -------------------------
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
    parser.add_argument("--chunk_size", type=int, default=50000)

    # IMPORTANT: Glue-safe parsing
    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"[INFO] Ignored Glue system args: {unknown_args}")

    return args


# -------------------------
# DynamoDB Helpers
# -------------------------
def get_last_processed_ts(ddb_table, job_name):
    response = ddb_table.get_item(
        Key={"pipeline_name": job_name}
    )

    item = response.get("Item")
    if not item:
        print(f"[INFO] First run detected for job: {job_name}")
        return None

    return item.get("last_processed_ts")


def update_last_processed_ts(ddb_table, job_name, ts):
    ddb_table.put_item(
        Item={
            "pipeline_name": job_name,
            "last_processed_ts": ts
        }
    )


# -------------------------
# MySQL Read Logic
# -------------------------
def fetch_mysql_data(engine, table, last_ts, chunk_size):
    if last_ts:
        query = f"""
            SELECT *
            FROM {table}
            WHERE updated_at > '{last_ts}'
            ORDER BY updated_at
        """
        print(f"[INFO] CDC load using updated_at > {last_ts}")
    else:
        query = f"SELECT * FROM {table}"
        print("[INFO] Full load (no CDC state found)")

    return pd.read_sql(query, engine, chunksize=chunk_size)


# -------------------------
# Main Glue Job
# -------------------------
def main():
    args = parse_args()

    job_name = args.JOB_NAME

    # DynamoDB
    dynamodb = boto3.resource("dynamodb")
    ddb_table = dynamodb.Table(args.ddb_table)

    last_ts = get_last_processed_ts(ddb_table, job_name)

    # MySQL Engine
    mysql_url = (
    f"mysql+pymysql://{args.mysql_user}:{args.mysql_password}"
    f"@{args.mysql_host}:3306/{args.mysql_db}"
    )

    engine = create_engine(
    mysql_url,
    pool_pre_ping=True,
    pool_recycle=300
    )




    max_processed_ts = last_ts

    try:
        for chunk in fetch_mysql_data(
            engine,
            args.mysql_table,
            last_ts,
            args.chunk_size
        ):
            if chunk.empty:
                continue

            if "updated_at" in chunk.columns:
                max_processed_ts = chunk["updated_at"].max()

            s3_key = (
                f"raw/{args.mysql_table}/"
                f"load_date={datetime.utcnow().date()}/"
                f"data_{datetime.utcnow().timestamp()}.parquet"
            )

            s3_path = f"s3://{args.s3_bucket}/{s3_key}"
            chunk.to_parquet(s3_path, index=False)
            print(f"[INFO] Written chunk to {s3_path}")

        if max_processed_ts:
            update_last_processed_ts(
                ddb_table,
                job_name,
                str(max_processed_ts)
            )
            print(f"[INFO] Updated pipeline state to {max_processed_ts}")

    except Exception:
        print("[ERROR] Job failed. DynamoDB state not updated.")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
