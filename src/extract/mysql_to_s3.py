import pandas as pd
import boto3
from sqlalchemy import create_engine
from datetime import date
from io import StringIO

# =========================
# CONFIG
# =========================

MYSQL_USER = "project_one"
MYSQL_PASSWORD = "ProjectOne2026"
MYSQL_HOST = "localhost"
MYSQL_DB = "kyc_db"
MYSQL_TABLE = "customers"

BUCKET_NAME = "bfs-kyc-lake"
CHUNK_SIZE = 10000

load_date = date.today().strftime("%Y-%m-%d")
S3_PREFIX = f"raw/customer_kyc/load_date={load_date}/"

# =========================
# CONNECTIONS
# =========================

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
)

s3 = boto3.client("s3")

# =========================
# FUNCTIONS
# =========================

def upload_df_to_s3(df, part_no):
    """
    Upload one dataframe chunk to S3 as a standalone CSV
    """
    buffer = StringIO()

    # ALWAYS write header (each file is independent)
    df.to_csv(buffer, index=False, header=True)

    s3_key = f"{S3_PREFIX}{MYSQL_TABLE}_part_{part_no:04d}.csv"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")


def extract_mysql_to_s3():
    query = f"SELECT * FROM {MYSQL_TABLE}"

    chunk_no = 1
    total_rows = 0

    print("Starting MySQL → S3 chunked load...")

    for chunk in pd.read_sql(query, engine, chunksize=CHUNK_SIZE):
        print(f"Processing chunk {chunk_no}, rows: {len(chunk)}")

        if chunk.empty:
            print("⚠️ Empty chunk detected, skipping")
            continue

        total_rows += len(chunk)

        upload_df_to_s3(chunk, chunk_no)
        chunk_no += 1

    print(f"Load complete. Total rows processed: {total_rows}")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    extract_mysql_to_s3()

