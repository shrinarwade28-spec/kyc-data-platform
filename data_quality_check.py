import pandas as pd
import boto3
import re
from io import StringIO
from datetime import date

# -------------------------
# CONFIG
# -------------------------
BUCKET_NAME = "bfs-kyc-lake"

RAW_PREFIX = "raw/customer_kyc/"
CURATED_PREFIX = "curated/customer_kyc/"
REJECTED_PREFIX = "rejected/customer_kyc/"

LOAD_DATE = date.today().strftime("%Y-%m-%d")

s3 = boto3.client("s3")

# -------------------------
# VALIDATORS
# -------------------------
def is_valid_pan(pan):
    return bool(re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", str(pan)))

def is_valid_aadhaar(aadhaar):
    return bool(re.match(r"^[0-9]{12}$", str(aadhaar)))

# -------------------------
# MAIN QUALITY FUNCTION
# -------------------------
def quality_check():

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=RAW_PREFIX
    )

    raw_dfs = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".csv"):
                continue

            print(f"Reading RAW file: {key}")

            response = s3.get_object(
                Bucket=BUCKET_NAME,
                Key=key
            )

            df = pd.read_csv(response["Body"])

            #  DROP HEADER-LIKE ROWS
            df = df[df["customer_id"] != "customer_id"]

            raw_dfs.append(df)

    if not raw_dfs:
        print("No RAW data found")
        return

    df = pd.concat(raw_dfs, ignore_index=True)

    print("Total RAW records:", len(df))

    # -------------------------
    # APPLY VALIDATIONS
    # -------------------------
    df["invalid_pan"] = ~df["pan_number"].apply(is_valid_pan)
    df["invalid_aadhaar"] = ~df["aadhaar_number"].apply(is_valid_aadhaar)

    df["rejection_reason"] = ""

    df.loc[df["invalid_pan"], "rejection_reason"] += "INVALID_PAN,"
    df.loc[df["invalid_aadhaar"], "rejection_reason"] += "INVALID_AADHAAR,"

    # -------------------------
    # SPLIT DATA
    # -------------------------
    rejected_df = df[df["rejection_reason"] != ""].copy()
    curated_df = df[df["rejection_reason"] == ""].copy()

    print("Curated records:", len(curated_df))
    print("Rejected records:", len(rejected_df))

    # -------------------------
    # WRITE CURATED
    # -------------------------
    if not curated_df.empty:
        curated_df.drop(columns=["invalid_pan", "invalid_aadhaar"]) \
                  .to_csv(
                      buffer := StringIO(),
                      index=False
                  )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{CURATED_PREFIX}load_date={LOAD_DATE}/customers.csv",
            Body=buffer.getvalue()
        )

    # -------------------------
    # WRITE REJECTED
    # -------------------------
    if not rejected_df.empty:
        rejected_df.to_csv(
            buffer := StringIO(),
            index=False
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{REJECTED_PREFIX}load_date={LOAD_DATE}/customers.csv",
            Body=buffer.getvalue()
        )

    print("Data Quality step completed")

# -------------------------
# ENTRY POINT
# -------------------------
if __name__ == "__main__":
    quality_check()

