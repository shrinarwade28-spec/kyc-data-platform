import boto3
import pandas as pd
from io import StringIO
from datetime import date
import re

# =========================
# CONFIG
# =========================
BUCKET_NAME = "bfs-kyc-lake"

LOAD_DATE = date.today().strftime("%Y-%m-%d")

CURATED_PREFIX = f"curated/customer_kyc/load_date={LOAD_DATE}/"
MASKED_PREFIX = f"masked/customer_kyc/load_date={LOAD_DATE}/"

s3 = boto3.client("s3")

# =========================
# MASKING FUNCTIONS
# =========================
def mask_pan(pan):
    """
    ABCDE1234F -> ABCXX123XF
    """
    pan = str(pan)
    if len(pan) != 10:
        return pan
    return pan[:3] + "XX" + pan[5:9] + "X"

def mask_aadhaar(aadhaar):
    """
    123456789012 -> XXXXXXXX9012
    """
    aadhaar = str(aadhaar)
    if not re.match(r"^\d{12}$", aadhaar):
        return aadhaar
    return "XXXXXXXX" + aadhaar[-4:]

# =========================
# MAIN LOGIC
# =========================
def read_curated_data():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=CURATED_PREFIX
    )

    dfs = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".csv"):
                print(f"Reading curated file: {key}")
                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                df = pd.read_csv(response["Body"])
                dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

def write_masked_data(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{MASKED_PREFIX}customers_masked.csv",
        Body=buffer.getvalue()
    )

    print(f"Masked data written to s3://{BUCKET_NAME}/{MASKED_PREFIX}")

def mask_pii():
    df = read_curated_data()

    if df.empty:
        print("No curated data found. Exiting.")
        return

    print(f"Total curated records: {len(df)}")

    # Apply masking
    df["pan_number"] = df["pan_number"].apply(mask_pan)
    df["aadhaar_number"] = df["aadhaar_number"].apply(mask_aadhaar)

    write_masked_data(df)

    print("PII Masking step completed")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    mask_pii()
