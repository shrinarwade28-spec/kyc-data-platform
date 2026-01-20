import pymysql
import pandas as pd

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "project_one",
    "password": "ProjectOne2026",
    "database": "kyc_db",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor
}

TABLE_NAME = "customers"
CHUNK_SIZE = 10000   # 10k rows per batch

def extract_in_chunks():
    connection = pymysql.connect(**MYSQL_CONFIG)

    query = f"SELECT * FROM {TABLE_NAME}"

    chunk_number = 0
    total_rows = 0

    for chunk_df in pd.read_sql(query, connection, chunksize=CHUNK_SIZE):
        chunk_number += 1
        rows = len(chunk_df)
        total_rows += rows

        print(f"Chunk {chunk_number}: {rows} rows")

        # For now, just show sample
        print(chunk_df.head(2))

    connection.close()

    print(f"\nTotal rows processed: {total_rows}")

if __name__ == "__main__":
    extract_in_chunks()
