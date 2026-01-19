import pymysql
import pandas as pd

#MYSQL Connection details

MYSQL_CONFIG={
	"host":"127.0.0.1",
	"user":"project_one",
	"password":"ProjectOne2026",
	"database":"kyc_db",
	"port":3306
}


def read_mysql_table(table_name):
	"""
	Reads a MySQL table into  a Pandas DataFrame
	"""
	connection = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
	port=MYSQL_CONFIG["port"]
    )

	query = f"select * from {table_name}"
	df = pd.read_sql(query,connection)
	connection.close()
	return df

if __name__== "__main__":
	table_name = "customers"
	df = read_mysql_table(table_name)

	print("Data extracted from MySQL: ")
	print(df)
	print("\nRow count:",len(df)) 
