import pymysql

try:
    connection = pymysql.connect(
        host="localhost",
        user="project_one",
        password="ProjectOne2026",
        database="kyc_db",
        port=3306
    )

    print("Connected to MySQL successfully")

    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()

        print("Databases:")
        for db in databases:
            print(" -", db[0])

except Exception as e:
    print("Connection failed")
    print(e)

finally:
    if 'connection' in locals():
        connection.close()
        print("Connection closed")

