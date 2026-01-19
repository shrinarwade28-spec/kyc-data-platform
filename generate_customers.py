import pymysql
import random
from datetime import datetime, timedelta
import string

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "project_one",
    "password": "ProjectOne2026",
    "database": "kyc_db",
    "cursorclass": pymysql.cursors.DictCursor
}

TOTAL_ROWS = 100000   # you can increase later

def random_name():
    first = random.choice(["Amit", "Ravi", "Suresh", "Neha", "Pooja", "Rahul", "Anita"])
    last = random.choice(["Sharma", "Verma", "Patil", "Iyer", "Gupta", "Khan"])
    return f"{first} {last}"

def random_pan():
    return (
        random.choice(string.ascii_uppercase) +
        random.choice(string.ascii_uppercase) +
        random.choice(string.ascii_uppercase) +
        random.choice(string.ascii_uppercase) +
        random.choice(string.ascii_uppercase) +
        str(random.randint(1000, 9999)) +
        random.choice(string.ascii_uppercase)
    )

def random_aadhaar():
    return "".join([str(random.randint(0, 9)) for _ in range(12)])

def random_mobile():
    return "9" + "".join([str(random.randint(0, 9)) for _ in range(9)])

def random_email(name):
    return name.lower().replace(" ", ".") + "@example.com"

def random_dob():
    start = datetime(1965, 1, 1)
    end = datetime(2005, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def main():
    connection = pymysql.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO customers
        (full_name, pan_number, aadhaar_number, mobile_number, email, dob)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    batch = []
    batch_size = 1000

    for i in range(1, TOTAL_ROWS + 1):
        name = random_name()
        batch.append((
            name,
            random_pan(),
            random_aadhaar(),
            random_mobile(),
            random_email(name),
            random_dob()
        ))

        if i % batch_size == 0:
            cursor.executemany(insert_sql, batch)
            connection.commit()
            batch.clear()
            print(f"Inserted {i} records")

    cursor.close()
    connection.close()
    print("Data generation completed")

if __name__ == "__main__":
    main()
