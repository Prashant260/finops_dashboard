import boto3
import mysql.connector
from datetime import datetime, timedelta
from tabulate import tabulate

# --- Date Setup ---
AWS_REGION = "us-east-1"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpass"
MYSQL_DB = "finopsdb"


def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aws_cost_usage (
            id INT AUTO_INCREMENT PRIMARY KEY,
            start_date DATE,
            end_date DATE,
            service VARCHAR(255),
            amount DECIMAL(10, 2),
            unit VARCHAR(50),
            retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def fetch_cost_data(start_date, end_date):
    client = boto3.client('ce', region_name=AWS_REGION)
    response = client.get_cost_and_usage(
        TimePeriod={'Start': start_date, 'End': end_date},
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )
    return response['ResultsByTime']


def store_cost_data(data):
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Strongpass",
        database="finopsdb",
        port=3306
    )
    cursor = connection.cursor()
    create_table_if_not_exists(cursor)

    for day in data:
        start = day['TimePeriod']['Start']
        end = day['TimePeriod']['End']
        for group in day['Groups']:
            service = group['Keys'][0]
            amount = float(group['Metrics']['UnblendedCost']['Amount'])
            unit = group['Metrics']['UnblendedCost']['Unit']

            cursor.execute("""
                INSERT INTO aws_cost_usage (start_date, end_date, service, amount)
                VALUES (%s, %s, %s, %s)
            """, (start, end, service, amount))

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=7)

    print(f"Fetching AWS cost data from {start_date} to {end_date}...")
    data = fetch_cost_data(str(start_date), str(end_date))

    print("Storing results in MySQL...")
    store_cost_data(data)

    print("✅ Done! Cost data stored successfully.")
