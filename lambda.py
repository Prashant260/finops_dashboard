import boto3
import mysql.connector
from datetime import datetime, timedelta


# AWS Client

ce = boto3.client("ce")



# DB Connection

def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Strongpass",
        database="finopsdb"        # << your actual DB name
    )



# Fetch Cost Data 

def fetch_cost_data(start_date, end_date, tag_key=None):
    # AWS allows only TWO GroupBy items
    group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}]

    if tag_key:
        group_by.append({"Type": "TAG", "Key": tag_key})
    else:
        group_by.append({"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"})

    print(f"\nFetching AWS Cost Data from {start_date} to {end_date} ...")
    print("Using GroupBy:", group_by)

    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start_date, "End": end_date},
        Granularity="DAILY",
        Metrics=["UnblendedCost", "AmortizedCost", "UsageQuantity"],
        GroupBy=group_by
    )

    return resp["ResultsByTime"]



# Store Into MySQL

def store_cost_data(results, tag_key=None):
    conn = get_db()
    cur = conn.cursor()

    sql = """
        INSERT INTO aws_cost_usage (
            start_date, end_date, service, usage_type, linked_account,
            tag_key, tag_value, unblended_cost, amortized_cost,
            usage_quantity, unit
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    records = []

    for day in results:
        start = day["TimePeriod"]["Start"]
        end = day["TimePeriod"]["End"]

        for group in day.get("Groups", []):
            keys = group.get("Keys", [])

            service = keys[0] if len(keys) > 0 else None

            if tag_key:
                tag_value = keys[1] if len(keys) > 1 else None
                linked_account = None
            else:
                linked_account = keys[1] if len(keys) > 1 else None
                tag_value = None

            metrics = group["Metrics"]
            unblended = float(metrics["UnblendedCost"]["Amount"])
            amortized = float(metrics["AmortizedCost"]["Amount"])
            usage_qty = float(metrics["UsageQuantity"]["Amount"])
            unit = metrics["UnblendedCost"]["Unit"]

            records.append((
                start, end, service, None, linked_account,
                tag_key, tag_value, unblended, amortized, usage_qty, unit
            ))

    print(f"Rows to insert: {len(records)}")

    if records:
        cur.executemany(sql, records)
        conn.commit()

    cur.close()
    conn.close()

    print("Data inserted into MySQL successfully!")



# Main Handler

def lambda_handler(event=None, context=None):
    end = datetime.utcnow().date()
    start = end - timedelta(days=30)

    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    TAG_KEY = "Environment"

    results = fetch_cost_data(start_date, end_date, TAG_KEY)
    store_cost_data(results, TAG_KEY)

    print("Ingestion Completed Successfully!")
    return {"status": "success"}



# Run Locally

if __name__ == "__main__":
    print("Running local ingestion ...")
    lambda_handler()