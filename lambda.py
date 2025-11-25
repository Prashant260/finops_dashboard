#!/usr/bin/env python3

if len(keys) > 3:
    # tag key will be something like 'user:Environment' depending on how CE returns it
    tag_value = keys[3]
    tag_key = 'Environment'  # the caller controlled which tag_key was used; this script uses tag per fetch

metrics = group.get('Metrics', {})
unblended = float(metrics.get('UnblendedCost', {}).get('Amount', 0.0)) if metrics.get('UnblendedCost') else None
amortized = float(metrics.get('AmortizedCost', {}).get('Amount', 0.0)) if metrics.get('AmortizedCost') else None
usage_qty = float(metrics.get('UsageQuantity', {}).get('Amount', 0.0)) if metrics.get('UsageQuantity') else None

unit = None
if metrics.get('UnblendedCost'):
    unit = metrics['UnblendedCost'].get('Unit')

batch.append(
    (start, end, service, usage_type, linked_account,
     tag_key, tag_value, unblended, amortized, usage_qty, unit)
)

if len(batch) >= 500:
    cur.executemany(insert_sql, batch)
    conn.commit()
    batch = []

if batch:
    cur.executemany(insert_sql, batch)
    conn.commit()

cur.close()
conn.close()


def run_ingest(days=7):
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days)

    logger.info('Fetching cost data from %s to %s', start_date, end_date)

    # Fetch without tags first (tag_key=None) to get base groups
    data = fetch_cost_data(str(start_date), str(end_date), tag_key=None)
    store_cost_data(data)

    # Fetch per-tag so we can capture tag breakdowns
    for tag_key in TAG_KEYS:
        logger.info('Fetching cost data for tag: %s', tag_key)
        data = fetch_cost_data(str(start_date), str(end_date), tag_key=tag_key)
        store_cost_data(data)

    logger.info('Ingest complete')


if __name__ == '__main__':
    # Optional CLI days parameter via env
    days = int(os.getenv('INGEST_DAYS', '7'))
    run_ingest(days=days)
