import requests
import os
import csv
import time
from datetime import date
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

# Snowflake connector
import snowflake.connector

def _clean(v):
    return None if v is None else v.strip().strip('"').strip("'")

# Read Snowflake config from env (strip quotes/spaces)
SF_ACCOUNT = _clean(os.getenv("SNOWFLAKE_ACCOUNT"))
SF_USER = _clean(os.getenv("SNOWFLAKE_USER"))
SF_PASSWORD = _clean(os.getenv("SNOWFLAKE_PASSWORD"))
SF_WAREHOUSE = _clean(os.getenv("SNOWFLAKE_WAREHOUSE"))
SF_DATABASE = _clean(os.getenv("SNOWFLAKE_DATABASE"))
SF_SCHEMA = _clean(os.getenv("SNOWFLAKE_SCHEMA"))
SF_ROLE = _clean(os.getenv("SNOWFLAKE_ROLE"))
SF_TABLE = os.getenv("SNOWFLAKE_TABLE", "stock_tickers")

def fetch_all_tickers(api_key, limit=1000, max_retries=6):
    tickers = []
    next_url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={api_key}'
    retries = 0
    while next_url:
        print("requesting:", next_url)
        resp = requests.get(next_url)
        try:
            data = resp.json()
        except ValueError:
            print("Non-JSON response:", resp.text)
            break

        # handle API-level errors (rate limit, etc.)
        if resp.status_code != 200 or data.get("status") == "ERROR":
            err_msg = data.get("error") or resp.text
            print("API error:", err_msg)
            if "exceeded the maximum requests per minute" in str(err_msg).lower() or resp.status_code == 429:
                retries += 1
                if retries > max_retries:
                    raise RuntimeError("Rate limited: exceeded max retries")
                sleep_seconds = min(60, 2 ** retries)
                print(f"Rate limited, sleeping {sleep_seconds}s (retry {retries}/{max_retries})")
                time.sleep(sleep_seconds)
                continue
            else:
                raise RuntimeError(f"API error: {err_msg}")

        retries = 0
        results = data.get("results")
        if results:
            tickers.extend(results)
        else:
            # if first page has no results, break
            if not tickers:
                print("No 'results' returned, response:", data)
            break

        next_url = data.get("next_url")
        if next_url:
            # polygon next_url often requires apiKey appended
            next_url = next_url + f"&apiKey={api_key}"
            time.sleep(1)  # small pause to avoid rate limits

    return tickers

def write_csv(tickers, example_ticker, output_csv="tickers.csv"):
    fieldnames = list(example_ticker.keys())
    with open(output_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            row = {}
            for k in fieldnames:
                if k == "ds":
                    last = t.get("last_updated_utc")
                    row["ds"] = last.split("T")[0] if last else date.today().isoformat()
                else:
                    row[k] = t.get(k, "")
            writer.writerow(row)
    print(f"Wrote {len(tickers)} rows to {output_csv}")

def write_snowflake(tickers, example_ticker):
    if not SF_ACCOUNT or not SF_USER or not SF_PASSWORD:
        print("Snowflake credentials not fully set in environment. Skipping Snowflake load.")
        return

    fieldnames = list(example_ticker.keys())

    # connect
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE
    )
    cur = conn.cursor()

    # create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SF_TABLE} (
        ticker VARCHAR,
        name VARCHAR,
        market VARCHAR,
        locale VARCHAR,
        primary_exchange VARCHAR,
        type VARCHAR,
        active BOOLEAN,
        currency_name VARCHAR,
        cik VARCHAR,
        composite_figi VARCHAR,
        share_class_figi VARCHAR,
        last_updated_utc TIMESTAMP_TZ(9),
        ds DATE
    )
    CLUSTER BY (ds)
    """
    cur.execute(create_table_sql)

    rows = []
    for t in tickers:
        vals = []
        for col in fieldnames:
            if col == "ds":
                last = t.get("last_updated_utc")
                v = last.split("T")[0] if last else date.today().isoformat()
            else:
                v = t.get(col, None)
                if v == "":
                    v = None
            vals.append(v)
        rows.append(tuple(vals))

    if rows:
        cols = ", ".join(fieldnames)
        # wrap last_updated_utc with TO_TIMESTAMP_TZ and ds with TO_DATE
        placeholders = []
        for col in fieldnames:
            if col == "last_updated_utc":
                placeholders.append("TO_TIMESTAMP_TZ(%s)")
            elif col == "ds":
                placeholders.append("TO_DATE(%s)")
            else:
                placeholders.append("%s")
        placeholders_sql = ", ".join(placeholders)
        insert_sql = f"INSERT INTO {SF_TABLE} ({cols}) VALUES ({placeholders_sql})"
        cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"Wrote {len(rows)} rows to Snowflake table {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
    else:
        print("No rows to write to Snowflake")

    cur.close()
    conn.close()

def run_stock_job():
    if not API_KEY:
        print("POLYGON_API_KEY not set in environment.")
        return

    tickers = fetch_all_tickers(API_KEY, LIMIT)

    example_ticker = {
        'ticker': 'ZWS',
        'name': 'Zurn Elkay Water Solutions Corporation',
        'market': 'stocks',
        'locale': 'us',
        'primary_exchange': 'XNYS',
        'type': 'CS',
        'active': True,
        'currency_name': 'usd',
        'cik': '0001439288',
        'composite_figi': 'BBG000H8R0N8',
        'share_class_figi': 'BBG001T36GB5',
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'ds': '2025-09-26'
    }

    # write CSV (optional) and Snowflake
    write_csv(tickers, example_ticker)
    write_snowflake(tickers, example_ticker)

if __name__ == "__main__":
    run_stock_job()