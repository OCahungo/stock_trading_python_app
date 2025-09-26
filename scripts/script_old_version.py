import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")

print(f"API Key: {API_KEY}")

limit = 100

import time

def fetch_all_tickers(api_key, limit=1000, max_retries=5):
    tickers = []
    next_url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={api_key}'
    retries = 0
    while next_url:
        print("requesting:", next_url)
        response = requests.get(next_url)
        try:
            data = response.json()
        except ValueError:
            print("Non-JSON response:", response.text)
            break

        # Handle API-level errors (rate limit, etc.)
        if response.status_code != 200 or data.get('status') == 'ERROR':
            err_msg = data.get('error') or response.text
            print("API error:", err_msg)
            if "exceeded the maximum requests per minute" in str(err_msg).lower():
                retries += 1
                if retries > max_retries:
                    raise RuntimeError("Rate limited: exceeded max retries")
                sleep_seconds = min(60, 2 ** retries)  # exponential backoff bounded to 60s
                print(f"Rate limited, sleeping {sleep_seconds}s (retry {retries}/{max_retries})")
                time.sleep(sleep_seconds)
                continue
            else:
                raise RuntimeError(f"API error: {err_msg}")

        retries = 0  # reset after successful request

        results = data.get('results')
        if not results:  # defensive: avoid KeyError
            print("No 'results' in response, full response:", data)
            break

        tickers.extend(results)

        next_url = data.get('next_url')
        if next_url:
            # polygon next_url often requires apiKey appended
            next_url = next_url + f'&apiKey={api_key}'
            # avoid exceeding requests per minute
            time.sleep(1)

    return tickers

tickers = fetch_all_tickers(API_KEY, limit)

example_ticker = { 
    'ticker': 'AMX',
    'name': 'America Movil S.A.B de C.V American Depositary Shares (each representing the right to receive twenty (20) Series B Shares)',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'XNYS',
    'type': 'ADRC',
    'active': True,
    'currency_name': 'usd',
    'cik': '0001129137',
    'composite_figi': 'BBG01FRH5MK2',
    'share_class_figi': 'BBG01FRH5ND8',
    'last_updated_utc': '2025-09-15T06:04:58.614804883Z'
}

# Write to CSV
fieldnames = list(example_ticker.keys())
csv_filename = "tickers.csv"
with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in tickers:
        row = {key: ticker.get(key, "") for key in fieldnames}
        writer.writerow(row)

print(f"Data written to {csv_filename}")
