import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")

print(f"API Key: {API_KEY}")

LIMIT = 1000

def run_stock_job():
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}'
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={API_KEY}')
        data = response.json()
        print(data)
        for ticker in data['results']:
            tickers.append(ticker)

    example_ticker =  {'ticker': 'ZWS', 
        'name': 'Zurn Elkay Water Solutions Corporation', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNYS', 
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001439288', 
        'composite_figi': 'BBG000H8R0N8', 	'share_class_figi': 'BBG001T36GB5', 	'last_updated_utc': '2025-09-11T06:11:10.586204443Z'}

    # Write tickers to CSV with example_ticker schema
    fieldnames = list(example_ticker.keys())
    output_csv = 'tickers.csv'
    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in fieldnames}
            writer.writerow(row)
    print(f'Wrote {len(tickers)} rows to {output_csv}')

if __name__ == '__main__':
    run_stock_job()