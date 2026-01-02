import time
import requests
import os
import csv
from dotenv import load_dotenv

load_dotenv()
POLYGON_API_KEY = os.getenv("polygon_api_key")
limit = 1000

# first page
url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={POLYGON_API_KEY}"

tickers = []
while url:
    print("Requesting:", url)
    response = requests.get(url)
    data = response.json()

    # check for API errors
    if data.get("status") == "ERROR":
        print("Error:", data.get("error"))
        print("Waiting 60 seconds before retry...")
        time.sleep(60)
        continue  # retry same page
		
	# collect tickers
    tickers.extend(data.get("results", []))

    # move to next page
    next_url = data.get("next_url")
    if next_url:
        url = next_url + f"&apiKey={POLYGON_API_KEY}"
        time.sleep(0.5)  # small delay between requests to avoid limit
    else:
        url = None  # no more pages

# Write tickers to CSV
csv_filename = "tickers.csv"
fieldnames = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active', 'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc']

with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in tickers:
        writer.writerow(ticker)

print(f"Written {len(tickers)} tickers to {csv_filename}")