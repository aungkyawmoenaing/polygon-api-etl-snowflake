import requests
import os
from dotenv import load_dotenv

load_dotenv()
polygon_api_key = os.getenv("polygon_api_key")
limit = 1000
url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={polygon_api_key}"
response = requests.get(url)

tickers = []
data = response.json()

for ticker in data['results']:
    tickers.append(ticker)

while 'next_url' in data:
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url']+ f"&apiKey={polygon_api_key}")
    data = response.json()
    print(data)
    for ticker in data["results"]:
        tickers.append(ticker)
print(len(tickers))