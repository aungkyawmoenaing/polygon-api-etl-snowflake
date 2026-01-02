import time
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector

def run_stock_job():
    load_dotenv()
    POLYGON_API_KEY = os.getenv("polygon_api_key")
    limit = 1000

    # get today's date
    DS = datetime.now().strftime('%Y-%m-%d')

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

        # collect tickers and add DS field
        for ticker in data.get("results", []):
            ticker['ds'] = DS
            tickers.append(ticker)

        # move to next page
        next_url = data.get("next_url")
        if next_url:
            url = next_url + f"&apiKey={POLYGON_API_KEY}"
            time.sleep(0.5)  # small delay to avoid hitting API limit
        else:
            url = None  # no more pages

    # Determine fieldnames from first row if available
    if tickers:
        fieldnames = list(tickers[0].keys())
        load_to_snowflake(tickers, fieldnames)
        print(f'Loaded {len(tickers)} rows to Snowflake')
    else:
        print("No tickers fetched")


def load_to_snowflake(rows, fieldnames):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }

    table_name = os.getenv('SNOWFLAKE_TABLE', 'apitosnowflake')

    # Type overrides for Snowflake columns
    type_overrides = {
        'ticker': 'VARCHAR',
        'name': 'VARCHAR',
        'market': 'VARCHAR',
        'locale': 'VARCHAR',
        'primary_exchange': 'VARCHAR',
        'type': 'VARCHAR',
        'active': 'BOOLEAN',
        'currency_name': 'VARCHAR',
        'cik': 'VARCHAR',
        'composite_figi': 'VARCHAR',
        'share_class_figi': 'VARCHAR',
        'last_updated_utc': 'TIMESTAMP_NTZ',
        'ds': 'VARCHAR'
    }

    # Connect to Snowflake
    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        cs = conn.cursor()
        try:
            # Create table if not exists
            columns_sql_parts = []
            for col in fieldnames:  # <- fixed for loop syntax
                col_type = type_overrides.get(col, 'VARCHAR')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')
            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ( ' + ', '.join(columns_sql_parts) + ' )'
            cs.execute(create_table_sql)

            # Insert rows
            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join([f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( {placeholders} )'

            transformed = []
            for t in rows:
                row = {k: t.get(k, None) for k in fieldnames}
                transformed.append(row)

            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()


if __name__ == '__main__':
    run_stock_job()