'''
Purpose: Load processed cryptocurrency data into Snowflake.
'''

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

'''
Load environment variables
- No hard-coded credentials in scripts (security best practice)
- A '.env' file keeps secrets out of GitHub and allows easy environment management
'''
load_dotenv()

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')    # default warehouse
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'CRYPTO_DB')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')

'''
Connect to Snowflake

Returns:
    Snowflake connection object
'''
def get_connection():
    return snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA
    )

'''
Create Table

TIMESTAMP_NTZ (no time zone):
    - data source is already standardized (everything in UTC)
context manager with ... as ...:
    - automatically closes cursor once you leave the block (avoids resource leaks)
    - best practice in production ETL pipelines
'''
def create_table(conn):
    create_stmt = f'''
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.prices (
        coin_id STRING,
        timestamp_utc TIMESTAMP_NTZ,
        price_usd FLOAT,
        return FLOAT,
        sma_7 FLOAT
    )
    '''

    with conn.cursor() as cur:
        cur.execute(create_stmt)
        print("Table ensured in Snowflake.")

'''
Load data (CSV) into Snowflake

Best practice:
- Use df[['col1', 'col2', ...]].values.tolist() in production ETL pipelines
- Explicitly selecting columns ensures order matches table schema
- executemany() is faster for bulk inserts than looping execute() for each row
'''
def load_data(df, conn):
    insert_stmt = f'''
    INSERT INTO {SNOWFLAKE_SCHEMA}.prices
    (coint_id, timestamp_utc, price_usd, return, sma_7)
    VALUES (%s, %s, %s, %s, %s)
    '''

    data_tuples = df[['coin_id', 'timestamp_utc', 'price_usd', 'return', 'sma_7']].values.tolist()

    with conn.cursor() as cur:
        cur.executemany(insert_stmt, data_tuples)
        print(f"Loaded {len(df)} rows into Snowflake.")


'''
Main

- parse dates (pd.read_csv() by default loads timestamps as object)
'''
if __name__ == '__main__':
    # Load data (CSV)
    path = "data/processed/processed_crypto_data.csv"
    df = pd.read_csv(path, parse_dates = ['timestamp_utc'])
    # Connect to Snowflake
    conn = get_connection()
    # Create table if it doesn't exist
    create_table(conn)
    # Load data
    load_data(df, conn)
    # Close connection (best practice)
    conn.close()
    print('Snowflake connection closed.')