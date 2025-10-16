'''
Purpose: Load processed cryptocurrency data (from S3) into Snowflake.
'''

import os
import snowflake.connector
from dotenv import load_dotenv

'''
Load environment variables
- No hard-coded credentials in scripts (security best practice)
- A '.env' file keeps secrets out of GitHub and allows easy environment management
'''
# Load environment variables from .env
load_dotenv()
# Snowflake credentials
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')    # default warehouse
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'CRYPTO_DB')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
# AWS
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")  # Snowflake COPY INTO command pulls directly from S3
AWS_REGION = os.getenv("AWS_REGION")  # optional, needed if using S3 integration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


'''
Connect to Snowflake

Returns:
    Snowflake connection object

Notes:
    Having a get_connection() func let's us open one connection (main or Airflow task) and pass it to multiple functions.
    - Realistic for production pipelines, where you want to reuse the same session (cheaper, faster, cleaner logs).
    - Makes orchestration easier, because orchestrator (Airflow) controls when the connection is created and destroyed.
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
def create_table(conn, table_name):
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{table_name} (
        coin_id STRING,
        timestamp_ms BIGINT,
        price FLOAT,
        currency STRING,
        extracted_at TIMESTAMP_NTZ
    )
    """

    with conn.cursor() as cur:
        cur.execute(create_stmt)
        print("Table {SNOWFLAKE_SCHEMA}.{table_name} ensured in Snowflake.")

'''
Load data from S3 into Snowflake

Parameters:
    s3_key (str): Full path in S3 (e.g., raw/crypto/crypto_prices_20251013.csv)
    table_name (str): Snowflake staging table name
    file_format (str): File format (default "CSV")

Returns:
    bool: True if load succeeded, False otherwise

Notes:
COPY INTO with AWS credentials
    - Works without extra Snowflake setup (just needs .env AWS keys)
    - Great for a portfolio/demo project (can run end-to-end with only Python + Snowflake)
    - Not best practice in production (embedding AWS secrets in queries)
'''
def load_data(conn, s3_key, table_name, file_format="CSV"):
    s3_path = f"s3://{AWS_BUCKET_NAME}/{s3_key}"
    copy_stmt = f"""
        COPY INTO {SNOWFLAKE_SCHEMA}.{table_name}
        FROM '{s3_path}'
        CREDENTIALS=(
            AWS_KEY_ID='{AWS_ACCESS_KEY_ID}'
            AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}'
        )
        FILE_FORMAT = (TYPE={file_format} FIELD_OPTIONALLY_ENCLOSED_BY='"')
        ON_ERROR = 'ABORT_STATEMENT'
    """

    with conn.cursor() as cur:
        cur.execute(copy_stmt)
        print(f"Successfully loaded {s3_path} into {table_name}")


# Standalone testing/demo only (Airflow DAG will call functions directly)
if __name__ == '__main__':
    # Example S3 prefix / folder
    s3_prefix = "raw/crypto"
    filename = "crypto_prices_df.csv"
    s3_key = os.path.join(s3_prefix, filename)
    table_name = "crypto_prices_staging"
    # Connect to Snowflake
    conn = get_connection()
    # Create table if it doesn't exist
    create_table(conn, table_name)
    # Load data (assumes the file already exists in S3)
    load_data(conn, s3_key, table_name)
    # Close connection (best practice)
    conn.close()
    print('Snowflake connection closed.')