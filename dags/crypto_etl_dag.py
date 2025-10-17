"""
Crypto ETL Pipeline with Airflow
-------------------------------- 

Purpose:
    Reproducible ELT pipeline that fetches last 30 days of crypto data daily.
    Demonstrates modular Python tasks, XCom communication, and Airflow macros.

Workflow:
    - Extract: fetch raw crypto data and upload directly to S3
    - Load: load data into Snowflake staging table
    - Transform: run dbt models (staging + marts)
    - Test: run dbt tests
    - Communication: tasks share file paths via XComs
    - Reproducibility: filenames use Airflow macros (ds) for backfill safety

Airflow notes:
    - Airflow Python task 'def some_task(**context):'
    - '**context' is standard practice to access Xcoms (cross-communication between tasks)
    - '**context' is a dictionary of metadata that contains:
        - execution date
        - task instance (ti)
        - DAG run ID
        - logical run time
dbt notes:
    - cd {DBT_PROJECT_DIR}: point this to wherever dbt project lives inside the Airflow container or environment.
    - DBT_PROJECT_DIR can be overridden via environment variable
    - --profiles-dir .: makes sure dbt picks up profiles.yml config.
"""

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# DAG start_date expects a datetime object
from datetime import datetime, timedelta
# pipeline functions
from src.extract import fetch_multicoin_data
from src.load_s3 import upload_df_to_s3
from src.load_snowflake import get_connection, create_table, load_data

# dbt config
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")

# -------------------------------------------------------------------- 
# Airflow task functions
# --------------------------------------------------------------------

def extract_task(ds, **context):
    """
    Extracts crypto data from CoinGecko API and upload to S3.
    
    Parameters:
        ds (str): Airflow execution date in YYYY-MM-DD format (provided by Airflow)
        **context (dict): Airflow runtime context metadata.
    Returns:
        str: S3 key (pushed to XCom for downstream tasks).
    """
    coins = ['bitcoin', 'ethereum', 'cardano']
    df = fetch_multicoin_data(coins, days=30, interval='daily')
    s3_prefix = "raw/crypto"
    filename = f"crypto_prices_{ds}.csv"    # make the DAG backfill-safe with Airflow's ds macro
    s3_key = os.path.join(s3_prefix, filename)
    upload_df_to_s3(df, os.getenv("AWS_BUCKET_NAME"), s3_key)
    # Push path to XCom for downstream tasks
    context['ti'].xcom_push(key='s3_key', value=s3_key)
    # Return path (useful for testing outside Airflow)
    return s3_key


def load_snowflake_task(**context):
    """
    Load crypto data from S3 into Snowflake.

    Parameters:
        **context (dict): Airflow runtime context metadata.
    Returns:
        None
    """
    s3_key = context['ti'].xcom_pull(task_ids='extract', key='s3_key')
    conn = get_connection()
    table_name = "crypto_prices_staging"
    create_table(conn, table_name)
    load_data(conn, s3_key, table_name)
    conn.close()

# --------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------

# Default task parameters (applied to all tasks in the DAG)
default_args = {
    'owner': 'airflow',                     # task owner
    'retries': 1,                           # number of retry attempts
    'retry_delay': timedelta(minutes=5),    # wait time between retries
}

with DAG(
    dag_id = 'crypto_elt_pipeline',
    default_args = default_args,
    description = "End-to-end crypto ELT pipeline (CoinGecko → S3 → Snowflake → dbt)",
    start_date = datetime(2025, 1, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Step 1: Extract and upload directly to S3
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_task
    )

    # Step 2: Load from S3 → Snowflake
    load_snowflake = PythonOperator(
        task_id = 'load',
        python_callable = load_snowflake_task
    )

    # Step 3: Run dbt models
    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
        # dbt can fail if Snowflake is busy
        retries = 2,
        retry_delay = timedelta(minutes=3)
    )

    # Step 4: Run dbt tests
    dbt_test = BashOperator(
        task_id = 'dbt_test',
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir ."
    )

    # Task dependencies
    extract >> load_snowflake >> dbt_run >> dbt_test