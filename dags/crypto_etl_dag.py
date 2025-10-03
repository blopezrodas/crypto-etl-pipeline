"""
Crypto ETL Pipeline with Airflow
-------------------------------- 

Purpose:
    Reproducible ETL pipeline that fetches last 30 days of crypto data daily.
    Demonstrates modular Python tasks, XCom communication, and Airflow macros.

Workflow:
    - Extract: fetch raw crypto data and save as CSV
    - Transform: clean and format the data
    - Load: insert processed data into Snowflake
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
"""

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
# DAG start_date expects a datetime object
# 
from datetime import datetime, timedelta
# pipeline functions
from src.extract import fetch_multicoin_data, save_data as save_raw_data
from src.transform import transform_data, save_data as save_processed_data
from src.load_snowflake import get_connection, create_table, load_data

# -------------------------------------------------------------------- 
# Airflow task functions
# --------------------------------------------------------------------

def extract_task(ds, **context):
    """
    Extracts crypto data from CoinGecko API and save raw CSV.
    
    Parameters:
        ds (str): Airflow execution date in YYYY-MM-DD format (provided by Airflow)
        **context (dict): Airflow runtime context metadata.
    Returns:
        str: Filepath of the saved raw data CSV (pushed to XCom for downstream tasks).
    """
    coins = ['bitcoin', 'ethereum', 'cardano']
    df = fetch_multicoin_data(coins, days=30, interval='daily')
    # make the DAG backfill-safe with Airflow's ds macro
    filename = f"crypto_raw_{ds}.csv"
    save_raw_data(df, filename=filename, path='data/raw')
    # Push path to XCom for downstream tasks
    context['ti'].xcom_push(key='path_raw', value=f'data/raw/{filename}')
    # Return path (useful for testing outside Airflow)
    return f'data/raw/{filename}'


def transform_task(ds, **context):
    """
    Transform raw crypto data and save processed CSV.

    Parameters:
        ds (str): Airflow execution date in YYYY-MM-DD format
        **context (dict): Airflow runtime context metadata.
    Returns:
        str: Filepath of the saved processed data CSV (pushed to XCom for downstream tasks).
    """
    # Get path from extract (avoids hardcoding)
    # xcom_pull() = "pull something that another task pushed"
    # task_ids='extract' = “give me whatever value the extract task returned”
    path = context['ti'].xcom_pull(task_ids='extract', key='path_raw')
    df = pd.read_csv(path)
    df_processed = transform_data(df)
    # make the DAG backfill-safe with Airflow's ds macro
    filename = f"crypto_processed_{ds}.csv"
    save_processed_data(df_processed, filename=filename, path='data/processed')
    context['ti'].xcom_push(key='path_processed', value=f'data/processed/{filename}')
    # Return path (useful for testing outside Airflow)
    return f'data/processed/{filename}'

def load_task(**context):
    """
    Load processed crypto data into Snowflake.

    Parameters:
        **context (dict): Airflow runtime context metadata.
    Returns:
        None
    """
    path = context['ti'].xcom_pull(task_ids='transform', key='path_processed')
    df = pd.read_csv(path, parse_dates=['timestamp_utc'])
    conn = get_connection()
    create_table(conn)
    load_data(df, conn)
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
    dag_id = 'crypto_etl_pipeline',
    default_args = default_args,
    start_date = datetime(2025, 1, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_task
    )

    transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform_task
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = load_task
    )

    # Task dependencies
    extract >> transform >> load