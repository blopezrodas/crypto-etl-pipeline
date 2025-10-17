# crypto-elt-pipeline

End-to-end **Cryptocurrency ELT Pipeline** built with Python, Docker, AWS S3, Snowflake, dbt, and Airflow.  
This project automates the extraction, storage, transformation, and orchestration of cryptocurrency data from the CoinGecko API.

---

## Project Goal

**Modern data engineering ELT workflow**, including:

- Extracting historical crypto prices and transactions from the CoinGecko API  
- Storing raw data in **AWS S3**  
- Loading data into **Snowflake** staging tables  
- Transforming data using **dbt** (calculations like volatility, moving averages)  
- Automating the pipeline with **Airflow** (or Prefect)  

---

## Tech Stack

- **Python**: ELT scripts, API requests, and data manipulation 
- **pandas / numpy**: Data cleaning and analysis  
- **requests**: API calls  
- **AWS S3 / boto3**: Cloud storage for raw data  
- **Snowflake / snowflake-connector-python**: Cloud data warehouse 
- **dbt / dbt-snowflake**: Transformations and metrics  
- **Airflow**: Orchestration of automated pipeline  
- **Jupyter notebooks**: Exploration and testing  
- **Docker / Docker Compose**: Containerization for reproducible environment

---

## Workflow

1. **Extract**: Pull cryptocurrency data from the CoinGecko API (`extract.py`)  
2. **Load to S3**: Upload raw data to AWS S3 (`load_s3.py`)  
3. **Load to Snowflake**: Copy S3 data into staging tables in Snowflake (`load_snowflake.py`)  
4. **Transform**: Run dbt models for calculated metrics like moving averages and volatility (`stg_crypto_prices.sql`, `fct_crypto_prices.sql`)  
5. **Orchestrate**: Automate daily pipeline runs with Airflow DAGs (`dags/crypto_etl_dag.py`)  

---

## Results / Outputs

- Raw and transformed cryptocurrency datasets stored in **AWS S3** and **Snowflake**  
- Key metrics computed: **moving averages**, **volatility**, and other aggregate trends 
- Demonstrates end-to-end **ELT data engineering workflow**, from ingestion → staging → transformation → orchestration
