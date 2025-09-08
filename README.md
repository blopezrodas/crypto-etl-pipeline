# crypto-etl-pipeline

End-to-end **Cryptocurrency ETL Pipeline** built with Python, Docker, AWS S3, Snowflake, dbt, and Airflow.  
This project automates the extraction, storage, transformation, and orchestration of cryptocurrency data from the CoinGecko API.

---

## Project Goal

The goal of this project is to demonstrate a **modern data engineering workflow**, including:

- Extracting historical crypto prices and transactions from the CoinGecko API  
- Storing raw data in **AWS S3**  
- Loading data into **Snowflake** staging tables  
- Transforming data using **dbt** (calculations like volatility, moving averages)  
- Automating the pipeline with **Airflow** (or Prefect)  

---

## Tech Stack

- **Python**: ETL scripts, data manipulation  
- **pandas / numpy**: Data cleaning and analysis  
- **requests**: API calls  
- **AWS S3 / boto3**: Cloud storage for raw data  
- **Snowflake / snowflake-connector-python**: Data warehouse  
- **dbt / dbt-snowflake**: Transformations and metrics  
- **Airflow**: Orchestration of automated pipeline  
- **Jupyter notebooks**: Exploration and testing  
- **Docker / Docker Compose**: Containerization for reproducible environment

---

## Project Structure

crypto_etl_project/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── src/
│   ├── extract.py
│   ├── load_s3.py
│   ├── load_snowflake.py
│   ├── transform_dbt.py
│   └── etl_pipeline.py
├── notebooks/
├── dbt/
├── dags/
├── requirements.txt
├── README.md
└── .gitignore

---

## Workflow

1. **Extract**: Pull cryptocurrency data from the CoinGecko API (`extract.py`)  
2. **Load to S3**: Upload raw JSON/CSV files to AWS S3 (`load_s3.py`)  
3. **Load to Snowflake**: Copy S3 data into staging tables in Snowflake (`load_snowflake.py`)  
4. **Transform**: Run dbt models for calculated metrics like moving averages and volatility (`transform_dbt.py`)  
5. **Orchestrate**: Automate daily runs with Airflow DAGs (`etl_pipeline.py` + `dags/`)  

---

## Results / Outputs

- Raw and transformed cryptocurrency datasets stored in **AWS S3** and **Snowflake**  
- Key metrics computed: **moving averages**, **volatility**, and other aggregate trends  
- Optional dashboards provide interactive visualization of crypto market trends  
- Demonstrates end-to-end **data engineering workflow**, from ingestion to transformation to orchestration
