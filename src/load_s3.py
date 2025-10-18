import os                       # access environment variables & file paths
from dotenv import load_dotenv  # load environment variables
import boto3                    # AWS SDK
from io import StringIO         # in-memory buffer to hold CSV data
from botocore.exceptions import NoCredentialsError  # Handle missing AWS credentials
import pandas as pd

# ---------- S3 Notes ----------
# S3 path: s3://<bucket-name>/<key> equivalent to s3://{AWS_BUCKET_NAME}/{s3_key}
#   s3://           -> Prefix indicates that the path refers to an Amazon S3 resource.
#   <bucket-name>   -> S3 bucket name (must be globally unique across all of Amazon S3)
#   <key>           -> Full object path inside bucket (prefixes + filename)
#   prefix          -> folder or "directory" path inside bucket
# Example:
#   s3_prefix   = "raw/crypto"
#   filename    = "crypto_prices.csv"
#   s3_key      = os.path.join(s3_prefix, filename)
#   Uploads to: s3://my-bucket/raw/crypto/crypto_prices.csv
# ------------------------------


# Load environment variables from .env
load_dotenv()
# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# S3 client object
s3 = boto3.client(
    "s3",
    region_name = AWS_REGION,
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY
)


"""
Upload a local file to an S3 bucket.

Parameters:
    local_path (str): Path to local file
    bucket_name (str): Name of the S3 bucket
    s3_key (str): Full S3 key (path + filename)

Returns:
    bool: True if upload succeeded, False otherwise
"""
def upload_file_to_s3(local_path, bucket_name, s3_key):
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        return True
    except FileNotFoundError:
        print(f"Error: File {local_path} not found.")
        return False
    except NoCredentialsError:
        print("Error: AWS credentials not available.")
        return False


"""
Upload a Pandas DataFrame directly to S3 as a CSV file.
    
Parameters:
    df (pd.DataFrame): DataFrame to upload
    bucket_name (str): Name of the S3 bucket
    s3_key (str): Full S3 key (path + filename)
"""
def upload_df_to_s3(df, bucket_name, s3_key):
    # Create in-memory buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer as CSV
    df.to_csv(csv_buffer, index = False)
    # Upload to S3
    try:
        s3.put_object(Bucket = bucket_name, Key = s3_key, Body = csv_buffer.getvalue())
        return True
    except NoCredentialsError:
        print("Error: AWS credentials not available.")
        return False


# Standalone testing/demo only (Airflow DAG will call functions directly)
if __name__ == "__main__":
    # Example S3 prefix / folder
    s3_prefix = "raw/crypto"

    # Example local file upload
    local_path = "data/raw/crypto_prices.csv"
    s3_key_file = os.path.join(s3_prefix, "crypto_prices_file.csv")
    upload_file_to_s3(local_path, AWS_BUCKET_NAME, s3_key_file)

    # Example DataFrame direct upload
    df = pd.DataFrame({
        "timestamp": [1, 2, 3],
        "price": [100, 200, 300],
        "coin": ["bitcoin", "bitcoin", "bitcoin"],
        "currency": ["usd", "usd", "usd"],
        "extracted_at": pd.Timestamp.utcnow()
    })
    s3_key_df = os.path.join(s3_prefix, "crypto_prices_df.csv")
    upload_df_to_s3(df, AWS_BUCKET_NAME, s3_key_df)
