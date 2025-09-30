import pandas as pd

'''
Transform raw crypto data:
- Drop duplicated rows
- Convert data types
- Drop rows with missing values (original columns of raw dataset)
- Feature engineering
- Normalize for dbt/Snowflake
'''
def transform_data(df):
  # remove duplicates
  df = df.drop_duplicates(subset=['coin', 'timestamp'], keep = 'first')

  # convert timestamp (ms) to date format (coerce errors → NaT)
  df['timestamp'] = pd.to_datetime(df['timestamp'], unit = 'ms', errors = 'coerce')
  
  # Ensure price is numeric (coerce errors → NaN)
  df['price'] = pd.to_numeric(df['price'], errors = 'coerce')
  
  # remove missing values
  # rows are useless if any column values are missing
  # missing values in timestamp will prevent timeseries (most likely interested in this)
  # missing values in price will prevent any analysis
  # missing valus in coin will prevent any analysis
  df = df.dropna()

  # Add daily returns
  df = df.sort_values(by = ['coin', 'timestamp'])
  df['return'] = df.groupby('coin')['price'].pct_change()

  # Add moving averages
  # min_periods = 1 to avoid NAs
  df['sma_7'] = df.groupby('coin')['price'].transform(
    lambda x: x.rolling(window = 7, min_periods = 1).mean()
  )

  # Normalize column names
  df = df.rename(columns = {
    'coin': 'coin_id',
    'price': 'price_usd',
    'timestamp': 'timestamp_utc'
  })

  # Normalize categorical values
  df['coin_id'] = df['coin_id'].str.strip().str.lower()

  return df

'''
Save DataFrame to CSV (create directories if needed)
'''
def save_data(df, filename = 'processed_crypto_data.csv', path = 'data/processed'):
  # Check if folder exists
  import os
  os.makedirs(os.path.dirname(path), exist_ok=True)
  # Save processed data
  df.to_csv(path + '/' + filename, index = False)

'''
Main
'''
if __name__ == '__main__':
  raw_path = 'data/raw/raw_crypto_data.csv'
  df_raw = pd.read_csv(raw_path)
  df_transformed = transform_data(df_raw)
  save_data(df_transformed)