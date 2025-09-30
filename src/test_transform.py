'''
TEST

Purpose: Test/Validate transformed dataset

- Check schema (column names, dtypes)
- Ensure no nulls in keyfields (coin_id, timestamp_utc, price_usd)
- Ensure consistency (e.g., timestamps in UTC and sorted, no negative prices)
'''

import numpy as np
import pandas as pd

if __name__ == '__main__':
  # parse dates (pd.read_csv() by default loads timestamps as object)
  df = pd.read_csv("data/processed/processed_crypto_data.csv", parse_dates=["timestamp_utc"])
  
  # Check column names
  expected_columns = ['coin_id', 'timestamp_utc', 'price_usd', 'sma_7', 'return']
  missing_columns = [col for col in expected_columns if col not in df.columns]
  extra_columns = [col for col in df.columns if col not in expected_columns]
  assert missing_columns == []
  assert extra_columns == []

  # Check duplicate rows
  essential_columns = ['coin_id', 'timestamp_utc', 'price_usd']
  num_duplicated = df.duplicated(subset=essential_columns).sum()
  assert  num_duplicated == 0, f'df has {num_duplicated} duplicate rows, expected 0'

  # Check data types
#   Sometimes float64 vs float32 may vary (np.issubdtype for flexibility)
  expected_dtypes = {
      'coin_id': 'object', 
      'timestamp_utc': 'datetime64[ns]', 
      'price_usd': 'float64', 
      'sma_7': 'float64', 
      'return': 'float64'
  }
  for col, dtype in expected_dtypes.items():
    assert np.issubdtype(df[col].dtype, np.dtype(dtype)), f'{col} has dtype {df[col].dtype}, expected {dtype}'

  # Check nulls
  for col in essential_columns:
    num_missing = df[col].isna().sum()
    assert num_missing == 0, f'Missing values found in {col}: {num_missing}'

  # Check timestamp
  # x.diff() to calculate differece between consecutive timestamps (current-prev)
  num_timestamp_issues = df.groupby('coin_id')['timestamp_utc'].apply(lambda x: (x.diff() < pd.Timedelta(0)).sum())
  total_issues = num_timestamp_issues.sum()
  assert total_issues == 0, f'Timestamp issues found: {num_timestamp_issues}'

  # Check prices
  neg_prices = (df['price_usd'] < 0).sum()
  assert neg_prices == 0, f'Negative prices: {neg_prices}'


