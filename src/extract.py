import requests
import pandas as pd

'''
Fetch data for a single coin from CoinGecko API.

Parameters:
  coin (str): Coin ID
  vs_currency (str): Currency to price against
  days (int): Number of days to fetch data for
  interval (str): 'daily' or 'hourly'
Returns:
  pd.DataFrame: Timestamp (ms), price, coin, currency, extracted_at
'''
def fetch_data(coin = 'bitcoin', vs_currency = 'usd', days = 30, interval = 'daily'):
  # API endpoint URL
  url = f'https://api.coingecko.com/api/v3/coins/{coin}/market_chart'
  # Dictionary of query parameters to send to API
  params = {'vs_currency': vs_currency, 'days': days, 'interval': interval}
  # send HTTP GET request to CoinGecko API
  response = requests.get(url, params = params)
  # convert response from json to dictionary (CoinGecko returns JSON)
  data = response.json()
  # convert to dataframe
  # data['prices'] = [timestamp in ms, price]
  df = pd.DataFrame(data['prices'], columns = ['timestamp', 'price'])
  # Add metadata
  df['coin'] = coin
  df['currency'] = vs_currency
  df['extracted_at'] = pd.Timestamp.utcnow()
  return df

'''
Fetch data for multiple coins from CoinGecko API.

Parameters:
  coins (list): List of coin IDs
  vs_currency (str): Currency to price against
  days (int): Number of days to fetch data for
  interval (str): 'daily' or 'hourly'
Returns:
  pd.DataFrame: Timestamp (ms), price (usd), coin
'''
def fetch_multicoin_data(coins, vs_currency = 'usd', days = 30, interval = 'daily'):
  data = [fetch_data(coin, vs_currency, days, interval) for coin in coins]
  return pd.concat(data, ignore_index = True)

'''
Save DataFrame to CSV (path + filename decided by Airflow)
'''
def save_data(df, filename, path):
  # cross-platform compatibility
  import os
  # Check if folder exists
  os.makedirs(os.path.dirname(path), exist_ok=True)
  # Save raw data
  df.to_csv(os.path.join(path, filename), index = False)

'''
Main
'''
if __name__ == '__main__':
  coins = ['bitcoin', 'ethereum', 'cardano']
  df = fetch_multicoin_data(coins)
  save_data(df, 'crypto_prices.csv', 'data/raw')