import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3 as sql
from IPython.display import clear_output

# Import data from the historical_prices table

conn = sql.connect("crypto_data.db")
data = pd.read_sql("SELECT * FROM historical_prices_raw;", conn)
conn.close()

all_coins = data['coin'].unique().tolist()

# Remove coins missing all market cap information
df = data.fillna({
    'market_cap' : 0
}).groupby('coin', as_index = False).agg({
    'market_cap' : 'max'
})

coins_with_no_market_cap = df[df['market_cap'] == 0]['coin'].tolist()

data = data[~data['coin'].isin(coins_with_no_market_cap)]

# Remove coins missing market cap on most recent day
today = max(data['date'])
coins_missing_todays_market_cap = data[(data['market_cap'].isnull()) & (data['date'] == today)]['coin'].tolist()
data = data[~data['coin'].isin(coins_missing_todays_market_cap)]

max_btc_price = max(data[data['coin'] == 'bitcoin']['high'])
coins_with_large_prices = data[data['open'] > max_btc_price]['coin'].unique().tolist()
data = data[~data['coin'].isin(coins_with_large_prices)]

# Estimate daily market cap

while len(data[data['market_cap'].isnull()]) != 0:
    prices = data[['coin', 'date', 'close', 'market_cap']].copy()
    prices['shifted_close'] = prices.groupby(['coin'])['close'].shift(-1)
    prices['close_change'] = (prices['shifted_close'] - prices['close']) / prices['shifted_close']
    prices['market_cap_est'] = prices['market_cap'] + (prices['market_cap'] * prices['close_change'])
    prices['market_cap_est'] = prices.groupby(['coin'])['market_cap_est'].shift(1)

    data = data.merge(prices[['coin', 'date', 'market_cap_est']], 'left', on = ['coin', 'date'])
    data['market_cap'] = data['market_cap'].combine_first(data['market_cap_est'])
    del data['market_cap_est']
    clear_output()
    print("{}% complete".format(round((len(data[data['market_cap'].notnull()]) / len(data)) * 100, 2)))
    
# Save to new table in database
conn = sql.connect('crypto_data.db')
data.to_sql('historical_prices_clean', conn, if_exists = 'replace', index = False)
conn.close()