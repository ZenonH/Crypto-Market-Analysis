# Import packages
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import sys
from IPython.display import clear_output
import sqlite3

# Get all coin listings
listings = pd.DataFrame(requests.get(url = "https://api.coinmarketcap.com/v2/listings/").json()['data']).set_index("symbol")
all_coins = listings['website_slug'].tolist()

def get_coin_data(coin):
    url = "https://coinmarketcap.com/currencies/{}/historical-data/?start=20130101&end=20181202".format(coin)
    content = requests.get(url).content
    soup = BeautifulSoup(content,'html.parser')
    table = soup.find('table', {'class': 'table'})
    data = [[td.text.strip() for td in tr.findChildren('td')] 
        for tr in table.findChildren('tr')]
    df = pd.DataFrame(data)
    df.drop(df.index[0], inplace=True) # first row is empty
    df[0] =  pd.to_datetime(df[0]) # date
    for i in range(1,7):
        df[i] = pd.to_numeric(df[i].str.replace(",","").str.replace("-","")) # some vol is missing and has -
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'market_cap']
    df['coin'] = coin
    
    return df

# Loop through all coins and store historical prices
historical_prices = pd.DataFrame()
coins = list(all_coins)
length = len(coins)

error = []
print("Fetching Historical Prices\n")
for count, coin in enumerate(coins):
    try:
        historical_prices = historical_prices.append(get_coin_data(coin))
    except:
        # Sleep for a minute and try again
        time.sleep(60)
        try:
            historical_prices = historical_prices.append(get_coin_data(coin))
        except:
            error.append(coin)


    sys.stdout.write("\r" + str(round((count + 1) * 100/ length, 2)) + '% complete')
    sys.stdout.flush()

print()

# Save data to database
# Note: Any previous data is completely overwritten
conn = sqlite3.connect('crypto_data.db')
historical_prices.to_sql('historical_prices_raw', conn, 
                         if_exists = 'replace', index = False)
conn.close()

clear_output()
if len(error) != 0:
    print("Done. {} coins could not be imported".format(len(error)))
    for e in error:
        print(' - ' + e)
else:
    print("Done")
