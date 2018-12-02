# Crypto-Market-Analysis

## File Descriptions
* `crypto_data.db` This SQLite database contains historical pricing for each coin given by Coin Market Cap. This file has price data as of December 1, 2018 EST.
* `fetch_data.py` Contains the code needed to fetch updated data from Coin Market Cap
* `clean_data.py` Contains the coded needed to clean the raw data in the historical_prices_raw table
* `Clean Data.ipynb` Is a visual walkthrough of the steps to clean the data



## Set Up Instructions
1. After downloading the files, run `python fetch_data.py` in your terminal to get updated price information. Warning, this step takes 3 hours to complete. 
2. To clean the data, run `python clean_data.py` in your terminal. This creates / updates the historical_prices_clean table within the crypto_data databases with clean data
3. Happy coding!
