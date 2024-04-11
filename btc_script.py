"""
ETL (Extract, Transform, Load) Script - Bitcoin
"""

# %%

## Libraries
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

import json
import os
import pandas as pd
import requests
import yfinance as yf

# %%

## Part 0: Connect to PostgreSQL database
load_dotenv()

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")

# Connect to PostgreSQL database
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# %%

## Part 1: 90 days of Bitcoin Data compared to NDX and Gold (with Normalization)
response = requests.get("https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=90&interval=daily", \
                        timeout=10)
content = response.content
data = json.loads(content)

btc_total_volumes_90d = pd.DataFrame(data['total_volumes'], columns=['time', 'vol_24h'])
btc_total_volumes_90d['vol_24h'] = btc_total_volumes_90d['vol_24h'].apply(lambda x: format(x, ','))
btc_total_volumes_90d['time'] = pd.to_datetime(btc_total_volumes_90d['time'] / 1000, unit='s',\
                                               origin='unix', utc=True)

btc_price_90d = pd.DataFrame(data['prices'], columns=['time', 'price'])
btc_price_90d['price'] = btc_price_90d['price'].apply(lambda x: format(x, ','))
btc_price_90d['time'] = pd.to_datetime(btc_price_90d['time'] \
                                       / 1000, unit='s', origin='unix', utc=True)

btc_marketcap_90d = pd.DataFrame(data['market_caps'], columns=['time', 'marketcap'])
btc_marketcap_90d['marketcap'] = btc_marketcap_90d['marketcap'].apply(lambda x: format(x, ','))
btc_marketcap_90d['time'] = pd.to_datetime(btc_marketcap_90d['time'] / 1000, unit='s', \
                                           origin='unix', utc=True)

# Join market cap, price, and volume
btc_90d = btc_price_90d.merge(btc_marketcap_90d, on='time').merge(btc_total_volumes_90d, on='time')

# Create date column on the merged dataframe
btc_90d['Date'] = btc_90d['time'].dt.date

end_date = btc_90d['Date'].max()
start_date = btc_90d['Date'].min()

# Get NDX and Gold data from Yahoo Finance
NDX_90d = yf.download("^NDX", start=start_date, end=end_date)
gold_90d = yf.download("GC=F", start=start_date, end=end_date)

NDX_90d.reset_index(inplace=True)
gold_90d.reset_index(inplace=True)

#Convert 'Date' column to datetime format in NDX_90d and gold_90d
NDX_90d['Date'] = pd.to_datetime(NDX_90d['Date'])
gold_90d['Date'] = pd.to_datetime(gold_90d['Date'])

# Convert 'Date' column to datetime format in btc_90d
btc_90d['Date'] = pd.to_datetime(btc_90d['Date'])

# Merge the data
btc_90d_w_external = btc_90d.merge(NDX_90d[['Date', 'Volume', 'Adj Close']], on='Date', how='right')
btc_90d_w_external = btc_90d_w_external.merge(gold_90d[['Date', 'Volume', 'Adj Close']], on='Date',\
                                              how='left', suffixes=('_NDX', '_gold'))

# Change data type for consistency
btc_90d_w_external['price'] = btc_90d_w_external['price'].str.replace(',', '').astype(float)
btc_90d_w_external['marketcap'] = btc_90d_w_external['marketcap'].str.replace(',', '').astype(float)
btc_90d_w_external['vol_24h'] = btc_90d_w_external['vol_24h'].str.replace(',', '').astype(float)

# Rename the column names
btc_90d_w_external = btc_90d_w_external.rename(columns={
    'Adj Close_NDX': 'ndx_price',
    'Volume_NDX': 'ndx_volume',
    'Adj Close_gold': 'gold_price',
    'Volume_gold': 'gold_volume'
})

# Remove the last row of the dataframe to match the length of filled data
btc_90d_w_external = btc_90d_w_external.iloc[:-1]

# Normalize the price between btc, nasdaq, and gold
btc_90d_w_external['btc_price_normalized'] = btc_90d_w_external['price'] \
                                         / btc_90d_w_external['price'].iloc[0]
btc_90d_w_external['ndx_price_normalized'] = btc_90d_w_external['ndx_price'] \
                                         / btc_90d_w_external['ndx_price'].iloc[0]
btc_90d_w_external['gold_price_normalized'] = btc_90d_w_external['gold_price'] \
                                         / btc_90d_w_external['gold_price'].iloc[0]


# %%

## Part 2: Median Tx Fee from Mempool for the Past 1 Month (each row represents a group of blocks - per 3 blocks)
def mempool_get_1m_avg_fee():
    """Fetch average fee rates for the past 1 month"""
    url = "https://mempool.space/api/v1/mining/blocks/fee-rates/1m"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError("Failed to fetch 1 Month average fee rates!")

mempool_data_1m_btc_fee = mempool_get_1m_avg_fee()

# Convert to DataFrame for easier handling and analysis
df_mempool_data_1m_btc_fee = pd.DataFrame(mempool_data_1m_btc_fee)

# Extract the median fee, which is 'avgFee_50', along with timestamps and average block heights for reference
btc_1m_mempool_fee = df_mempool_data_1m_btc_fee[['avgHeight', 'timestamp', 'avgFee_50']].copy()

# Converting timestamp to readable date format
btc_1m_mempool_fee['time'] = pd.to_datetime(btc_1m_mempool_fee['timestamp'], unit='s')


# %%

## Part 3: Mining Pools Data from Mempools
bitcoin_mempool = requests.get("https://mempool.space/api/v1/mining/pools/1m")
data = bitcoin_mempool.json()

btc_mining_pools = pd.DataFrame(data['pools']).drop(columns=['poolId', 'poolUniqueId'])


# %%

## Part 4: Developers and Community Data
bitcoin_details = requests.get("https://api.coingecko.com/api/v3/coins/bitcoin?localization=false&tickers=true&market_data=false&community_data=true&developer_data=true&sparkline=true", \
                               timeout=10)
bitcoin_details_data = bitcoin_details.json()

# developers_data
btc_developers_data = {
    "commit_count_4_weeks": bitcoin_details_data["developer_data"]["commit_count_4_weeks"],
    "forks": bitcoin_details_data["developer_data"]["forks"],
    "stars": bitcoin_details_data["developer_data"]["stars"],
    "subscribers": bitcoin_details_data["developer_data"]["subscribers"]
}
btc_developers_df = pd.DataFrame([btc_developers_data])

# community_data
btc_community_data = bitcoin_details_data["community_data"]
btc_community_df = pd.DataFrame(list(btc_community_data.items()), columns=["new_column", "V1"]).T
header = btc_community_df.iloc[0]
btc_community_df = btc_community_df[1:]
btc_community_df.columns = header
btc_community_df.reset_index(drop=True, inplace=True)

# combine developers and community data
btc_combined_data = pd.concat([btc_community_df[["twitter_followers"]], btc_developers_df], axis=1)

# rename columns and use melt to reshape the data frame
btc_combined_data.rename(columns={
    "forks": "github forks",
    "stars": "github stars",
    "subscribers": "github subscribers",
    "twitter_followers": "twitter followers"
}, inplace=True)

btc_combined_data = btc_combined_data.round(0).melt(var_name="Data", value_name="Value")


# %%

## Part 5: Bitcoin Stats

# Define an error handling function for clarity
def fetch_btc_stats():
    """Fetch Bitcoin stats from CoinGecko API"""
    try:
        response = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=90d&locale=en", \
                                timeout=10)
        response.raise_for_status()  # This will help to raise an HTTP Error if one exists

        data = response.json()

        if not data:
            print("Empty response from the API.")
            return None

        btc_stats = data[0]  # Extracting the first item for Bitcoin
        return btc_stats

    except requests.RequestException as req_exception:  # Catch any Request-related exceptions
        print(f"Error fetching data from API: {req_exception}")
        return None
    except (IndexError, TypeError, KeyError):
        print("Unexpected structure in API response or Bitcoin data not found.")
        return None

btc_stats = fetch_btc_stats()

if btc_stats is None:
    print("Failed to fetch Bitcoin stats.")
else:
    pass

# Selecting the required columns
columns = ["market_cap", "market_cap_rank", "fully_diluted_valuation", \
           "circulating_supply", "max_supply", "ath", "atl"]
btc_stats_1 = {col: btc_stats[col] for col in columns if col in btc_stats}

# Rename columns
column_rename = {
    "market_cap": "market cap",
    "market_cap_rank": "market cap rank",
    "fully_diluted_valuation": "fully diluted valuation",
    "circulating_supply": "circulating supply",
    "max_supply": "max supply",
    "ath": "ATH",
    "atl": "ATL"
}

btc_stats_1 = {column_rename[key] if key in column_rename \
               else key: value for key, value in btc_stats_1.items()}

# Converting the dictionary to DataFrame and then melting (similar to pivot_longer in R)
btc_stats_cleaned = pd.DataFrame([btc_stats_1]).melt(var_name="Data", value_name="Value").round(0)

# Since we already created `btc_combined_data`, concatenate both DataFrames
btc_combined_data_final = pd.concat([btc_stats_cleaned, btc_combined_data], ignore_index=True)


# %%

## Part 6: Exchanges (Spot) Data

# List of exchanges
exchange_names = ["binance", "gdax", "kraken", "bitstamp", "okex", "kucoin", \
                  "bitfinex", "huobi", "gemini"]

# Initialize an empty dataframe to store the final result
spot_exchanges_volume = None

# Loop through top exchanges
for exchange in exchange_names:
    url = f"https://api.coingecko.com/api/v3/exchanges/{exchange}/volume_chart?days=90"
    details = requests.get(url, timeout=10)
    details_data = details.json()
    print(details_data)

    # Convert to dataframe
    volume = pd.DataFrame(details_data, columns=["timestamp", f"{exchange}_vol_in_btc"])

    # Fix UNIX time stamp, data type, and decimal numbers
    volume["timestamp"] = volume["timestamp"].astype(float)
    volume["date_with_hour"] = pd.to_datetime(volume["timestamp"], unit='ms', \
                                              origin='unix', utc=True)
    volume["date"] = volume["date_with_hour"].dt.date
    volume[f"{exchange}_vol_in_btc"] = volume[f"{exchange}_vol_in_btc"]
    volume = volume[["date", f"{exchange}_vol_in_btc"]]

    # Merge with the final dataframe
    if spot_exchanges_volume is None:
        spot_exchanges_volume = volume
    else:
        spot_exchanges_volume = pd.merge(spot_exchanges_volume, volume, on="date", how="left")

# Convert the volume columns to float64
spot_exchanges_volume['binance_vol_in_btc'] = spot_exchanges_volume['binance_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['gdax_vol_in_btc'] = spot_exchanges_volume['gdax_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['kraken_vol_in_btc'] = spot_exchanges_volume['kraken_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['bitstamp_vol_in_btc'] = spot_exchanges_volume['bitstamp_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['okex_vol_in_btc'] = spot_exchanges_volume['okex_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['kucoin_vol_in_btc'] = spot_exchanges_volume['kucoin_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['bitfinex_vol_in_btc'] = spot_exchanges_volume['bitfinex_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['huobi_vol_in_btc'] = spot_exchanges_volume['huobi_vol_in_btc']\
                                          .astype('float64')
spot_exchanges_volume['gemini_vol_in_btc'] = spot_exchanges_volume['gemini_vol_in_btc']\
                                          .astype('float64')

# %%

## Part 7: Lightning Network Data from Mempool
response = requests.get("https://mempool.space/api/v1/lightning/statistics/3m", timeout=10)
data = response.json()

# Convert JSON data to DataFrame and select relevant columns
lightning_mempool_total_capacity = pd.DataFrame(data)[["added", "total_capacity", "channel_count"]]

# Fix UNIX time stamp
lightning_mempool_total_capacity["added"] = pd.to_datetime(\
                                     lightning_mempool_total_capacity["added"], \
                                     unit='s', origin='unix', utc=True)

# Check if values are 0 and delete the rows if they are
lightning_mempool_total_capacity = lightning_mempool_total_capacity[\
                                 (lightning_mempool_total_capacity[['total_capacity', \
                                                                'channel_count']] != 0).all(axis=1)]

# %%

## Part 8: Export Finished Dataframes to PostgreSQL tables
# Create an inspector
inspector = inspect(engine)

# Get table names
tables = inspector.get_table_names()

# List of dataframes and their corresponding base table names
dfs_and_tables = [
    (btc_90d_w_external, 'btc_90d_w_external'),
    (btc_1m_mempool_fee, 'btc_1m_mempool_fee'),
    (btc_mining_pools, 'btc_mining_pools'),
    (btc_combined_data_final, 'btc_combined_data_final'),
    (spot_exchanges_volume, 'spot_exchanges_volume'),
    (lightning_mempool_total_capacity, 'lightning_mempool_total_capacity')
]

# Loop through dataframes and their corresponding base table names
for df, base_table_name in dfs_and_tables:
    # Check if base table name exists
    if base_table_name in tables:
        # If it does, find the next available table name
        i = 2
        while f"{base_table_name}_{i}" in tables:
            i += 1
        # Use the next available table name
        table_name = f"{base_table_name}_{i}"
    else:
        table_name = base_table_name
    # Export DataFrame to the table
    df.to_sql(table_name, engine, if_exists='replace', index=False)
