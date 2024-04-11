"""
ETL (Extract, Transform, Load) Script - BNB
"""

# %%

## Libraries
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

import json
import os
import pandas as pd
import requests

# %%

## Part 0: Connect to environment variables
load_dotenv()

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")

OWLRACLE_API_KEY = os.environ.get("OWLRACLE_API")

# Connect to PostgreSQL database
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


# %%

## Part 1: 90 days of BNB Data
response = requests.get("https://api.coingecko.com/api/v3/coins/binancecoin/market_chart?vs_currency=usd&days=90&interval=daily", \
                        timeout=10)
content = response.content
data = json.loads(content)

bnb_total_volumes_90d = pd.DataFrame(data['total_volumes'], columns=['time', 'vol_24h'])
bnb_total_volumes_90d['vol_24h'] = bnb_total_volumes_90d['vol_24h'].apply(lambda x: format(x, ','))
bnb_total_volumes_90d['time'] = pd.to_datetime(bnb_total_volumes_90d['time'] / 1000, unit='s',\
                                               origin='unix', utc=True)

bnb_price_90d = pd.DataFrame(data['prices'], columns=['time', 'price'])
bnb_price_90d['price'] = bnb_price_90d['price'].apply(lambda x: format(x, ','))
bnb_price_90d['time'] = pd.to_datetime(bnb_price_90d['time'] \
                                       / 1000, unit='s', origin='unix', utc=True)

bnb_marketcap_90d = pd.DataFrame(data['market_caps'], columns=['time', 'marketcap'])
bnb_marketcap_90d['marketcap'] = bnb_marketcap_90d['marketcap'].apply(lambda x: format(x, ','))
bnb_marketcap_90d['time'] = pd.to_datetime(bnb_marketcap_90d['time'] / 1000, unit='s', \
                                           origin='unix', utc=True)

# Get BNB/BTC price for comparison
response = requests.get("https://api.coingecko.com/api/v3/coins/binancecoin/market_chart?vs_currency=btc&days=90&interval=daily", \
                        timeout=10)
content = response.content
data = json.loads(content)

bnb_price_90d_vs_btc = pd.DataFrame(data['prices'], columns=['time', 'price'])
bnb_price_90d_vs_btc['price'] = bnb_price_90d_vs_btc['price'].apply(lambda x: format(x, ','))
bnb_price_90d_vs_btc['time'] = pd.to_datetime(bnb_price_90d_vs_btc['time'] \
                                       / 1000, unit='s', origin='unix', utc=True)

bnb_price_90d_vs_btc.rename(columns={'price': 'price_vs_btc'}, inplace=True)


# Join market cap, price, volume, and price_vs_btc
bnb_90d = bnb_price_90d.merge(bnb_marketcap_90d, on='time').merge(bnb_total_volumes_90d, on='time')\
          .merge(bnb_price_90d_vs_btc, on='time')

# Create date column on the merged dataframe
bnb_90d['Date'] = bnb_90d['time'].dt.date

# Convert 'Date' column to datetime format
bnb_90d['Date'] = pd.to_datetime(bnb_90d['Date'])

# Change data type for consistency
bnb_90d['price'] = bnb_90d['price'].str.replace(',', '').astype(float)
bnb_90d['price_vs_btc'] = bnb_90d['price_vs_btc'].str.replace(',', '').astype(float)

# Normalize the price between bnb vs. USD and bnb vs. BTC
bnb_90d['bnb_vs_usd_normalized'] = bnb_90d['price'] \
                                         / bnb_90d['price'].iloc[0]
bnb_90d['bnb_vs_btc_normalized'] = bnb_90d['price_vs_btc'] \
                                         / bnb_90d['price_vs_btc'].iloc[0]

# %%

## Part 2: Average Gas Fee History from Owlracle for the Past 30 Days
#  Documentation: https://owlracle.info/docs#endpoint-history
response = requests.get(f"https://api.owlracle.info/v4/bsc/history?apikey={OWLRACLE_API_KEY}&candles=30&timeframe=1440", \
                        timeout=30)

content = response.content
data = json.loads(content)

bnb_gas_fee = pd.DataFrame(data['candles'], columns=['avgGas', 'gasPrice', 'samples', 'timestamp'])

# Unnest the 'gasPrice' column, and drop the original gasPrice column
bnb_gas_fee[['gasPrice_open', 'gasPrice_close', 'gasPrice_low', \
             'gasPrice_high']] = bnb_gas_fee['gasPrice'].apply(pd.Series)
bnb_gas_fee.drop('gasPrice', axis=1, inplace=True)

# Convert the 'timestamp' column to a datetime format, then format the Date column properly
bnb_gas_fee['Date'] = pd.to_datetime(bnb_gas_fee['timestamp'])
bnb_gas_fee['Date'] = bnb_gas_fee['Date'].dt.strftime('%Y-%m-%d')


# %%

## Part 3: Developers and Community Data
bnb_details = requests.get("https://api.coingecko.com/api/v3/coins/binancecoin?localization=false&tickers=true&market_data=false&community_data=true&developer_data=true&sparkline=true", \
                               timeout=10)
bnb_details_data = bnb_details.json()

# developers_data
bnb_developers_data = {
    "commit_count_4_weeks": bnb_details_data["developer_data"]["commit_count_4_weeks"],
    "forks": bnb_details_data["developer_data"]["forks"],
    "stars": bnb_details_data["developer_data"]["stars"],
    "subscribers": bnb_details_data["developer_data"]["subscribers"]
}
bnb_developers_df = pd.DataFrame([bnb_developers_data])

# community_data
bnb_community_data = bnb_details_data["community_data"]
bnb_community_df = pd.DataFrame(list(bnb_community_data.items()), columns=["new_column", "V1"]).T
header = bnb_community_df.iloc[0]
bnb_community_df = bnb_community_df[1:]
bnb_community_df.columns = header
bnb_community_df.reset_index(drop=True, inplace=True)

# combine developers and community data
bnb_combined_data = pd.concat([bnb_community_df[["twitter_followers"]], bnb_developers_df], axis=1)

# rename columns and use melt to reshape the data frame
bnb_combined_data.rename(columns={
    "forks": "github forks",
    "stars": "github stars",
    "subscribers": "github subscribers",
    "twitter_followers": "twitter followers"
}, inplace=True)

bnb_combined_data = bnb_combined_data.round(0).melt(var_name="Data", value_name="Value")


# %%

## Part 4: BNB Stats

# Define an error handling function for clarity
def fetch_bnb_stats():
    """function to fetch bnb stats from CoinGecko API"""
    try:
        response = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=binancecoin&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=90d&locale=en", \
                                timeout=10)
        response.raise_for_status()

        data = response.json()

        if not data:
            print("Empty response from the API.")
            return None

        bnb_stats = data[0]
        return bnb_stats

    except requests.RequestException as req_exception:  # Catch any Request-related exceptions
        print(f"Error fetching data from API: {req_exception}")
        return None
    except (IndexError, TypeError, KeyError):
        print("Unexpected structure in API response or bnb data not found.")
        return None

bnb_stats = fetch_bnb_stats()

if bnb_stats is None:
    print("Failed to fetch bnb stats.")
else:
    # Continue processing
    pass

# Selecting the required columns
columns = ["market_cap", "market_cap_rank", "fully_diluted_valuation", \
           "circulating_supply", "max_supply", "ath"]
bnb_stats_1 = {col: bnb_stats[col] for col in columns if col in bnb_stats}

# Rename columns
column_rename = {
    "market_cap": "market cap",
    "market_cap_rank": "market cap rank",
    "fully_diluted_valuation": "fully diluted valuation",
    "circulating_supply": "circulating supply",
    "max_supply": "max supply",
    "ath": "ATH"
}
bnb_stats_1 = {column_rename[key] if key in column_rename \
               else key: value for key, value in bnb_stats_1.items()}

# Converting the dictionary to DataFrame and then melt to reshape the data frame
bnb_stats_cleaned = pd.DataFrame([bnb_stats_1]).melt(var_name="Data", value_name="Value").round(0)

# Since we already created `bnb_combined_data`, we can concatenate both DataFrames
bnb_combined_data_final = pd.concat([bnb_stats_cleaned, bnb_combined_data], ignore_index=True)

# there's no max supply value for bnb, so we need to drop it before applying format_value function
bnb_mask = (bnb_combined_data_final['Data'] == 'max supply')
bnb_combined_data_final = bnb_combined_data_final.drop(bnb_combined_data_final[bnb_mask].index)



# %%

## Part 5: BNB (BSC) Historical TVL Data from DefiLlama
# Documentation: https://defillama.com/docs/api

response = requests.get("https://api.llama.fi/v2/historicalChainTvl/bsc", \
                        timeout=30)
content = response.content
data = json.loads(content)

bnb_tvl = pd.DataFrame(data)

# Fix the TVL value and the Date format (from UNIX)
bnb_tvl['tvl'] = bnb_tvl['tvl'].apply(lambda x: format(x, ','))
bnb_tvl['date'] = pd.to_datetime(bnb_tvl['date'], unit='s', origin='unix', utc=True)
bnb_tvl['date'] = bnb_tvl['date'].dt.date
bnb_tvl.rename(columns={'date': 'Date'}, inplace=True)
bnb_tvl['Date'] = pd.to_datetime(bnb_tvl['Date'])

# %%

## Part 6: Historical TVL Data by Protocol from DefiLlama (Only Top 10 Protocols from BSC/Binance)
# Documentation: https://defillama.com/docs/api
# List of Top DeFi protocol names
bnb_defi_protocol_list_names = ["pancakeswap", "venus", "biswap", \
                                "alpaca-finance", "uncx-network", ]

# Initialize an empty variable to store the result later
bnb_defi_protocol_list = []

for protocol in bnb_defi_protocol_list_names:
    try:
        response = requests.get(f"https://api.llama.fi/protocol/{protocol}", timeout=30)
        response.raise_for_status()

        data = response.json()

        chain_to_use = 'BSC'
        if chain_to_use in data['chainTvls']:
            bnb_defi_tvl = pd.DataFrame(data['chainTvls'][chain_to_use]['tvl'])
        else:
            print(f"No data for '{chain_to_use}' in protocol: {protocol}")

        bnb_defi_tvl['totalLiquidityUSD'] = bnb_defi_tvl['totalLiquidityUSD'].apply(lambda x: format(x, ','))
        bnb_defi_tvl.rename(columns={'totalLiquidityUSD': f'totalLiquidity_{protocol}'}, inplace=True)

        bnb_defi_tvl['date'] = pd.to_datetime(bnb_defi_tvl['date'], unit='s', origin='unix', utc=True)
        bnb_defi_tvl['date'] = bnb_defi_tvl['date'].dt.strftime('%Y-%m-%d')

        bnb_defi_protocol_list.append(bnb_defi_tvl)

    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

# Merge all dataframes on the 'date' column
bnb_defi_tvl_top_dapps = bnb_defi_protocol_list[0]
for df in bnb_defi_protocol_list[1:]:
    bnb_defi_tvl_top_dapps = pd.merge(bnb_defi_tvl_top_dapps, df, on='date', how='inner')

# rename date to Date for consistency
bnb_defi_tvl_top_dapps.rename(columns={'date': 'Date'}, inplace=True)
bnb_defi_tvl_top_dapps['Date'] = pd.to_datetime(bnb_defi_tvl_top_dapps['Date'])


# %%

## Part 7: Export Finished Dataframes to PostgreSQL tables
# Create an inspector
inspector = inspect(engine)

# Get table names
tables = inspector.get_table_names()

# List of dataframes and their corresponding base table names
dfs_and_tables = [
    (bnb_90d, 'bnb_90d'),
    (bnb_gas_fee, 'bnb_gas_fee'),
    (bnb_combined_data_final, 'bnb_combined_data_final'),
    (bnb_tvl, 'bnb_tvl'),
    (bnb_defi_tvl_top_dapps, 'bnb_defi_tvl_top_dapps')
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
