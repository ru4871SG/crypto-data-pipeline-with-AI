"""
ETL (Extract, Transform, Load) Script - Ethereum
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

## Part 1: 90 days of Ethereum Data
response = requests.get("https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=90&interval=daily", \
                        timeout=10)
content = response.content
data = json.loads(content)

eth_total_volumes_90d = pd.DataFrame(data['total_volumes'], columns=['time', 'vol_24h'])
eth_total_volumes_90d['vol_24h'] = eth_total_volumes_90d['vol_24h'].apply(lambda x: format(x, ','))
eth_total_volumes_90d['time'] = pd.to_datetime(eth_total_volumes_90d['time'] / 1000, unit='s',\
                                               origin='unix', utc=True)

eth_price_90d = pd.DataFrame(data['prices'], columns=['time', 'price'])
eth_price_90d['price'] = eth_price_90d['price'].apply(lambda x: format(x, ','))
eth_price_90d['time'] = pd.to_datetime(eth_price_90d['time'] \
                                       / 1000, unit='s', origin='unix', utc=True)

eth_marketcap_90d = pd.DataFrame(data['market_caps'], columns=['time', 'marketcap'])
eth_marketcap_90d['marketcap'] = eth_marketcap_90d['marketcap'].apply(lambda x: format(x, ','))
eth_marketcap_90d['time'] = pd.to_datetime(eth_marketcap_90d['time'] / 1000, unit='s', \
                                           origin='unix', utc=True)

# Get ETH/BTC price for comparison
response = requests.get("https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=btc&days=90&interval=daily", \
                        timeout=10)
content = response.content
data = json.loads(content)

eth_price_90d_vs_btc = pd.DataFrame(data['prices'], columns=['time', 'price'])
eth_price_90d_vs_btc['price'] = eth_price_90d_vs_btc['price'].apply(lambda x: format(x, ','))
eth_price_90d_vs_btc['time'] = pd.to_datetime(eth_price_90d_vs_btc['time'] \
                                       / 1000, unit='s', origin='unix', utc=True)

eth_price_90d_vs_btc.rename(columns={'price': 'price_vs_btc'}, inplace=True)


# Join market cap, price, volume, and price_vs_btc
eth_90d = eth_price_90d.merge(eth_marketcap_90d, on='time').merge(eth_total_volumes_90d, on='time')\
          .merge(eth_price_90d_vs_btc, on='time')

# Create date column on the merged dataframe
eth_90d['Date'] = eth_90d['time'].dt.date

# Convert 'Date' column to datetime format
eth_90d['Date'] = pd.to_datetime(eth_90d['Date'])

# Change data type for consistency
eth_90d['price'] = eth_90d['price'].str.replace(',', '').astype(float)
eth_90d['price_vs_btc'] = eth_90d['price_vs_btc'].str.replace(',', '').astype(float)

# Normalize the price between ETH vs. USD and ETH vs. BTC
eth_90d['eth_vs_usd_normalized'] = eth_90d['price'] \
                                         / eth_90d['price'].iloc[0]
eth_90d['eth_vs_btc_normalized'] = eth_90d['price_vs_btc'] \
                                         / eth_90d['price_vs_btc'].iloc[0]

# %%

## Part 2: Average Gas Fee History from Owlracle for the Past 30 Days
#  Documentation: https://owlracle.info/docs#endpoint-history
response = requests.get(f"https://api.owlracle.info/v4/eth/history?apikey={OWLRACLE_API_KEY}&candles=30&timeframe=1440", \
                        timeout=30)

content = response.content
data = json.loads(content)

eth_gas_fee = pd.DataFrame(data['candles'], columns=['avgGas', 'gasPrice', 'samples', 'timestamp'])

# Unnest the 'gasPrice' column, and drop the original gasPrice column
eth_gas_fee[['gasPrice_open', 'gasPrice_close', 'gasPrice_low', \
             'gasPrice_high']] = eth_gas_fee['gasPrice'].apply(pd.Series)
eth_gas_fee.drop('gasPrice', axis=1, inplace=True)

# Convert the 'timestamp' column to a datetime format, then format the Date column properly
eth_gas_fee['Date'] = pd.to_datetime(eth_gas_fee['timestamp'])
eth_gas_fee['Date'] = eth_gas_fee['Date'].dt.strftime('%Y-%m-%d')


# %%

## Part 3: Developers and Community Data
ethereum_details = requests.get("https://api.coingecko.com/api/v3/coins/ethereum?localization=false&tickers=true&market_data=false&community_data=true&developer_data=true&sparkline=true", \
                               timeout=10)
ethereum_details_data = ethereum_details.json()

# developers_data
eth_developers_data = {
    "commit_count_4_weeks": ethereum_details_data["developer_data"]["commit_count_4_weeks"],
    "forks": ethereum_details_data["developer_data"]["forks"],
    "stars": ethereum_details_data["developer_data"]["stars"],
    "subscribers": ethereum_details_data["developer_data"]["subscribers"]
}
eth_developers_df = pd.DataFrame([eth_developers_data])

# community_data
eth_community_data = ethereum_details_data["community_data"]
eth_community_df = pd.DataFrame(list(eth_community_data.items()), columns=["new_column", "V1"]).T
header = eth_community_df.iloc[0]
eth_community_df = eth_community_df[1:]
eth_community_df.columns = header
eth_community_df.reset_index(drop=True, inplace=True)

# combine developers and community data
eth_combined_data = pd.concat([eth_community_df[["twitter_followers"]], eth_developers_df], axis=1)

# rename columns and use melt to reshape the data frame
eth_combined_data.rename(columns={
    "forks": "github forks",
    "stars": "github stars",
    "subscribers": "github subscribers",
    "twitter_followers": "twitter followers"
}, inplace=True)

eth_combined_data = eth_combined_data.round(0).melt(var_name="Data", value_name="Value")


# %%

## Part 4: Ethereum Stats

# Define an error handling function for clarity
def fetch_eth_stats():
    """function to fetch ethereum stats from CoinGecko API"""
    try:
        response = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=ethereum&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=90d&locale=en", \
                                timeout=10)
        response.raise_for_status()

        data = response.json()

        if not data:
            print("Empty response from the API.")
            return None

        eth_stats = data[0]
        return eth_stats

    except requests.RequestException as req_exception:  # Catch any Request-related exceptions
        print(f"Error fetching data from API: {req_exception}")
        return None
    except (IndexError, TypeError, KeyError):
        print("Unexpected structure in API response or ethereum data not found.")
        return None

eth_stats = fetch_eth_stats()

if eth_stats is None:
    print("Failed to fetch ethereum stats.")
else:
    # Continue processing
    pass

# Selecting the required columns
columns = ["market_cap", "market_cap_rank", "fully_diluted_valuation", \
           "circulating_supply", "max_supply", "ath"]
eth_stats_1 = {col: eth_stats[col] for col in columns if col in eth_stats}

# Rename columns
column_rename = {
    "market_cap": "market cap",
    "market_cap_rank": "market cap rank",
    "fully_diluted_valuation": "fully diluted valuation",
    "circulating_supply": "circulating supply",
    "max_supply": "max supply",
    "ath": "ATH"
}
eth_stats_1 = {column_rename[key] if key in column_rename \
               else key: value for key, value in eth_stats_1.items()}

# Converting the dictionary to DataFrame and then melt to reshape the data frame
eth_stats_cleaned = pd.DataFrame([eth_stats_1]).melt(var_name="Data", value_name="Value").round(0)

# Since we already created `eth_combined_data`, we can concatenate both DataFrames
eth_combined_data_final = pd.concat([eth_stats_cleaned, eth_combined_data], ignore_index=True)

# there's no max supply value for ETH, so we need to drop it before applying format_value function
eth_mask = (eth_combined_data_final['Data'] == 'max supply')
eth_combined_data_final = eth_combined_data_final.drop(eth_combined_data_final[eth_mask].index)


# %%

## Part 5: Ethereum Historical TVL Data from DefiLlama
# Documentation: https://defillama.com/docs/api

response = requests.get("https://api.llama.fi/v2/historicalChainTvl/Ethereum", \
                        timeout=30)
content = response.content
data = json.loads(content)

eth_tvl = pd.DataFrame(data)

# Fix the TVL value and the Date format (from UNIX)
eth_tvl['tvl'] = eth_tvl['tvl'].apply(lambda x: format(x, ','))
eth_tvl['date'] = pd.to_datetime(eth_tvl['date'], unit='s', origin='unix', utc=True)
eth_tvl['date'] = eth_tvl['date'].dt.date
eth_tvl.rename(columns={'date': 'Date'}, inplace=True)
eth_tvl['Date'] = pd.to_datetime(eth_tvl['Date'])

# %%

## Part 6: Historical TVL Data by Protocol from DefiLlama (Only Top 10 Protocols from Ethereum)
# Documentation: https://defillama.com/docs/api
# List of Top DeFi protocol names
eth_defi_protocol_list_names = ["aave", "lido", "makerdao", "uniswap", "summer.fi", "instadapp", \
                                "compound-finance", "rocket-pool", "curve-dex", "convex-finance"]

# Initialize an empty variable to store the result later
eth_defi_protocol_list = []


for protocol in eth_defi_protocol_list_names:
    try:
        response = requests.get(f"https://api.llama.fi/protocol/{protocol}", timeout=30)
        response.raise_for_status()

        data = response.json()

        chain_to_use = 'Ethereum'
        if chain_to_use in data['chainTvls']:
            eth_defi_tvl = pd.DataFrame(data['chainTvls'][chain_to_use]['tvl'])
        else:
            print(f"No data for '{chain_to_use}' in protocol: {protocol}")

        eth_defi_tvl['totalLiquidityUSD'] = eth_defi_tvl['totalLiquidityUSD'].apply(lambda x: format(x, ','))
        eth_defi_tvl.rename(columns={'totalLiquidityUSD': f'totalLiquidity_{protocol}'}, inplace=True)

        eth_defi_tvl['date'] = pd.to_datetime(eth_defi_tvl['date'], unit='s', origin='unix', utc=True)
        eth_defi_tvl['date'] = eth_defi_tvl['date'].dt.strftime('%Y-%m-%d')

        eth_defi_protocol_list.append(eth_defi_tvl)

    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")


# Merge all dataframes on the 'date' column
eth_defi_tvl_top10 = eth_defi_protocol_list[0]
for df in eth_defi_protocol_list[1:]:
    eth_defi_tvl_top10 = pd.merge(eth_defi_tvl_top10, df, on='date', how='inner')

# rename date to Date for consistency
eth_defi_tvl_top10.rename(columns={'date': 'Date'}, inplace=True)
eth_defi_tvl_top10['Date'] = pd.to_datetime(eth_defi_tvl_top10['Date'])


# %%

## Part 7: Export Finished Dataframes to PostgreSQL tables
# Create an inspector
inspector = inspect(engine)

# Get table names
tables = inspector.get_table_names()

# List of dataframes and their corresponding base table names
dfs_and_tables = [
    (eth_90d, 'eth_90d'),
    (eth_gas_fee, 'eth_gas_fee'),
    (eth_combined_data_final, 'eth_combined_data_final'),
    (eth_tvl, 'eth_tvl'),
    (eth_defi_tvl_top10, 'eth_defi_tvl_top10')
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