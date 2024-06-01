"""
ETL (Extract, Transform, Load) Script - Ethereum
"""

# %%

## Libraries

from datetime import datetime, timedelta
from dotenv import load_dotenv
from dune_client.client import DuneClient
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from pymongo import MongoClient

import os
import json
import numpy as np
import pandas as pd
import requests
import pytz

# %%

# Part 0: Connect to environment variables
load_dotenv()

thegraph_api = os.environ.get('THEGRAPH_API')
dune_api = os.environ.get('DUNE_API')
mongodb_uri = os.getenv('MONGODB_URI')

OWLRACLE_API_KEY = os.environ.get("OWLRACLE_API")

# %%

# Part 1: 30 days of Ethereum and Bitcoin Price (Normalized) with TheGraph's Subgraph
headers = {
    "Authorization": f"Bearer {thegraph_api}",
    "Content-Type": "application/json",
}

# We will use the same GraphQL client for both WBTC and ETH data
transport = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/deployments/id/QmZeCuoZeadgHkGwLwMeguyqUKz1WPWQYKcKyMCeQqGhsF",
    headers=headers,
    use_json=True,
    timeout=10,
)
client = Client(
    transport=transport,
    fetch_schema_from_transport=True,
)

# Get the current block number
meta_query = """
{
  _meta {
    block {
      number
    }
  }
}
"""

current_block_response = client.execute(gql(meta_query))
current_block = current_block_response['_meta']['block']['number']

# we will use the block number from about 1 hour ago since very recent block might have indexing issue
current_block_delayed = current_block - 300

# Assuming ~12.06 seconds average per block on Ethereum daily
seconds_per_day = 86400
blocks_per_day = seconds_per_day // 12.06

results = []

# Loop through the past 30 days
for days_ago in range(0, 30):
    target_block = int(current_block_delayed - (blocks_per_day * days_ago))

    # WBTC contract address
    wbtc_query = f"""
    query {{
        token(id: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", block: {{ number: {target_block} }}) {{
            id
            symbol
            name
            derivedETH
        }}
        bundle(id: "1", block: {{ number: {target_block} }}) {{
            ethPriceUSD
        }}
    }}
    """

    wbtc_response = None

    # Execute the WBTC query
    try:
        wbtc_response = client.execute(gql(wbtc_query))
    except Exception as e:
        print("Error executing wbtc_query:", e)

    wbtc_price_in_eth = float(wbtc_response['token']['derivedETH'])
    eth_price_in_usd = float(wbtc_response['bundle']['ethPriceUSD'])
    wbtc_price_in_usd = wbtc_price_in_eth * eth_price_in_usd

    # Append the results with ETH data instead of PAXG
    results.append({
        'target_block': target_block,
        'wbtc_price_in_eth': wbtc_price_in_eth,
        'eth_price_in_usd': eth_price_in_usd,
        'wbtc_price_in_usd': wbtc_price_in_usd,
    })

    # Reverse the order of the results
    results_ordered = list(reversed(results))

    # Convert results to a pandas DataFrame
    eth_1m_w_external = pd.DataFrame(results_ordered)

# Normalize the price between BTC and ETH
eth_1m_w_external['eth_price_normalized'] = eth_1m_w_external['eth_price_in_usd'] \
                                         / eth_1m_w_external['eth_price_in_usd'].iloc[0]
eth_1m_w_external['btc_price_normalized'] = eth_1m_w_external['wbtc_price_in_usd'] \
                                         / eth_1m_w_external['wbtc_price_in_usd'].iloc[0]

# for 30 days of Ethereum price, simply use the 'eth_price_in_usd' column

# %%

# Part 2: Average Gas Fee History from Owlracle for the Past 30 Days
#  Documentation: https://owlracle.info/docs#endpoint-history
response = requests.get(f"https://api.owlracle.info/v4/eth/history?apikey={OWLRACLE_API_KEY}&candles=30&timeframe=1440", \
                        timeout=30)

content = response.content
data = json.loads(content)

eth_1m_gas_fee = pd.DataFrame(data['candles'], columns=['gasPrice', 'samples', 'timestamp'])

# Unnest the 'gasPrice' column to get 'high', and drop the original gasPrice column
eth_1m_gas_fee['gasPrice_close'] = eth_1m_gas_fee['gasPrice'].apply(lambda x: x['close'])
eth_1m_gas_fee.drop('gasPrice', axis=1, inplace=True)

# Convert the 'timestamp' column to a datetime format
eth_1m_gas_fee['timestamp'] = pd.to_datetime(eth_1m_gas_fee['timestamp'])


# %%

# Part 3: Fetch TVL and Fees from Uniswap for the Past Month with TheGraph's Subgraph

# GraphQL query to fetch Uniswap data
uniswap_query = """
{
  uniswapDayDatas(orderBy: date, orderDirection: desc, first: 31) {
    date
    feesUSD
    tvlUSD
    txCount
    volumeUSD
  }
}
"""

# Execute the query using the same GraphQL client as before
try:
    uniswap_response = client.execute(gql(uniswap_query))
except Exception as e:
    print("Error executing uniswap_query:", e)
    uniswap_response = None

if uniswap_response:
    # Extract the data from the response and convert it to a DataFrame
    uniswap_data = uniswap_response['uniswapDayDatas']
    eth_1m_uniswap_data = pd.DataFrame(uniswap_data)

    # Convert the 'date' column from UNIX timestamp
    eth_1m_uniswap_data['date'] = pd.to_datetime(eth_1m_uniswap_data['date'], unit='s')

    # Convert the other columns to the appropriate data types
    eth_1m_uniswap_data['feesUSD'] = eth_1m_uniswap_data['feesUSD'].astype(float)
    eth_1m_uniswap_data['tvlUSD'] = eth_1m_uniswap_data['tvlUSD'].astype(float)
    eth_1m_uniswap_data['volumeUSD'] = eth_1m_uniswap_data['volumeUSD'].astype(float)
    eth_1m_uniswap_data['txCount'] = eth_1m_uniswap_data['txCount'].astype(int)

else:
    print("Failed to fetch Uniswap data from TheGraph")


# %%

# Part 4: Fetch TVL from Lido with TheGraph's Subgraph

# GraphQL query to fetch Lido data
lido_query = """
{
  financialsDailySnapshots(orderBy: timestamp, orderDirection: desc, first: 31) {
    totalValueLockedUSD
    timestamp
  }
}
"""

# Different client for Lido data
transport_lido = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/subgraphs/id/F7qb71hWab6SuRL5sf6LQLTpNahmqMsBnnweYHzLGUyG",
    headers=headers,
    use_json=True,
    timeout=10,
)
client_lido = Client(
    transport=transport_lido,
    fetch_schema_from_transport=True,
)

# Execute the query
try:
    lido_response = client_lido.execute(gql(lido_query))
except Exception as e:
    print("Error executing lido_query:", e)
    lido_response = None

if lido_response:
    # Extract the data from the response and convert it to a DataFrame
    lido_data = lido_response['financialsDailySnapshots']
    eth_1m_lido_data = pd.DataFrame(lido_data)

    # Convert the columns to the appropriate data types
    eth_1m_lido_data['totalValueLockedUSD'] = eth_1m_lido_data['totalValueLockedUSD'].astype(float)
    eth_1m_lido_data['timestamp'] = pd.to_datetime(eth_1m_lido_data['timestamp'], unit='s')

else:
    print("Failed to fetch Lido data from TheGraph")



# %%

# Part 5: Fetch TVL from Aave with TheGraph's Subgraph

# GraphQL query to fetch Aave data
aave_query = """
{
  financialsDailySnapshots(orderBy: timestamp, orderDirection: desc, first: 31) {
    totalValueLockedUSD
    timestamp
  }
}
"""

# Different client for Aave data
transport_aave = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/subgraphs/id/C2zniPn45RnLDGzVeGZCx2Sw3GXrbc9gL4ZfL8B8Em2j",
    headers=headers,
    use_json=True,
    timeout=10,
)
client_aave = Client(
    transport=transport_aave,
    fetch_schema_from_transport=True,
)

# Execute the query
try:
    aave_response = client_aave.execute(gql(aave_query))
except Exception as e:
    print("Error executing aave_query:", e)
    aave_response = None

if aave_response:
    # Extract the data from the response and convert it to a DataFrame
    aave_data = aave_response['financialsDailySnapshots']
    eth_1m_aave_data = pd.DataFrame(aave_data)

    # Convert the columns to the appropriate data types
    eth_1m_aave_data['totalValueLockedUSD'] = eth_1m_aave_data['totalValueLockedUSD'].astype(float)
    eth_1m_aave_data['timestamp'] = pd.to_datetime(eth_1m_aave_data['timestamp'], unit='s')

else:
    print("Failed to fetch Aave data from TheGraph")


# %%

# Part 6: Fetch TVL from Makerdao with TheGraph's Subgraph

# GraphQL query to fetch Makerdao data
makerdao_query = """
{
  financialsDailySnapshots(orderBy: timestamp, orderDirection: desc, first: 31) {
    totalValueLockedUSD
    timestamp
  }
}
"""

# Different client for makerdao data
transport_makerdao = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/subgraphs/id/8sE6rTNkPhzZXZC6c8UQy2ghFTu5PPdGauwUBm4t7HZ1",
    headers=headers,
    use_json=True,
    timeout=10,
)
client_makerdao = Client(
    transport=transport_makerdao,
    fetch_schema_from_transport=True,
)

# Execute the query
try:
    makerdao_response = client_makerdao.execute(gql(makerdao_query))
except Exception as e:
    print("Error executing makerdao_query:", e)
    makerdao_response = None

if makerdao_response:
    # Extract the data from the response and convert it to a DataFrame
    makerdao_data = makerdao_response['financialsDailySnapshots']
    eth_1m_makerdao_data = pd.DataFrame(makerdao_data)

    # Convert the columns to the appropriate data types
    eth_1m_makerdao_data['totalValueLockedUSD'] = eth_1m_makerdao_data['totalValueLockedUSD'].astype(float)
    eth_1m_makerdao_data['timestamp'] = pd.to_numeric(eth_1m_makerdao_data['timestamp'])
    eth_1m_makerdao_data['timestamp'] = pd.to_datetime(eth_1m_makerdao_data['timestamp'], unit='s')

else:
    print("Failed to fetch Makerdao data from TheGraph")

# %%
# Part 7: Merge Non-Uniswap Protocol Dataframes
eth_1m_aave_data = eth_1m_aave_data.rename(columns={'totalValueLockedUSD': 'tvl_aave'})
eth_1m_lido_data = eth_1m_lido_data.rename(columns={'totalValueLockedUSD': 'tvl_lido'})
eth_1m_makerdao_data = eth_1m_makerdao_data.rename(columns={'totalValueLockedUSD': 'tvl_makerdao'})

eth_1m_aave_data['row_index'] = np.arange(len(eth_1m_aave_data))
eth_1m_lido_data['row_index'] = np.arange(len(eth_1m_lido_data))
eth_1m_makerdao_data['row_index'] = np.arange(len(eth_1m_makerdao_data))

eth_1m_tvl = pd.merge(eth_1m_aave_data, eth_1m_lido_data, on='row_index', how='left')
eth_1m_tvl = pd.merge(eth_1m_tvl, eth_1m_makerdao_data, on='row_index', how='left')

eth_1m_tvl['day_num'] = eth_1m_tvl['row_index'].max() - eth_1m_tvl['row_index']

columns_to_keep = ['row_index', 'day_num', 'tvl_aave', 'tvl_lido', 'tvl_makerdao']
eth_1m_tvl = eth_1m_tvl[columns_to_keep]

# %%

# Part 8: L2 Bridge Stats From Dune Analytics
# Get the current date and calculate the date for 31 days (1 month) ago
current_date = datetime.now(pytz.utc)
date_1_month_ago = current_date - timedelta(days=31)

# ZKSync Stats
dune = DuneClient(dune_api, request_timeout=10)
query_result_zksync = dune.get_latest_result(784184)
dune_rows_zksync = query_result_zksync.get_rows()

eth_1m_l2_bridge_zksync = pd.DataFrame(dune_rows_zksync)

eth_1m_l2_bridge_zksync['day'] = pd.to_datetime(eth_1m_l2_bridge_zksync['day'])

eth_1m_l2_bridge_zksync = eth_1m_l2_bridge_zksync[eth_1m_l2_bridge_zksync['day'] >= date_1_month_ago]

eth_1m_l2_bridge_zksync = eth_1m_l2_bridge_zksync.sort_values('day')

# Remove the last row since the data might not be complete for the current day
eth_1m_l2_bridge_zksync = eth_1m_l2_bridge_zksync.drop(eth_1m_l2_bridge_zksync.index[-1])
# Reset the index to have a clean 'row_num' column
eth_1m_l2_bridge_zksync.reset_index(drop=True, inplace=True)

# Starknet Stats
query_result_starknet = dune.get_latest_result(831568)
dune_rows_starknet = query_result_starknet.get_rows()

eth_1m_l2_bridge_starknet = pd.DataFrame(dune_rows_starknet)

eth_1m_l2_bridge_starknet['day'] = pd.to_datetime(eth_1m_l2_bridge_starknet['day'])

eth_1m_l2_bridge_starknet = eth_1m_l2_bridge_starknet[eth_1m_l2_bridge_starknet['day'] >= date_1_month_ago]

eth_1m_l2_bridge_starknet = eth_1m_l2_bridge_starknet.sort_values('day')

# Remove the last row since the data might not be complete for the current day
eth_1m_l2_bridge_starknet = eth_1m_l2_bridge_starknet.drop(eth_1m_l2_bridge_starknet.index[-1])
# Reset the index to have a clean 'row_num' column
eth_1m_l2_bridge_starknet.reset_index(drop=True, inplace=True)

# Arbitrum Stats
query_result_arbitrum = dune.get_latest_result(784216)
dune_rows_arbitrum = query_result_arbitrum.get_rows()

eth_1m_l2_bridge_arbitrum = pd.DataFrame(dune_rows_arbitrum)

eth_1m_l2_bridge_arbitrum['day'] = pd.to_datetime(eth_1m_l2_bridge_arbitrum['day'])

eth_1m_l2_bridge_arbitrum = eth_1m_l2_bridge_arbitrum[eth_1m_l2_bridge_arbitrum['day'] >= date_1_month_ago]

eth_1m_l2_bridge_arbitrum = eth_1m_l2_bridge_arbitrum.sort_values('day')

# Remove the last row since the data might not be complete for the current day
eth_1m_l2_bridge_arbitrum = eth_1m_l2_bridge_arbitrum.drop(eth_1m_l2_bridge_arbitrum.index[-1])
# Reset the index to have a clean 'row_num' column
eth_1m_l2_bridge_arbitrum.reset_index(drop=True, inplace=True)

# Optimism Stats
query_result_optimism = dune.get_latest_result(784244)
dune_rows_optimism = query_result_optimism.get_rows()

eth_1m_l2_bridge_optimism = pd.DataFrame(dune_rows_optimism)

eth_1m_l2_bridge_optimism['day'] = pd.to_datetime(eth_1m_l2_bridge_optimism['day'])

eth_1m_l2_bridge_optimism = eth_1m_l2_bridge_optimism[eth_1m_l2_bridge_optimism['day'] >= date_1_month_ago]

eth_1m_l2_bridge_optimism = eth_1m_l2_bridge_optimism.sort_values('day')

# Remove the last row since the data might not be complete for the current day
eth_1m_l2_bridge_optimism = eth_1m_l2_bridge_optimism.drop(eth_1m_l2_bridge_optimism.index[-1])
# Reset the index to have a clean 'row_num' column
eth_1m_l2_bridge_optimism.reset_index(drop=True, inplace=True)

# Base Stats
query_result_base = dune.get_latest_result(2896672)
dune_rows_base = query_result_base.get_rows()

eth_1m_l2_bridge_base = pd.DataFrame(dune_rows_base)

eth_1m_l2_bridge_base['day'] = pd.to_datetime(eth_1m_l2_bridge_base['day'])

eth_1m_l2_bridge_base = eth_1m_l2_bridge_base[eth_1m_l2_bridge_base['day'] >= date_1_month_ago]

eth_1m_l2_bridge_base = eth_1m_l2_bridge_base.sort_values('day')

# Remove the last row since the data might not be complete for the current day
eth_1m_l2_bridge_base = eth_1m_l2_bridge_base.drop(eth_1m_l2_bridge_base.index[-1])
# Reset the index to have a clean 'row_num' column
eth_1m_l2_bridge_base.reset_index(drop=True, inplace=True)

# Merge all the L2 bridge dataframes
eth_1m_l2_bridge_arbitrum = eth_1m_l2_bridge_arbitrum.rename(columns={'users': 'users_arbitrum'})
eth_1m_l2_bridge_base = eth_1m_l2_bridge_base.rename(columns={'total': 'total_base', 'users': 'users_base'})
eth_1m_l2_bridge_optimism = eth_1m_l2_bridge_optimism.rename(columns={'total': 'total_optimism', 'users': 'users_optimism'})
eth_1m_l2_bridge_starknet = eth_1m_l2_bridge_starknet.rename(columns={'total': 'total_starknet', 'users': 'users_starknet'})
eth_1m_l2_bridge_zksync = eth_1m_l2_bridge_zksync.rename(columns={'total': 'total_zksync', 'users': 'users_zksync'})

eth_1m_l2_bridge_all = pd.merge(eth_1m_l2_bridge_zksync, eth_1m_l2_bridge_starknet, on='day', how='left')
eth_1m_l2_bridge_all = pd.merge(eth_1m_l2_bridge_all, eth_1m_l2_bridge_arbitrum, on='day', how='left')
eth_1m_l2_bridge_all = pd.merge(eth_1m_l2_bridge_all, eth_1m_l2_bridge_optimism, on='day', how='left')
eth_1m_l2_bridge_all = pd.merge(eth_1m_l2_bridge_all, eth_1m_l2_bridge_base, on='day', how='left')

columns_to_keep = ['users_arbitrum', 'users_base', 'users_optimism', 'users_starknet', 'users_zksync', 'day']
eth_1m_l2_bridge_all = eth_1m_l2_bridge_all[columns_to_keep]


# %%

# Part 9: Load Dataframes to MongoDB
def load_to_mongodb(df, df_name, date_df, uri):
    client = MongoClient(uri)
    db = client['deftify_research']

    # Check if 'timestamp' exists in date_df
    if 'timestamp' in date_df.columns:
        # Get the latest date from the 'timestamp' column of 'date_df'
        latest_date = date_df['timestamp'].max()

        # Extract the month and year from the latest date
        month = latest_date.strftime('%B').lower()
        year = latest_date.year
    else:
        print(f"'timestamp' column doesn't exist in date_df: {date_df.columns}")

    # Create the collection name based on the latest month and year from 'date_df'
    collection_name = f"{df_name}_{month}_{year}"

    # Check if the collection already exists
    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' already exists. Skipping data upload.")
        return

    collection = db[collection_name]

    # Convert DataFrame to dict
    data_dict = df.to_dict("records")

    # Insert each record in the DataFrame into the collection
    collection.insert_many(data_dict)

# Store all the transformed dataframes into MongoDB collections, with 'eth_1m_gas_fee' as 'date_df'
load_to_mongodb(eth_1m_w_external, 'eth_1m_w_external', eth_1m_gas_fee, mongodb_uri)
load_to_mongodb(eth_1m_gas_fee, 'eth_1m_gas_fee', eth_1m_gas_fee, mongodb_uri)
load_to_mongodb(eth_1m_uniswap_data, 'eth_1m_uniswap_data', eth_1m_gas_fee, mongodb_uri)
load_to_mongodb(eth_1m_tvl, 'eth_1m_tvl', eth_1m_gas_fee, mongodb_uri)
load_to_mongodb(eth_1m_l2_bridge_all, 'eth_1m_l2_bridge_all', eth_1m_gas_fee, mongodb_uri)
