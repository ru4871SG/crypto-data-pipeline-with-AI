"""
ETL (Extract, Transform, Load) Script - Chainlink
"""

# %%

## Libraries

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from pymongo import MongoClient

import os
import json
import pandas as pd
import requests

# %%

# Part 0: Connect to environment variables
load_dotenv()

thegraph_api = os.environ.get('THEGRAPH_API')
dune_api = os.environ.get('DUNE_API')
mongodb_uri = os.getenv('MONGODB_URI')

OWLRACLE_API_KEY = os.environ.get("OWLRACLE_API")

# %%

# Part 1: 30 days of Bitcoin, Ethereum, and Chainlink Data (Normalized) with TheGraph's Subgraph
headers = {
    "Authorization": f"Bearer {thegraph_api}",
    "Content-Type": "application/json",
}

# We will use the same GraphQL client for WBTC, ETH, and Chainlink data
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

    # Chainlink contract address
    chainlink_query = f"""
    query {{
        token(id: "0x514910771af9ca656af840dff83e8264ecf986ca", block: {{ number: {target_block} }}) {{
            id
            symbol
            name
            derivedETH
        }}
    }}
    """

    wbtc_response = None
    chainlink_response = None

    # Execute the queries
    try:
        wbtc_response = client.execute(gql(wbtc_query))
    except Exception as e:
        print("Error executing wbtc_query:", e)

    try:
        chainlink_response = client.execute(gql(chainlink_query))
    except Exception as e:
        print("Error executing chainlink_query:", e)

    wbtc_price_in_eth = float(wbtc_response['token']['derivedETH'])
    eth_price_in_usd = float(wbtc_response['bundle']['ethPriceUSD'])
    wbtc_price_in_usd = wbtc_price_in_eth * eth_price_in_usd
    chainlink_price_in_eth = float(chainlink_response['token']['derivedETH'])
    chainlink_price_in_usd = chainlink_price_in_eth * eth_price_in_usd

    # Append the results
    results.append({
        'target_block': target_block,
        'wbtc_price_in_eth': wbtc_price_in_eth,
        'eth_price_in_usd': eth_price_in_usd,
        'wbtc_price_in_usd': wbtc_price_in_usd,
        'chainlink_price_in_eth': chainlink_price_in_eth,
        'chainlink_price_in_usd': chainlink_price_in_usd,
    })

    # Reverse the order of the results
    results_ordered = list(reversed(results))

    # Convert results to a pandas DataFrame
    chainlink_1m_w_external = pd.DataFrame(results_ordered)

# Normalize the price between BTC, ETH, and Chainlink
chainlink_1m_w_external['chainlink_price_normalized'] = chainlink_1m_w_external['chainlink_price_in_usd'] \
                                         / chainlink_1m_w_external['chainlink_price_in_usd'].iloc[0]
chainlink_1m_w_external['btc_price_normalized'] = chainlink_1m_w_external['wbtc_price_in_usd'] \
                                         / chainlink_1m_w_external['wbtc_price_in_usd'].iloc[0]
chainlink_1m_w_external['eth_price_normalized'] = chainlink_1m_w_external['eth_price_in_usd'] \
                                         / chainlink_1m_w_external['eth_price_in_usd'].iloc[0]

# for 30 days of Ethereum price, simply use the 'eth_price_in_usd' column


# %%

# Part 2: Fetch TVL, Volume, and Tx Count from Uniswap for the Last 30 days with TheGraph's Subgraph

uniswap_results = []

# Loop through the past 31 days
for days_ago in range(0, 31):
    target_block = int(current_block_delayed - (blocks_per_day * days_ago))

    uniswap_query = f"""
    query {{
        token(id: "0x514910771af9ca656af840dff83e8264ecf986ca", block: {{ number: {target_block} }}) {{
            totalValueLockedUSD
            volumeUSD
            txCount
        }}
    }}
    """
    
    uniswap_response = None

    # Execute the queries
    try:
        uniswap_response = client.execute(gql(uniswap_query))
    except Exception as e:
        print("Error executing uniswap_query:", e)


    totalValueLockedUSD = float(uniswap_response['token']['totalValueLockedUSD'])
    volumeUSD = float(uniswap_response['token']['volumeUSD'])
    txCount = int(uniswap_response['token']['txCount'])
    
    # Append the results
    uniswap_results.append({
        'target_block': target_block,
        'totalValueLockedUSD': totalValueLockedUSD,
        'volumeUSD': volumeUSD,
        'txCount': txCount,
    })

    # Reverse the order of the results
    uniswap_results_ordered = list(reversed(uniswap_results))

    # Convert results to a pandas DataFrame
    chainlink_1m_uniswap_data = pd.DataFrame(uniswap_results_ordered)

    # Calculate dailyVolumeUSD
    chainlink_1m_uniswap_data['dailyVolumeUSD'] = chainlink_1m_uniswap_data['volumeUSD'].diff().fillna(0)

    # Drop the first row (since dailyVolumeUSD is calculated from the second row onwards)
    chainlink_1m_uniswap_data = chainlink_1m_uniswap_data.drop(chainlink_1m_uniswap_data.index[0])

    # Reset the index
    chainlink_1m_uniswap_data.reset_index(drop=True, inplace=True)


# %%

# Part 3: Fetch TVL, Borrow, and Deposit from Aave with TheGraph's Subgraph

aave_results = []

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

# Loop through the past 30 days
for days_ago in range(0, 30):
    target_block = int(current_block_delayed - (blocks_per_day * days_ago))

    aave_query = f"""
    query {{
        token(id: "0x514910771af9ca656af840dff83e8264ecf986ca", block: {{ number: {target_block} }}) {{
            _market {{
                totalBorrowBalanceUSD
                totalDepositBalanceUSD
                totalValueLockedUSD
            }}
        }}
    }}
    """
    
    aave_response = None

    # Execute the queries
    try:
        aave_response = client_aave.execute(gql(aave_query))
    except Exception as e:
        print("Error executing aave_query:", e)


    totalBorrowBalanceUSD = float(aave_response['token']['_market']['totalBorrowBalanceUSD'])
    totalDepositBalanceUSD = float(aave_response['token']['_market']['totalDepositBalanceUSD'])
    totalValueLockedUSD = float(aave_response['token']['_market']['totalValueLockedUSD'])
    
    # Append the results
    aave_results.append({
        'target_block': target_block,
        'totalBorrowBalanceUSD': totalBorrowBalanceUSD,
        'totalDepositBalanceUSD': totalDepositBalanceUSD,
        'totalValueLockedUSD': totalValueLockedUSD,
    })

    # Reverse the order of the results
    aave_results_ordered = list(reversed(aave_results))

    # Convert results to a pandas DataFrame
    chainlink_1m_aave_data = pd.DataFrame(aave_results_ordered)


# %%

# Part 4: Load Dataframes to MongoDB
# Get the Date (timestamp) from Owlracle
response = requests.get(f"https://api.owlracle.info/v4/eth/history?apikey={OWLRACLE_API_KEY}&candles=1&timeframe=1440", \
                        timeout=30)

content = response.content
data = json.loads(content)

eth_date = pd.DataFrame(data['candles'], columns=['timestamp'])

eth_date['timestamp'] = pd.to_datetime(eth_date['timestamp'])

# Function to load a DataFrame to MongoDB
def load_to_mongodb(df, df_name, date_df, uri):
    client = MongoClient(uri)
    db = client['deftify_research']

    # Check if 'timestamp' exists in date_df
    if 'timestamp' in date_df.columns:
        # Get the date from the 'timestamp' column of 'date_df'
        latest_date = date_df['timestamp'].iloc[0]
        month = latest_date.strftime('%B').lower()
        year = latest_date.year

    else:
        print(f"'timestamp' column doesn't exist in date_df: {date_df.columns}")
        return

    # Create the collection name based on the month and year from 'date_df'
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

# Store all the transformed dataframes into MongoDB collections, with 'eth_date' as 'date_df'
load_to_mongodb(chainlink_1m_w_external, 'chainlink_1m_w_external', eth_date, mongodb_uri)
load_to_mongodb(chainlink_1m_uniswap_data, 'chainlink_1m_uniswap_data', eth_date, mongodb_uri)
load_to_mongodb(chainlink_1m_aave_data, 'chainlink_1m_aave_data', eth_date, mongodb_uri)
