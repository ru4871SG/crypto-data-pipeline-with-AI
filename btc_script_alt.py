"""
ETL (Extract, Transform, Load) Script - Bitcoin (With Alternative Sources)
Instead of using CoinGecko and Yahoo Finance, this script uses TheGraph and Dune Analytics.
It also extracts more data from Mempool
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
import pandas as pd
import requests

# %%

## Part 0: Connect to environment variables
load_dotenv()

thegraph_api = os.environ.get('THEGRAPH_API')
dune_api = os.environ.get('DUNE_API')
mongodb_uri = os.getenv('MONGODB_URI')

# %%

## Part 1: 30 days of Bitcoin and Gold Data (Normalized) with TheGraph's Subgraphs
headers = {
    "Authorization": f"Bearer {thegraph_api}",
    "Content-Type": "application/json",
}

# Configure the GraphQL client for WBTC (BTC on Ethereum)
transport_wbtc = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/deployments/id/QmZeCuoZeadgHkGwLwMeguyqUKz1WPWQYKcKyMCeQqGhsF",
    headers=headers,
    use_json=True,
    timeout=10,
)
client_wbtc = Client(
    transport=transport_wbtc,
    fetch_schema_from_transport=True,
)

# Configure the GraphQL client for PAXG (Gold on Ethereum)
transport_paxg = RequestsHTTPTransport(
    url="https://gateway-arbitrum.network.thegraph.com/api/{thegraph_api}/subgraphs/id/A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum",
    headers=headers,
    use_json=True,
    timeout=10,
)
client_paxg = Client(
    transport=transport_paxg,
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

current_block_response = client_wbtc.execute(gql(meta_query))
current_block = current_block_response['_meta']['block']['number']

# Assuming ~12.06 seconds average per block on Ethereum daily
seconds_per_day = 86400
blocks_per_day = seconds_per_day // 12.06

results = []

# Loop through the past 30 days
for days_ago in range(0, 30):
    target_block = int(current_block - (blocks_per_day * days_ago))

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

    # PAXG contract address
    paxg_query = f"""
    query {{
        token(id: "0x45804880de22913dafe09f4980848ece6ecbaf78", block: {{ number: {target_block} }}) {{
            id
            symbol
            name
            derivedETH
        }}
    }}
    """

    wbtc_response = None
    paxg_response = None

    # Execute the queries
    try:
        wbtc_response = client_wbtc.execute(gql(wbtc_query))
    except Exception as e:
        print("Error executing wbtc_query:", e)

    try:
        paxg_response = client_paxg.execute(gql(paxg_query))
    except Exception as e:
        print("Error executing paxg_query:", e)

    wbtc_price_in_eth = float(wbtc_response['token']['derivedETH'])
    eth_price_in_usd = float(wbtc_response['bundle']['ethPriceUSD'])
    wbtc_price_in_usd = wbtc_price_in_eth * eth_price_in_usd
    paxg_price_in_eth = float(paxg_response['token']['derivedETH'])
    paxg_price_in_usd = paxg_price_in_eth * eth_price_in_usd

    # Append the results
    results.append({
        'target_block': target_block,
        'wbtc_price_in_eth': wbtc_price_in_eth,
        'eth_price_in_usd': eth_price_in_usd,
        'wbtc_price_in_usd': wbtc_price_in_usd,
        'paxg_price_in_eth': paxg_price_in_eth,
        'paxg_price_in_usd': paxg_price_in_usd,
    })

    # Reverse the order of the results
    results_ordered = list(reversed(results))

    # Convert results to a pandas DataFrame
    btc_1m_w_external = pd.DataFrame(results_ordered)

# Normalize the price between BTC and Gold
btc_1m_w_external['btc_price_normalized'] = btc_1m_w_external['wbtc_price_in_usd'] \
                                         / btc_1m_w_external['wbtc_price_in_usd'].iloc[0]
btc_1m_w_external['gold_price_normalized'] = btc_1m_w_external['paxg_price_in_usd'] \
                                         / btc_1m_w_external['paxg_price_in_usd'].iloc[0]


# %%

## Part 2: Median Tx Fee from Mempool for the Past 1 Month (each row represents a group of blocks - per 3 blocks)
def mempool_get_1m_avg_fee():
    """Fetches the average fee rates for the past 1 month"""
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

# Convert the timestamp to readable date format
btc_1m_mempool_fee['time'] = pd.to_datetime(btc_1m_mempool_fee['timestamp'], unit='s')


# %%

## Part 3: Historical BTC Price Data from Mempool
def mempool_get_btc_historical_price(timestamp):
    """Fetch Bitcoin historical price in USD"""
    url = f"https://mempool.space/api/v1/historical-price?currency=USD&timestamp={timestamp}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()['prices'][0]
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

# Use the 'timestamp' data from 'btc_1m_mempool_fee', then calculate the step for every 48 rows(every 1 day)
# The [::-1] reverses the DataFrame order, then [::48] takes every 48th row
timestamps = btc_1m_mempool_fee['timestamp'].iloc[::-1].iloc[::48]

# Fetch the data
btc_historical_prices = []
for ts in timestamps:
    price_data = mempool_get_btc_historical_price(ts)
    if price_data:
        btc_historical_prices.append(price_data)

# Check if we get the data back
if btc_historical_prices:
    # Convert to DataFrame for easier handling and analysis
    btc_1m_mempool_price = pd.DataFrame(btc_historical_prices)

    # Convert 'time' to readable date format and sort the data
    btc_1m_mempool_price['date'] = pd.to_datetime(btc_1m_mempool_price['time'], unit='s')
    btc_1m_mempool_price.sort_values('date', inplace=True)
else:
    print("No data was returned from the API.")

# %%

## Part 4: Mining Pools Data from Mempools
mempool_bitcoin_mining = requests.get("https://mempool.space/api/v1/mining/pools/1m", timeout=10)
mining_pools_data = mempool_bitcoin_mining.json()

btc_mempool_mining_pools = pd.DataFrame(mining_pools_data['pools']).drop(columns=['poolId', 'poolUniqueId'])

# %%

## Part 5: Lightning Network Data from Mempool
response = requests.get("https://mempool.space/api/v1/lightning/statistics/1m", timeout=10)
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

# Reverse the order of the DataFrame for easier analysis
btc_1m_mempool_lightning = lightning_mempool_total_capacity.iloc[::-1]

# %%

## Part 6: Hashrate Fluctuation Data From Mempool (1 month)
def mempool_get_1m_hashrate():
    """Fetch Bitcoin hashrate data for the past 1 month and return as a Dataframe"""
    url = "https://mempool.space/api/v1/mining/hashrate/1m"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        hashrate_data = response.json()['hashrates']
        df = pd.DataFrame(hashrate_data)
        
        # Format 'timestamp' to datetime and convert 'avgHashrate' to Exahashes per second (EH/s)
        df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        df['avgHashrate_EHs'] = df['avgHashrate'] / 1e18
        df = df.drop(columns=['avgHashrate'])
        
        return df
    else:
        raise ValueError("Failed to fetch 1 Month hashrate data!")

btc_1m_mempool_hashrate = mempool_get_1m_hashrate()

# %%

## Part 7: Fee Breakdown (between ordinals, brc-20, and standard BTC tx) - Dune Analytics 1 Month Data
dune = DuneClient(dune_api, request_timeout=10)
query_result = dune.get_latest_result(2432967)

dune_rows = query_result.get_rows()

# Convert the retrieved data into a DataFrame
btc_1m_dune_fee_breakdown = pd.DataFrame(dune_rows)

# Ensure that the 'Day' column is in datetime format
btc_1m_dune_fee_breakdown['Day'] = pd.to_datetime(btc_1m_dune_fee_breakdown['Day'])

# Get the current date and calculate the date for 31 days (1 month) ago
current_date = datetime.now()
date_1_month_ago = current_date - timedelta(days=31)

# Filter the DataFrame to include only the last month
btc_1m_dune_fee_breakdown = btc_1m_dune_fee_breakdown[btc_1m_dune_fee_breakdown['Day'] >= date_1_month_ago]

# Sort the DataFrame by the 'Day' column from earliest to latest
btc_1m_dune_fee_breakdown = btc_1m_dune_fee_breakdown.sort_values('Day')

# Reset the index to have a clean 'row_num' column
btc_1m_dune_fee_breakdown.reset_index(drop=True, inplace=True)

# %%

## Part 8: Load Dataframes to MongoDB
def load_to_mongodb(df, df_name, date_df, uri):
    client = MongoClient(uri)
    db = client['deftify_research']

    # Check if 'time' exists in date_df
    if 'time' in date_df.columns:
        # Get the latest date from the 'time' column of 'date_df'
        latest_date = date_df['time'].max()

        # Extract the month and year from the latest date
        month = latest_date.strftime('%B').lower()
        year = latest_date.year
    else:
        print(f"'time' column doesn't exist in date_df: {date_df.columns}")

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

# Store all the transformed dataframes into MongoDB collections, with 'btc_1m_mempool_fee' as 'date_df'
load_to_mongodb(btc_1m_w_external, 'btc_1m_w_external', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_1m_mempool_fee, 'btc_1m_mempool_fee', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_1m_mempool_price, 'btc_1m_mempool_price', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_mempool_mining_pools, 'btc_mempool_mining_pools', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_1m_mempool_lightning, 'btc_1m_mempool_lightning', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_1m_mempool_hashrate, 'btc_1m_mempool_hashrate', btc_1m_mempool_fee, mongodb_uri)
load_to_mongodb(btc_1m_dune_fee_breakdown, 'btc_1m_dune_fee_breakdown', btc_1m_mempool_fee, mongodb_uri)
