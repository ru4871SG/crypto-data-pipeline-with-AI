"""
AI Analysis Fetch - Monthly
"""

import os
import json
import pandas as pd

from dotenv import load_dotenv
from pymongo import MongoClient

from ai_analyzer.ai_data_analysis import data_analyzer

## Connect to environment variables
load_dotenv()
mongodb_uri = os.getenv('MONGODB_URI')

# Create a MongoDB client
client = MongoClient(mongodb_uri)
# Access the 'deftify_research' database
db = client.deftify_research

# Read the month and year from the .txt file
with open('month_year.txt', 'r') as f:
    month_year = f.read().strip()


# Access the collections and load all documents into a pandas DataFrame
collection_btc_1m_w_external = db[f"btc_1m_w_external_{month_year}"]
collection_btc_1m_mempool_fee = db[f"btc_1m_mempool_fee_{month_year}"]
collection_btc_1m_mempool_price = db[f"btc_1m_mempool_price_{month_year}"]
collection_btc_1m_mempool_hashrate = db[f"btc_1m_mempool_hashrate_{month_year}"]
collection_btc_mempool_mining_pools = db[f"btc_mempool_mining_pools_{month_year}"]
collection_btc_1m_mempool_lightning = db[f"btc_1m_mempool_lightning_{month_year}"]
collection_btc_1m_dune_fee_breakdown = db[f"btc_1m_dune_fee_breakdown_{month_year}"]

collection_eth_1m_w_external = db[f"eth_1m_w_external_{month_year}"]
collection_eth_1m_gas_fee = db[f"eth_1m_gas_fee_{month_year}"]
collection_eth_1m_uniswap_data = db[f"eth_1m_uniswap_data_{month_year}"]
collection_eth_1m_tvl = db[f"eth_1m_tvl_{month_year}"]
collection_eth_1m_l2_bridge_all = db[f"eth_1m_l2_bridge_all_{month_year}"]

collection_chainlink_1m_w_external = db[f"chainlink_1m_w_external_{month_year}"]
collection_chainlink_1m_uniswap_data = db[f"chainlink_1m_uniswap_data_{month_year}"]
collection_chainlink_1m_aave_data = db[f"chainlink_1m_aave_data_{month_year}"]


btc_1m_w_external = pd.DataFrame(list(collection_btc_1m_w_external.find()))
btc_1m_mempool_fee = pd.DataFrame(list(collection_btc_1m_mempool_fee.find()))
btc_1m_mempool_price = pd.DataFrame(list(collection_btc_1m_mempool_price.find()))
btc_1m_mempool_hashrate = pd.DataFrame(list(collection_btc_1m_mempool_hashrate.find()))
btc_mempool_mining_pools = pd.DataFrame(list(collection_btc_mempool_mining_pools.find()))
btc_1m_mempool_lightning = pd.DataFrame(list(collection_btc_1m_mempool_lightning.find()))
btc_1m_dune_fee_breakdown = pd.DataFrame(list(collection_btc_1m_dune_fee_breakdown.find()))

eth_1m_w_external = pd.DataFrame(list(collection_eth_1m_w_external.find()))
eth_1m_gas_fee = pd.DataFrame(list(collection_eth_1m_gas_fee.find()))
eth_1m_uniswap_data = pd.DataFrame(list(collection_eth_1m_uniswap_data.find()))
eth_1m_tvl = pd.DataFrame(list(collection_eth_1m_tvl.find()))
eth_1m_l2_bridge_all = pd.DataFrame(list(collection_eth_1m_l2_bridge_all.find()))

chainlink_1m_w_external = pd.DataFrame(list(collection_chainlink_1m_w_external.find()))
chainlink_1m_uniswap_data = pd.DataFrame(list(collection_chainlink_1m_uniswap_data.find()))
chainlink_1m_aave_data = pd.DataFrame(list(collection_chainlink_1m_aave_data.find()))

## Extract the latest month and year from the 'btc_1m_mempool_fee' dataframe
if 'time' in btc_1m_mempool_fee.columns:
    latest_date = btc_1m_mempool_fee['time'].max()
    month = latest_date.strftime('%B').lower()
    year = latest_date.year
    json_filename = f"analysis_result_{month}_{year}.json"
else:
    raise ValueError(f"'time' column doesn't exist in btc_1m_mempool_fee: {btc_1m_mempool_fee.columns}")


## Function to remove text after the last period
def remove_after_last_period(text):
    if '.' in text:
        return text[:text.rfind('.')+1]
    return text

## Generate AI analysis texts
# For the 'btc_1m_w_external' dataframe
btc_1m_w_external_filtered = btc_1m_w_external.drop(columns=["target_block", "wbtc_price_in_eth", "eth_price_in_usd", "wbtc_price_in_usd", "paxg_price_in_usd", "paxg_price_in_eth"], errors='ignore')

text_to_analyze_btc_1m_w_external = ', '.join(f"{col}: {btc_1m_w_external_filtered[col].tolist()}" for col in btc_1m_w_external_filtered.columns)
prompt_btc_1m_w_external = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the price change between btc_price_normalized and gold_price_normalized on: '{text_to_analyze_btc_1m_w_external}'. "
    f"Keep the review concise and do not mention the numbers, nor the terms btc_price_normalized and gold_price_normalized. "
    f"Just mention the overall trend differences."
)
analysis_result_btc_1m_w_external = data_analyzer(prompt_btc_1m_w_external)
analysis_result_btc_1m_w_external = remove_after_last_period(analysis_result_btc_1m_w_external)
analysis_data_btc_1m_w_external = {'btc_1m_w_external': analysis_result_btc_1m_w_external}

# For the 'btc_1m_w_external' dataframe, but the correlation part
prompt_btc_1m_w_external_correlation = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the correlation strength between btc_price_normalized and gold_price_normalized on: '{text_to_analyze_btc_1m_w_external}'. "
    f"Keep the review concise and do not mention the numbers, nor the terms btc_price_normalized and gold_price_normalized. "
    f"Just mention the overall correlation strength."
)
analysis_result_btc_1m_w_external_correlation = data_analyzer(prompt_btc_1m_w_external)
analysis_result_btc_1m_w_external_correlation = remove_after_last_period(analysis_result_btc_1m_w_external_correlation)
analysis_data_btc_1m_w_external_correlation = {'btc_1m_w_external_correlation': analysis_result_btc_1m_w_external_correlation}


# For the 'btc_1m_mempool_fee' dataframe
btc_1m_mempool_fee_filtered = btc_1m_mempool_fee.drop(columns=["time", "timestamp"], errors='ignore')

text_to_analyze_btc_1m_mempool_fee = ', '.join(f"{col}: {btc_1m_mempool_fee_filtered[col].tolist()}" for col in btc_1m_mempool_fee_filtered.columns)
prompt_btc_1m_mempool_fee = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the fee changes (from the column avgFee_50) over time on: '{text_to_analyze_btc_1m_mempool_fee}'. "
    f"Keep the review concise and do not mention the numbers or time or timestamp or column names, just analyze the overall fee changes over time."
)
analysis_result_btc_1m_mempool_fee = data_analyzer(prompt_btc_1m_mempool_fee)
analysis_result_btc_1m_mempool_fee = remove_after_last_period(analysis_result_btc_1m_mempool_fee)
analysis_data_btc_1m_mempool_fee = {'btc_1m_mempool_fee': analysis_result_btc_1m_mempool_fee}


# For the 'btc_1m_mempool_hashrate' dataframe
text_to_analyze_btc_1m_mempool_hashrate = ', '.join(f"{col}: {btc_1m_mempool_hashrate[col].tolist()}" for col in btc_1m_mempool_hashrate.columns)
prompt_btc_1m_mempool_hashrate = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the hashrate changes (from the column avgHashrate_EHs) on: '{text_to_analyze_btc_1m_mempool_hashrate}'. "
    f"Keep the review concise and do not mention the numbers or time or timestamp, just analyze the average hashrate changes over time."
)
analysis_result_btc_1m_mempool_hashrate = data_analyzer(prompt_btc_1m_mempool_hashrate)
analysis_result_btc_1m_mempool_hashrate = remove_after_last_period(analysis_result_btc_1m_mempool_hashrate)
analysis_data_btc_1m_mempool_hashrate = {'btc_1m_mempool_hashrate': analysis_result_btc_1m_mempool_hashrate}


# For the 'btc_mempool_mining_pools' dataframe
text_to_analyze_btc_mempool_mining_pools = ', '.join(f"{col}: {btc_mempool_mining_pools[col].tolist()}" for col in btc_mempool_mining_pools.columns)
prompt_btc_mempool_mining_pools = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the mining pools names and rankings on: '{text_to_analyze_btc_mempool_mining_pools}'. "
    f"Keep the review concise and do not mention the numbers, just mention which mining pool names are ranking well."
)
analysis_result_btc_mempool_mining_pools = data_analyzer(prompt_btc_mempool_mining_pools)
analysis_result_btc_mempool_mining_pools = remove_after_last_period(analysis_result_btc_mempool_mining_pools)
analysis_data_btc_mempool_mining_pools = {'btc_mempool_mining_pools': analysis_result_btc_mempool_mining_pools}


# For the 'btc_1m_mempool_lightning' dataframe
text_to_analyze_btc_1m_mempool_lightning = ', '.join(f"{col}: {btc_1m_mempool_lightning[col].tolist()}" for col in btc_1m_mempool_lightning.columns)
prompt_btc_1m_mempool_lightning = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time for column total_capacity and channel_count on: '{text_to_analyze_btc_1m_mempool_lightning}'. "
    f"Keep the review concise and do not mention the numbers, also do not mention the terms total_capacity and channel_count. Just analyze their changes over time."
)
analysis_result_btc_1m_mempool_lightning = data_analyzer(prompt_btc_1m_mempool_lightning)
analysis_result_btc_1m_mempool_lightning = remove_after_last_period(analysis_result_btc_1m_mempool_lightning)
analysis_data_btc_1m_mempool_lightning = {'btc_1m_mempool_lightning': analysis_result_btc_1m_mempool_lightning}


# For the 'btc_1m_dune_fee_breakdown' dataframe
text_to_analyze_btc_1m_dune_fee_breakdown = ', '.join(f"{col}: {btc_1m_dune_fee_breakdown[col].tolist()}" for col in btc_1m_dune_fee_breakdown.columns)
prompt_btc_1m_dune_fee_breakdown = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the trend differences and their changes over time between BRC20 transactions (from column BRC20_Tx), Non-BRC20 transactions (from column Non_BRC20_Ordi_Tx), and Non-Ordinal transactions (from column Non_Odrdinal_Tx) on: '{text_to_analyze_btc_1m_dune_fee_breakdown}'. "
    f"Keep the review concise and do not mention the numbers, just analyze their changes over time and the overall trend differences. Also, do not mention the terms BRC20_Tx, Non_BRC20_Ordi_Tx, and Non_Odrdinal_Tx."
)
analysis_result_btc_1m_dune_fee_breakdown = data_analyzer(prompt_btc_1m_dune_fee_breakdown)
analysis_result_btc_1m_dune_fee_breakdown = remove_after_last_period(analysis_result_btc_1m_dune_fee_breakdown)
analysis_data_btc_1m_dune_fee_breakdown = {'btc_1m_dune_fee_breakdown': analysis_result_btc_1m_dune_fee_breakdown}


# For the 'eth_1m_w_external' dataframe
text_to_analyze_eth_1m_w_external = ', '.join(f"{col}: {eth_1m_w_external[col].tolist()}" for col in eth_1m_w_external.columns)
prompt_eth_1m_w_external = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the price change between ETH price (from column eth_price_normalized) and BTC Price (from column btc_price_normalized) on: '{text_to_analyze_eth_1m_w_external}'. "
    f"Keep the review concise and do not mention the numbers or column names, just mention the overall trend differences."
)
analysis_result_eth_1m_w_external = data_analyzer(prompt_eth_1m_w_external)
analysis_result_eth_1m_w_external = remove_after_last_period(analysis_result_eth_1m_w_external)
analysis_data_eth_1m_w_external = {'eth_1m_w_external': analysis_result_eth_1m_w_external}

# For the 'eth_1m_gas_fee' dataframe
text_to_analyze_eth_1m_gas_fee = ', '.join(f"{col}: {eth_1m_gas_fee[col].tolist()}" for col in eth_1m_gas_fee.columns)
prompt_eth_1m_gas_fee = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the gas fee changes over time from column gasPrice_close on: '{text_to_analyze_eth_1m_gas_fee}'. "
    f"Keep the review concise and do not mention the numbers or time or timestamp or column names, just analyze the overall fee changes over time."
)
analysis_result_eth_1m_gas_fee = data_analyzer(prompt_eth_1m_gas_fee)
analysis_result_eth_1m_gas_fee = remove_after_last_period(analysis_result_eth_1m_gas_fee)
analysis_data_eth_1m_gas_fee = {'eth_1m_gas_fee': analysis_result_eth_1m_gas_fee}

# For the 'eth_1m_uniswap_data' dataframe
text_to_analyze_eth_1m_uniswap_data = ', '.join(f"{col}: {eth_1m_uniswap_data[col].tolist()}" for col in eth_1m_uniswap_data.columns)
prompt_eth_1m_uniswap_data = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time of TVL in USD (from column tvlUSD) and Volume in USD (from column volumeUSD) on: '{text_to_analyze_eth_1m_uniswap_data}'. "
    f"Keep the review concise and do not mention the numbers or column names, just analyze their changes over time and the overall trend."
)
analysis_result_eth_1m_uniswap_data = data_analyzer(prompt_eth_1m_uniswap_data)
analysis_result_eth_1m_uniswap_data = remove_after_last_period(analysis_result_eth_1m_uniswap_data)
analysis_data_eth_1m_uniswap_data = {'eth_1m_uniswap_data': analysis_result_eth_1m_uniswap_data}

# For the 'eth_1m_tvl' dataframe
text_to_analyze_eth_1m_tvl = ', '.join(f"{col}: {eth_1m_tvl[col].tolist()}" for col in eth_1m_tvl.columns)
prompt_eth_1m_tvl = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time and compare the differences between TVL from Aave (from column tvl_aave), TVL from Lido (from column tvl_lido), and TVL from MakerDAO (from column tvl_makerdao): '{text_to_analyze_eth_1m_tvl}'. "
    f"Keep the review concise and do not mention the numbers or column names, just analyze their changes over time and the differences among them."
)
analysis_result_eth_1m_tvl = data_analyzer(prompt_eth_1m_tvl)
analysis_result_eth_1m_tvl = remove_after_last_period(analysis_result_eth_1m_tvl)
analysis_data_eth_1m_tvl = {'eth_1m_tvl': analysis_result_eth_1m_tvl}

# For the 'eth_1m_l2_bridge_all' dataframe
text_to_analyze_eth_1m_l2_bridge_all = ', '.join(f"{col}: {eth_1m_l2_bridge_all[col].tolist()}" for col in eth_1m_l2_bridge_all.columns)
prompt_eth_1m_l2_bridge_all = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time and compare the differences between the users for Arbitrum (from column users_arbitrum), users from Base (from column users_base), users from Optimism (from column users_optimism), users from Starknet (from column users_starknet), and users from ZKSync (from column users_zksync) on: '{text_to_analyze_eth_1m_l2_bridge_all}'. "
    f"Keep the review concise and do not mention the numbers or column names, just analyze their changes over time and the differences among each other."
)
analysis_result_eth_1m_l2_bridge_all = data_analyzer(prompt_eth_1m_l2_bridge_all)
analysis_result_eth_1m_l2_bridge_all = remove_after_last_period(analysis_result_eth_1m_l2_bridge_all)
analysis_data_eth_1m_l2_bridge_all = {'eth_1m_l2_bridge_all': analysis_result_eth_1m_l2_bridge_all}

# For the 'chainlink_1m_w_external' dataframe
text_to_analyze_chainlink_1m_w_external = ', '.join(f"{col}: {chainlink_1m_w_external[col].tolist()}" for col in chainlink_1m_w_external.columns)
prompt_chainlink_1m_w_external = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the price change between Chainlink price (from column chainlink_price_normalized) and BTC Price (from column btc_price_normalized) and ETH Price (from column eth_price_normalized) on: '{text_to_analyze_chainlink_1m_w_external}'. "
    f"Keep the review concise and do not mention the numbers or column names, just mention the overall trend differences."
)
analysis_result_chainlink_1m_w_external = data_analyzer(prompt_chainlink_1m_w_external)
analysis_result_chainlink_1m_w_external = remove_after_last_period(analysis_result_chainlink_1m_w_external)
analysis_data_chainlink_1m_w_external = {'chainlink_1m_w_external': analysis_result_chainlink_1m_w_external}

# For the 'chainlink_1m_uniswap_data' dataframe
chainlink_1m_uniswap_data_filtered = chainlink_1m_uniswap_data.drop(columns=["_id", "target_block", "txCount", "volumeUSD"], errors='ignore')

text_to_analyze_chainlink_1m_uniswap_data = ', '.join(f"{col}: {chainlink_1m_uniswap_data_filtered[col].tolist()}" for col in chainlink_1m_uniswap_data_filtered.columns)
prompt_chainlink_1m_uniswap_data = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time of Chainlink TVL in USD for Uniswap Protocol (from column totalValueLockedUSD) and daily volume in USD from Uniswap Protocol (from column dailyVolumeUSD) on: '{text_to_analyze_chainlink_1m_uniswap_data}'. "
    f"Keep the review concise and do not mention the numbers or column names, just analyze their changes over time."
)
analysis_result_chainlink_1m_uniswap_data = data_analyzer(prompt_chainlink_1m_uniswap_data)
analysis_result_chainlink_1m_uniswap_data = remove_after_last_period(analysis_result_chainlink_1m_uniswap_data)
analysis_data_chainlink_1m_uniswap_data = {'chainlink_1m_uniswap_data': analysis_result_chainlink_1m_uniswap_data}

# For the 'chainlink_1m_aave_data' dataframe
chainlink_1m_aave_data_filtered = chainlink_1m_aave_data.drop(columns=["_id", "target_block"], errors='ignore')

text_to_analyze_chainlink_1m_aave_data = ', '.join(f"{col}: {chainlink_1m_aave_data_filtered[col].tolist()}" for col in chainlink_1m_aave_data_filtered.columns)
prompt_chainlink_1m_aave_data = (
    f"Analyze the data and provide a cryptocurrency analysis report within one paragraph (maximum 4 sentences)."
    f"Analyze the changes over time of Chainlink TVL in USD for Aave Protocol (from column totalValueLockedUSD) and total borrow balance in USD for Aave Protocol (from column totalBorrowBalanceUSD) and total deposit balance in USD for Aave Protocol (from column totalDepositBalanceUSD) on: '{text_to_analyze_chainlink_1m_aave_data}'. "
    f"Keep the review concise and do not mention the numbers or column names, just analyze their changes over time."
)
analysis_result_chainlink_1m_aave_data = data_analyzer(prompt_chainlink_1m_aave_data)
analysis_result_chainlink_1m_aave_data = remove_after_last_period(analysis_result_chainlink_1m_aave_data)
analysis_data_chainlink_1m_aave_data = {'chainlink_1m_aave_data': analysis_result_chainlink_1m_aave_data}


## Update the dictionary with all the analysis results
analysis_data = {
    **analysis_data_btc_1m_w_external,
    **analysis_data_btc_1m_w_external_correlation,
    **analysis_data_btc_1m_mempool_fee,
    **analysis_data_btc_1m_mempool_hashrate,
    **analysis_data_btc_mempool_mining_pools,
    **analysis_data_btc_1m_mempool_lightning,
    **analysis_data_btc_1m_dune_fee_breakdown,
    **analysis_data_eth_1m_w_external,
    **analysis_data_eth_1m_gas_fee,
    **analysis_data_eth_1m_uniswap_data,
    **analysis_data_eth_1m_tvl,
    **analysis_data_eth_1m_l2_bridge_all,
    **analysis_data_chainlink_1m_w_external,
    **analysis_data_chainlink_1m_uniswap_data,
    **analysis_data_chainlink_1m_aave_data
}


# Save the result to a JSON file with the dynamic filename
os.makedirs('pages/ai_text_result', exist_ok=True)

with open(f'pages/ai_text_result/{json_filename}', 'w') as json_file:
    json.dump(analysis_data, json_file)

print(f"Analysis result saved to {json_filename}")
