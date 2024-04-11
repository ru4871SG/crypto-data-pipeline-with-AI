# Crypto ETL Scripts

Welcome to the repository of my Cryptocurrency ETL (Extract, Transform, Load) scripts. The repository contains a collection of 4 Python scripts, 1 SQL script, and 1 Bash script. 

The Python scripts are designed to extract and transform data for Bitcoin, Ethereum, and BNB from various free API endpoints, and subsequently store the data into your local PostgreSQL or MongoDB database.

The SQL script modifies the data types once the data has been stored in the database.

Lastly, the Bash script is a utility tool that can be used to automatically execute all the other scripts in the correct order.


## Table of Contents

- [Usage](#usage)
- [Explanation](#explanation)

## Usage

The process is fairly straightforward. First of all, you need to create your own `.env` file with your configuration settings for Postgre and MongoDB. You should also include your API keys for TheGraph, Dune Analytics, and Owlracle. Please check `.env.example` to learn the structure.

Afterward, you just need to run the Bash script `execute_scripts.sh`, which will automatically execute all the Python scripts, followed by the SQL script. You can also set up a cron job to automatically extract new data every once in awhile (preferrably every month).

## Explanation
There are 4 Python scripts, 1 SQL script, and 1 Bash script:

`btc_script.py` - This is the ETL (Extract, Transform, Load) script for Bitcoin data. It is used to extract and transform data from three different API sources (CoinGecko, MemPool, and Yahoo Finance). Once the data has been processed, the dataframes are automatically stored into your local PostgreSQL database. You can change the last code cell (part 8) if you don't want to store the dataframes into PostgreSQL. If you run this script more than once, you will create multiple tables.

`eth_script.py` - This is the ETL script for Ethereum data. It is used to extract and transform data from three different API sources (CoinGecko, Owlracle, and DefiLlama). You can change the last code cell (part 7) if you don't want to store the dataframes into PostgreSQL. If you run this script more than once, you will create multiple tables.

`bnb_script.py` - This is the ETL script for BNB data. It is used to extract and transform data from three different API sources (CoinGecko, Owlracle, and DefiLlama). Just like the BTC and ETH scripts, you can change the last code cell (part 7) if you don't want to store the dataframes into PostgreSQL. If you run this script more than once, you will create multiple tables.

`btc_script_alt.py` - This is similar to `btc_script.py` but with alternative data sources. Instead of using CoinGecko and Yahoo Finance, this script extracts data from TheGraph and Dune Analytics. It also extracts more data from Mempool for better analyses. Also, this script stores the transformed data to MongoDB database instead of PostgreSQL. The MongoDB collections are named automatically based on the latest date, using the format `{df_name}_{month}_{year}`. If a collection with the generated name already exists, the script skips the data upload for that collection.

Note: If you encounter any error while running any of the above Python scripts, most likely it's because you hit an API rate limit.

`alter_data_types.sql` - This is the SQL script that can be used to alter data types in your PostgreSQL database. Only execute this SQL script after you have successfully stored the data into your database using the above Python scripts.

`execute_scripts.sh` - This is the Bash script that you can run to execute all the above scripts in the correct order (`btc_script.py` -> `btc_script_alt.py` -> `eth_script.py` -> `bnb_script.py` -> `alter_data_types.sql`). For example, if you want to get all the data every month, you can set up a monthly cron job using this Bash script. That's it!
