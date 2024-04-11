#!/bin/bash

# Exit the script if any command fails
set -e

# Set the paths to the Python and PSQL executables
PYTHON_PATH="/usr/bin/python3"
PSQL_PATH="/usr/bin/psql"

# Load environment variables from .env file
source .env

# Run Python scripts
echo "Running Python scripts..."
"$PYTHON_PATH" btc_script.py
"$PYTHON_PATH" btc_script_alt.py
"$PYTHON_PATH" eth_script.py
"$PYTHON_PATH" bnb_script.py

# Run SQL script
echo "Running SQL script..."
"$PSQL_PATH" -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME -f alter_data_types.sql

echo "All scripts executed successfully."
