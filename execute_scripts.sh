#!/bin/bash

# Exit the script if any command fails
set -e

# Set Python path
# PYTHON_PATH="/usr/bin/python3"
PYTHON_PATH=$(which python3)

# Function to retry script execution
run_with_retry() {
    local script_name=$1
    local retries=1

    while (( retries <= 2 )); do
        if "$PYTHON_PATH" "$script_name"; then
            echo "$(date): $script_name script execution successful" >> logs/results.txt
            return 0
        else
            echo "$(date): $script_name script execution failed" >> logs/results.txt
            if (( retries == 2 )); then
                exit 1
            fi
            echo "$(date): Retrying $script_name in 300 seconds..." >> logs/results.txt
            sleep 300
            ((retries++))
        fi
    done
}

# Run Python ETL scripts
echo "Running Python ETL scripts..."
run_with_retry btc_etl.py
run_with_retry eth_etl.py

echo "All ETL scripts executed successfully."

# Run AI Analysis script
echo "Running AI Analysis script..."
run_with_retry ai_analysis_fetch.py

echo "AI Analysis script executed successfully."

# Read the month and year from the .txt file
month_year=$(cat month_year.txt)

# Generate all the new report pages
echo "Generating all the new report pages..."
sed "s/_april_2024/_${month_year}/g" pages/btc_april_2024.py > "pages/btc_${month_year}.py"
sed "s/_april_2024/_${month_year}/g" pages/eth_april_2024.py > "pages/eth_${month_year}.py"

# Replace every mention of "April 2024" with the latest month and year
formatted_month_year=$(echo $month_year | sed 's/_/ /' | tr '[:lower:]' '[:upper:]' | awk '{for(i=1;i<=NF;i++)sub(/./,toupper(substr($i,1,1)),$i)}1')
sed -i "s/APRIL 2024/${formatted_month_year}/g" "pages/btc_${month_year}.py"
sed -i "s/APRIL 2024/${formatted_month_year}/g" "pages/eth_${month_year}.py"

echo "All new report pages generated successfully."

# Update the homepage - copy the content of the newly created BTC report page to home.py
echo "Updating the homepage..."
cp "pages/btc_${month_year}.py" "pages/home.py"

# Replace the title and path in home.py
sed -i "s/dash.register_page(__name__, title='BTC ${formatted_month_year} Report')/dash.register_page(__name__, title='Deftify Monthly Cryptocurrency Analysis - Homepage', path='\/')/g" "pages/home.py"

echo "Homepage updated successfully."