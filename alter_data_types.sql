DO $$
DECLARE
    -- Declare variables
    v_table_name text;
    v_column_type text;
    -- Cursor to loop through table names that match a pattern
    cur_tables CURSOR FOR
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND (
            table_name LIKE 'btc_90d_w_external'
            OR table_name LIKE 'mempool_stats_30d_grouped'
            OR table_name LIKE 'btc_mining_pools'
            OR table_name LIKE 'btc_combined_data_final'
            OR table_name LIKE 'spot_exchanges_volume'
            OR table_name LIKE 'eth_90d'
            OR table_name LIKE 'eth_gas_fee'
            OR table_name LIKE 'eth_combined_data_final'
            OR table_name LIKE 'eth_tvl'
            OR table_name LIKE 'eth_defi_tvl_top10'
            OR table_name LIKE 'bnb_90d'
            OR table_name LIKE 'bnb_gas_fee'
            OR table_name LIKE 'bnb_combined_data_final'
            OR table_name LIKE 'bnb_tvl'
            OR table_name LIKE 'bnb_defi_tvl_top10'
            OR table_name LIKE 'btc_90d_w_external_%'
            OR table_name LIKE 'mempool_stats_30d_grouped_%'
            OR table_name LIKE 'btc_mining_pools_%'
            OR table_name LIKE 'btc_combined_data_final_%'
            OR table_name LIKE 'spot_exchanges_volume_%'
            OR table_name LIKE 'eth_90d_%'
            OR table_name LIKE 'eth_gas_fee_%'
            OR table_name LIKE 'eth_combined_data_final_%'
            OR table_name LIKE 'eth_tvl_%'
            OR table_name LIKE 'eth_defi_tvl_top10_%'
            OR table_name LIKE 'bnb_90d_%'
            OR table_name LIKE 'bnb_gas_fee_%'
            OR table_name LIKE 'bnb_combined_data_final_%'
            OR table_name LIKE 'bnb_tvl_%'
            OR table_name LIKE 'bnb_defi_tvl_top10_%'
        );
BEGIN
    -- Open the cursor
    OPEN cur_tables;
    
    -- Loop through all table names
    LOOP
        -- Fetch the next table name
        FETCH cur_tables INTO v_table_name;
        EXIT WHEN NOT FOUND;

        -- Apply the ALTER TABLE commands based on the table pattern
        IF v_table_name LIKE 'btc_90d_w_external' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING marketcap::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING vol_24h::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN ndx_volume TYPE numeric USING ndx_volume::numeric,
                        ALTER COLUMN ndx_price TYPE numeric USING ndx_price::numeric,
                        ALTER COLUMN gold_volume TYPE numeric USING gold_volume::numeric,
                        ALTER COLUMN gold_price TYPE numeric USING gold_price::numeric,
                        ALTER COLUMN btc_price_normalized TYPE numeric USING btc_price_normalized::numeric,
                        ALTER COLUMN ndx_price_normalized TYPE numeric USING ndx_price_normalized::numeric,
                        ALTER COLUMN gold_price_normalized TYPE numeric USING gold_price_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'mempool_stats_30d_grouped' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'block_group';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN block_group TYPE numeric USING block_group::numeric,
                        ALTER COLUMN "medianFee" TYPE numeric USING "medianFee"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'btc_mining_pools' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgMatchRate"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgMatchRate" TYPE numeric USING "avgMatchRate"::numeric,
                        ALTER COLUMN "avgFeeDelta" TYPE numeric USING "avgFeeDelta"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'btc_combined_data_final' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'spot_exchanges_volume' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'binance_vol_in_btc';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN binance_vol_in_btc TYPE numeric USING binance_vol_in_btc::numeric,
                        ALTER COLUMN gdax_vol_in_btc TYPE numeric USING gdax_vol_in_btc::numeric,
                        ALTER COLUMN kraken_vol_in_btc TYPE numeric USING kraken_vol_in_btc::numeric,
                        ALTER COLUMN bitstamp_vol_in_btc TYPE numeric USING bitstamp_vol_in_btc::numeric,
                        ALTER COLUMN okex_vol_in_btc TYPE numeric USING okex_vol_in_btc::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_90d' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING replace(marketcap, ',', '')::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING replace(vol_24h::text, ',', '')::numeric,
                        ALTER COLUMN price_vs_btc TYPE numeric USING price_vs_btc::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN eth_vs_usd_normalized TYPE numeric USING eth_vs_usd_normalized::numeric,
                        ALTER COLUMN eth_vs_btc_normalized TYPE numeric USING eth_vs_btc_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_gas_fee' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgGas"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgGas" TYPE numeric USING "avgGas"::numeric,
                        ALTER COLUMN timestamp TYPE timestamp with time zone USING timestamp::timestamp with time zone,
                        ALTER COLUMN "gasPrice_open" TYPE numeric USING "gasPrice_open"::numeric,
                        ALTER COLUMN "gasPrice_close" TYPE numeric USING "gasPrice_close"::numeric,
                        ALTER COLUMN "gasPrice_low" TYPE numeric USING "gasPrice_low"::numeric,
                        ALTER COLUMN "gasPrice_high" TYPE numeric USING "gasPrice_high"::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_combined_data_final' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_tvl' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'tvl';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN tvl TYPE numeric USING replace(tvl::text, ',', '')::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_defi_tvl_top10' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"totalLiquidity_aave"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN "totalLiquidity_aave" TYPE numeric USING replace("totalLiquidity_aave", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_lido" TYPE numeric USING replace("totalLiquidity_lido", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_makerdao" TYPE numeric USING replace("totalLiquidity_makerdao", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_uniswap" TYPE numeric USING replace("totalLiquidity_uniswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_summer.fi" TYPE numeric USING replace("totalLiquidity_summer.fi", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_instadapp" TYPE numeric USING replace("totalLiquidity_instadapp", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_compound-finance" TYPE numeric USING replace("totalLiquidity_compound-finance", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_rocket-pool" TYPE numeric USING replace("totalLiquidity_rocket-pool", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_curve-dex" TYPE numeric USING replace("totalLiquidity_curve-dex", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_convex-finance" TYPE numeric USING replace("totalLiquidity_convex-finance", ',', '')::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_90d' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING replace(marketcap, ',', '')::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING replace(vol_24h::text, ',', '')::numeric,
                        ALTER COLUMN price_vs_btc TYPE numeric USING price_vs_btc::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN bnb_vs_usd_normalized TYPE numeric USING bnb_vs_usd_normalized::numeric,
                        ALTER COLUMN bnb_vs_btc_normalized TYPE numeric USING bnb_vs_btc_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_gas_fee' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgGas"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgGas" TYPE numeric USING "avgGas"::numeric,
                        ALTER COLUMN timestamp TYPE timestamp with time zone USING timestamp::timestamp with time zone,
                        ALTER COLUMN "gasPrice_open" TYPE numeric USING "gasPrice_open"::numeric,
                        ALTER COLUMN "gasPrice_close" TYPE numeric USING "gasPrice_close"::numeric,
                        ALTER COLUMN "gasPrice_low" TYPE numeric USING "gasPrice_low"::numeric,
                        ALTER COLUMN "gasPrice_high" TYPE numeric USING "gasPrice_high"::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_combined_data_final' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_tvl' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'tvl';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN tvl TYPE numeric USING replace(tvl::text, ',', '')::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_defi_tvl_top10' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"totalLiquidity_pancakeswap"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN "totalLiquidity_pancakeswap" TYPE numeric USING replace("totalLiquidity_pancakeswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_venus" TYPE numeric USING replace("totalLiquidity_venus", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_biswap" TYPE numeric USING replace("totalLiquidity_biswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_alpaca-finance" TYPE numeric USING replace("totalLiquidity_alpaca-finance", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_uncx-network" TYPE numeric USING replace("totalLiquidity_uncx-network", ',', '')::numeric;
                $fmt$, v_table_name);
            END IF;

        ELSIF v_table_name LIKE 'btc_90d_w_external_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING marketcap::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING vol_24h::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN ndx_volume TYPE numeric USING ndx_volume::numeric,
                        ALTER COLUMN ndx_price TYPE numeric USING ndx_price::numeric,
                        ALTER COLUMN gold_volume TYPE numeric USING gold_volume::numeric,
                        ALTER COLUMN gold_price TYPE numeric USING gold_price::numeric,
                        ALTER COLUMN btc_price_normalized TYPE numeric USING btc_price_normalized::numeric,
                        ALTER COLUMN ndx_price_normalized TYPE numeric USING ndx_price_normalized::numeric,
                        ALTER COLUMN gold_price_normalized TYPE numeric USING gold_price_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'mempool_stats_30d_grouped_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'block_group';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN block_group TYPE numeric USING block_group::numeric,
                        ALTER COLUMN "medianFee" TYPE numeric USING "medianFee"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'btc_mining_pools_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgMatchRate"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgMatchRate" TYPE numeric USING "avgMatchRate"::numeric,
                        ALTER COLUMN "avgFeeDelta" TYPE numeric USING "avgFeeDelta"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'btc_combined_data_final_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'spot_exchanges_volume_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'binance_vol_in_btc';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN binance_vol_in_btc TYPE numeric USING binance_vol_in_btc::numeric,
                        ALTER COLUMN gdax_vol_in_btc TYPE numeric USING gdax_vol_in_btc::numeric,
                        ALTER COLUMN kraken_vol_in_btc TYPE numeric USING kraken_vol_in_btc::numeric,
                        ALTER COLUMN bitstamp_vol_in_btc TYPE numeric USING bitstamp_vol_in_btc::numeric,
                        ALTER COLUMN okex_vol_in_btc TYPE numeric USING okex_vol_in_btc::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_90d_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING replace(marketcap, ',', '')::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING replace(vol_24h::text, ',', '')::numeric,
                        ALTER COLUMN price_vs_btc TYPE numeric USING price_vs_btc::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN eth_vs_usd_normalized TYPE numeric USING eth_vs_usd_normalized::numeric,
                        ALTER COLUMN eth_vs_btc_normalized TYPE numeric USING eth_vs_btc_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_gas_fee_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgGas"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgGas" TYPE numeric USING "avgGas"::numeric,
                        ALTER COLUMN timestamp TYPE timestamp with time zone USING timestamp::timestamp with time zone,
                        ALTER COLUMN "gasPrice_open" TYPE numeric USING "gasPrice_open"::numeric,
                        ALTER COLUMN "gasPrice_close" TYPE numeric USING "gasPrice_close"::numeric,
                        ALTER COLUMN "gasPrice_low" TYPE numeric USING "gasPrice_low"::numeric,
                        ALTER COLUMN "gasPrice_high" TYPE numeric USING "gasPrice_high"::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_combined_data_final_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_tvl_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'tvl';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN tvl TYPE numeric USING replace(tvl::text, ',', '')::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'eth_defi_tvl_top10_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"totalLiquidity_aave"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN "totalLiquidity_aave" TYPE numeric USING replace("totalLiquidity_aave", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_lido" TYPE numeric USING replace("totalLiquidity_lido", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_makerdao" TYPE numeric USING replace("totalLiquidity_makerdao", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_uniswap" TYPE numeric USING replace("totalLiquidity_uniswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_summer.fi" TYPE numeric USING replace("totalLiquidity_summer.fi", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_instadapp" TYPE numeric USING replace("totalLiquidity_instadapp", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_compound-finance" TYPE numeric USING replace("totalLiquidity_compound-finance", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_rocket-pool" TYPE numeric USING replace("totalLiquidity_rocket-pool", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_curve-dex" TYPE numeric USING replace("totalLiquidity_curve-dex", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_convex-finance" TYPE numeric USING replace("totalLiquidity_convex-finance", ',', '')::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_90d_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'marketcap';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN time TYPE timestamp with time zone USING time::timestamp with time zone,
                        ALTER COLUMN price TYPE numeric USING price::numeric,
                        ALTER COLUMN marketcap TYPE numeric USING replace(marketcap, ',', '')::numeric,
                        ALTER COLUMN vol_24h TYPE numeric USING replace(vol_24h::text, ',', '')::numeric,
                        ALTER COLUMN price_vs_btc TYPE numeric USING price_vs_btc::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN bnb_vs_usd_normalized TYPE numeric USING bnb_vs_usd_normalized::numeric,
                        ALTER COLUMN bnb_vs_btc_normalized TYPE numeric USING bnb_vs_btc_normalized::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_gas_fee_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"avgGas"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "avgGas" TYPE numeric USING "avgGas"::numeric,
                        ALTER COLUMN timestamp TYPE timestamp with time zone USING timestamp::timestamp with time zone,
                        ALTER COLUMN "gasPrice_open" TYPE numeric USING "gasPrice_open"::numeric,
                        ALTER COLUMN "gasPrice_close" TYPE numeric USING "gasPrice_close"::numeric,
                        ALTER COLUMN "gasPrice_low" TYPE numeric USING "gasPrice_low"::numeric,
                        ALTER COLUMN "gasPrice_high" TYPE numeric USING "gasPrice_high"::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_combined_data_final_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"Value"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Value" TYPE numeric USING "Value"::numeric;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_tvl_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = 'tvl';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN tvl TYPE numeric USING replace(tvl::text, ',', '')::numeric,
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone;
                $fmt$, v_table_name);
            END IF;

        -- Apply the ALTER TABLE commands based on the table pattern
        ELSIF v_table_name LIKE 'bnb_defi_tvl_top10_%' THEN
            -- Get the data type of the specified column
            SELECT data_type INTO v_column_type
            FROM information_schema.columns
            WHERE table_name = v_table_name AND column_name = '"totalLiquidity_pancakeswap"';

            -- If the specified column is not of type 'numeric', alter the table
            IF v_column_type != 'numeric' THEN
                EXECUTE format($fmt$
                    ALTER TABLE public.%I
                        ALTER COLUMN "Date" TYPE timestamp with time zone USING "Date"::timestamp with time zone,
                        ALTER COLUMN "totalLiquidity_pancakeswap" TYPE numeric USING replace("totalLiquidity_pancakeswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_venus" TYPE numeric USING replace("totalLiquidity_venus", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_biswap" TYPE numeric USING replace("totalLiquidity_biswap", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_alpaca-finance" TYPE numeric USING replace("totalLiquidity_alpaca-finance", ',', '')::numeric,
                        ALTER COLUMN "totalLiquidity_uncx-network" TYPE numeric USING replace("totalLiquidity_uncx-network", ',', '')::numeric;
                $fmt$, v_table_name);
            END IF;

        END IF;
    END LOOP;
    
    -- Close the cursor
    CLOSE cur_tables;
END $$;