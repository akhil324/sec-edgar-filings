import pandas as pd
import yfinance as yf
import os
import time
import glob
import json # For saving/loading failed tickers list
from datetime import datetime

# --- Configuration (Global Variables for easy modification) ---

# File Paths
MASTER_DATA_PARQUET_DIR = '/Users/akhil/Documents/Work/PI/PROCESSED_DATA/output/sec_company_master_data/'
OUTPUT_PRICES_DIR = '/Users/akhil/Documents/Work/PI/PROCESSED_DATA/output/stock_prices/'
FAILED_TICKERS_FILE = os.path.join(OUTPUT_PRICES_DIR, 'failed_tickers.json')
PROCESSED_BATCHES_LOG = os.path.join(OUTPUT_PRICES_DIR, 'processed_batches.json')

# Batching & Limits (Production)
PROD_TICKER_BATCH_SIZE = 100

# Batching & Limits (Test Mode)
TEST_MODE_ENABLED = False      # SET TO False FOR PRODUCTION RUN
TEST_MODE_TOTAL_TICKERS = 30   # Total tickers to process in test mode
TEST_MODE_BATCH_SIZE = 10      # Batch size for test mode

# Rate Limiting & API Courtesy
DELAY_BETWEEN_BATCHES_SECONDS = 5  # Delay between successful batch downloads
MAX_RETRIES_PER_BATCH = 3
RETRY_DELAY_SECONDS = 10           # Initial delay, will increase with attempts

# Target Exchanges
TARGET_EXCHANGES = ['NYSE', 'Nasdaq'] # Case-insensitive matching will be applied

# --- Utility Class for Logging ---
class Logger:
    @staticmethod
    def log(message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")

# --- Data Handling Classes ---
class TickerExtractor:
    def __init__(self, parquet_dir_path, target_exchanges):
        self.parquet_dir_path = parquet_dir_path
        self.target_exchanges = [exchange.lower() for exchange in target_exchanges]

    def get_target_tickers(self):
        Logger.log(f"Starting ticker extraction from: {self.parquet_dir_path}")
        all_files = glob.glob(os.path.join(self.parquet_dir_path, "*.parquet"))
        if not all_files:
            Logger.log(f"No Parquet files found in {self.parquet_dir_path}")
            return []

        df_list = [pd.read_parquet(file) for file in all_files]
        if not df_list:
            Logger.log("No data found in Parquet files.")
            return []
            
        master_df = pd.concat(df_list, ignore_index=True)
        Logger.log(f"Loaded master data with {len(master_df)} rows.")

        # Ensure columns exist
        if 'exchange' not in master_df.columns or 'ticker' not in master_df.columns:
            Logger.log("Error: 'exchange' or 'ticker' column not found in master data.")
            return []

        # Filter by exchange (case-insensitive)
        master_df['exchange_lower'] = master_df['exchange'].astype(str).str.lower()
        filtered_df = master_df[master_df['exchange_lower'].isin(self.target_exchanges)]

        # Filter out null or invalid tickers
        filtered_df = filtered_df.dropna(subset=['ticker'])
        filtered_df = filtered_df[filtered_df['ticker'].astype(str).str.strip() != '']
        # Basic ticker sanity check (e.g., no spaces, not too long - adjust as needed)
        filtered_df = filtered_df[~filtered_df['ticker'].astype(str).str.contains(' ')]
        filtered_df = filtered_df[filtered_df['ticker'].astype(str).str.len() <= 6]


        unique_tickers = filtered_df['ticker'].unique().tolist()
        Logger.log(f"Found {len(unique_tickers)} unique tickers for exchanges: {', '.join(self.target_exchanges)}")
        return unique_tickers

class YFinanceFetcher:
    def __init__(self, max_retries, retry_delay):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def fetch_batch_data(self, ticker_batch):
        Logger.log(f"Attempting to fetch data for batch: {', '.join(ticker_batch)}")
        for attempt in range(self.max_retries):
            try:
                # yfinance expects a space-separated string or a list for multiple tickers
                data = yf.download(tickers=ticker_batch, period="max", interval="1d", progress=False, group_by='ticker', auto_adjust=False)
                
                if data.empty:
                    Logger.log(f"No data returned for batch (Attempt {attempt + 1}/{self.max_retries}): {', '.join(ticker_batch)}")
                    # If yf.download returns an empty df for all tickers in batch, it's often a valid response (no data)
                    # or an issue with all tickers in that batch.
                    # We consider this "successful" in terms of API call, but data is empty.
                    return pd.DataFrame() # Return empty DataFrame to signify no data found

                # Check if any ticker in the batch actually returned data
                # When group_by='ticker', columns are like ('AAPL', 'Open'), ('MSFT', 'Open')
                # If some tickers fail, they might be missing. If all fail, data might be empty or structured differently.
                # A simple check: if columns are MultiIndex and first level contains ticker symbols from the batch
                valid_tickers_in_response = [col[0] for col in data.columns if isinstance(col, tuple) and len(col) > 1]
                if not any(t in valid_tickers_in_response for t in ticker_batch):
                    Logger.log(f"Data structure unexpected or no valid ticker data in response for batch (Attempt {attempt + 1}/{self.max_retries}).")
                    # Continue to retry if structure is bad
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (attempt + 1)) # Exponential backoff
                        continue
                    else:
                        return pd.DataFrame() # Failed after retries

                Logger.log(f"Successfully fetched data for batch (some or all tickers) (Attempt {attempt + 1}/{self.max_retries})")
                return data
            except Exception as e:
                Logger.log(f"Error fetching batch (Attempt {attempt + 1}/{self.max_retries}): {', '.join(ticker_batch)}. Error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1)) # Exponential backoff
                else:
                    Logger.log(f"Failed to fetch batch after {self.max_retries} attempts: {', '.join(ticker_batch)}")
                    return None # Signifies complete failure for the batch
        return None


class DataProcessor:
    def transform_price_data(self, raw_data_map, ticker_batch):
        """
        Transforms raw yfinance data (potentially a DataFrame with MultiIndex columns if group_by='ticker')
        or a dictionary of DataFrames (if downloaded one by one) into a long format DataFrame.
        For this implementation, yf.download with a list of tickers and group_by='ticker' is used.
        The raw_data_map will be a single DataFrame with MultiIndex columns: (Ticker, PriceType)
        """
        if raw_data_map is None or raw_data_map.empty:
            return pd.DataFrame()

        all_ticker_dfs = []
        for ticker_symbol in ticker_batch:
            try:
                if ticker_symbol in raw_data_map.columns.get_level_values(0):
                    # Select data for the current ticker
                    ticker_df = raw_data_map[ticker_symbol]
                    if 'Adj Close' in ticker_df.columns:
                        adj_close_series = ticker_df['Adj Close'].dropna()
                        if not adj_close_series.empty:
                            temp_df = adj_close_series.reset_index()
                            temp_df.columns = ['Date', 'AdjClosePrice']
                            temp_df['Ticker'] = ticker_symbol
                            all_ticker_dfs.append(temp_df[['Date', 'Ticker', 'AdjClosePrice']])
                    else:
                        Logger.log(f"No 'Adj Close' data for {ticker_symbol} in fetched batch.")
                # else: Ticker was in batch request but yfinance didn't return data for it (e.g., invalid ticker)
            except Exception as e:
                Logger.log(f"Error processing data for ticker {ticker_symbol}: {e}")
                continue
        
        if not all_ticker_dfs:
            return pd.DataFrame()
            
        return pd.concat(all_ticker_dfs, ignore_index=True)

class PersistenceManager:
    def __init__(self, output_dir, failed_tickers_file, processed_batches_log_file):
        self.output_dir = output_dir
        self.failed_tickers_file = failed_tickers_file
        self.processed_batches_log_file = processed_batches_log_file
        os.makedirs(self.output_dir, exist_ok=True)

    def load_failed_tickers(self):
        if os.path.exists(self.failed_tickers_file):
            try:
                with open(self.failed_tickers_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                Logger.log(f"Error decoding JSON from {self.failed_tickers_file}. Starting with no previously failed tickers.")
                return []
        return []

    def save_failed_tickers(self, failed_tickers_list):
        # Overwrites the file with the current complete list of persistently failed tickers
        with open(self.failed_tickers_file, 'w') as f:
            json.dump(list(set(failed_tickers_list)), f, indent=4) # Store unique tickers
        Logger.log(f"Saved/Updated failed tickers to {self.failed_tickers_file}")

    def save_price_data(self, prices_df, filename_suffix="all_prices"):
        if prices_df.empty:
            Logger.log("No price data to save.")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(self.output_dir, f"stock_prices_{filename_suffix}_{timestamp}.parquet")
        prices_df['Date'] = pd.to_datetime(prices_df['Date']) # Ensure Date is datetime
        prices_df.to_parquet(filename, index=False)
        Logger.log(f"Successfully saved {len(prices_df)} price records to {filename}")

    def load_processed_batches(self):
        if os.path.exists(self.processed_batches_log_file):
            try:
                with open(self.processed_batches_log_file, 'r') as f:
                    return set(map(tuple, json.load(f))) # Store batches as tuples of tickers to make them hashable for set
            except json.JSONDecodeError:
                 Logger.log(f"Error decoding JSON from {self.processed_batches_log_file}. Starting fresh.")
                 return set()
        return set()

    def log_processed_batch(self, batch_tuple_sorted, processed_batches_set):
        processed_batches_set.add(batch_tuple_sorted)
        # Convert set of tuples back to list of lists for JSON serialization
        list_to_save = list(map(list, processed_batches_set))
        with open(self.processed_batches_log_file, 'w') as f:
            json.dump(list_to_save, f, indent=4)


class StockPriceOrchestrator:
    def __init__(self, config):
        self.config = config
        self.logger = Logger()
        self.ticker_extractor = TickerExtractor(config['master_data_dir'], config['target_exchanges'])
        self.fetcher = YFinanceFetcher(config['max_retries'], config['retry_delay'])
        self.processor = DataProcessor()
        self.persistence = PersistenceManager(config['output_dir'], config['failed_tickers_file'], config['processed_batches_log'])

    def run_etl(self):
        self.logger.log("Starting Stock Price ETL Process...")

        all_target_tickers = self.ticker_extractor.get_target_tickers()
        if not all_target_tickers:
            self.logger.log("No target tickers found. Exiting.")
            return

        # --- Load previously failed tickers and processed batches (optional, for resuming/avoiding rework) ---
        # For this version, we will mainly focus on saving failed tickers.
        # Reprocessing them can be a separate run or integrated more deeply.
        # We will use processed_batches_log to skip already processed batches in the current run if interrupted.
        
        previously_failed_tickers = self.persistence.load_failed_tickers() # Not actively used to re-queue in this version, but logged
        if previously_failed_tickers:
            self.logger.log(f"Loaded {len(previously_failed_tickers)} previously failed tickers (for information only in this run).")

        processed_batches_set = self.persistence.load_processed_batches()
        if processed_batches_set:
             self.logger.log(f"Loaded {len(processed_batches_set)} previously processed batches. Will attempt to skip.")


        # --- Apply Test Mode if enabled ---
        current_tickers_to_process = all_target_tickers
        current_batch_size = self.config['prod_batch_size']

        if self.config['test_mode_enabled']:
            self.logger.log(f"TEST MODE ENABLED: Processing up to {self.config['test_total_tickers']} tickers in batches of {self.config['test_batch_size']}.")
            current_tickers_to_process = all_target_tickers[:self.config['test_total_tickers']]
            current_batch_size = self.config['test_batch_size']
        else:
            self.logger.log(f"PRODUCTION MODE: Processing {len(current_tickers_to_process)} tickers in batches of {current_batch_size}.")


        all_fetched_prices_dfs = []
        session_failed_tickers = list(previously_failed_tickers) # Start with old failed ones to update the file

        num_batches = (len(current_tickers_to_process) + current_batch_size - 1) // current_batch_size
        self.logger.log(f"Total tickers to process this session: {len(current_tickers_to_process)}. Number of batches: {num_batches}")

        for i in range(num_batches):
            start_index = i * current_batch_size
            end_index = start_index + current_batch_size
            ticker_batch = current_tickers_to_process[start_index:end_index]
            
            # Sort batch for consistent logging and checking against processed_batches_set
            batch_tuple_sorted = tuple(sorted(ticker_batch))

            if batch_tuple_sorted in processed_batches_set:
                self.logger.log(f"Skipping already processed batch {i+1}/{num_batches}: {', '.join(ticker_batch)}")
                continue

            self.logger.log(f"Processing Batch {i+1}/{num_batches}: {', '.join(ticker_batch)}")

            raw_data = self.fetcher.fetch_batch_data(ticker_batch)

            if raw_data is None: # Signifies complete failure for the batch after retries
                self.logger.log(f"Adding entire batch to failed tickers: {', '.join(ticker_batch)}")
                session_failed_tickers.extend(ticker_batch)
            elif not raw_data.empty:
                transformed_df = self.processor.transform_price_data(raw_data, ticker_batch)
                if not transformed_df.empty:
                    all_fetched_prices_dfs.append(transformed_df)
                    self.logger.log(f"Successfully processed and transformed data for batch {i+1}/{num_batches}.")
                else:
                    self.logger.log(f"No valid price data extracted after transformation for batch {i+1}/{num_batches}.")
                    # Check which tickers from this batch didn't make it into transformed_df
                    # This logic is a bit more complex as yfinance might return partial data
                    # For simplicity now, if raw_data was fetched but transform is empty, we don't add to failed_tickers
                    # but one could compare `ticker_batch` with `transformed_df['Ticker'].unique()`
            else: # raw_data is an empty DataFrame (no data found for any ticker in batch)
                 self.logger.log(f"No data found for any ticker in batch {i+1}/{num_batches}. Not an error, but no data to process.")

            # Log this batch as processed (even if it failed or had no data, we attempted it)
            self.persistence.log_processed_batch(batch_tuple_sorted, processed_batches_set)

            if i < num_batches - 1 : # Don't sleep after the last batch
                self.logger.log(f"Delaying for {self.config['delay_between_batches']} seconds before next batch...")
                time.sleep(self.config['delay_between_batches'])
        
        # --- Consolidate and Save ---
        if all_fetched_prices_dfs:
            final_prices_df = pd.concat(all_fetched_prices_dfs, ignore_index=True)
            self.persistence.save_price_data(final_prices_df)
        else:
            self.logger.log("No data collected in this session to save.")

        # Save all unique tickers that failed in this session or were previously failed
        if session_failed_tickers:
            self.persistence.save_failed_tickers(list(set(session_failed_tickers))) # Ensure uniqueness

        self.logger.log("Stock Price ETL Process Finished.")


# --- Main Execution ---
if __name__ == "__main__":
    config_params = {
        'master_data_dir': MASTER_DATA_PARQUET_DIR,
        'output_dir': OUTPUT_PRICES_DIR,
        'failed_tickers_file': FAILED_TICKERS_FILE,
        'processed_batches_log': PROCESSED_BATCHES_LOG,
        'prod_batch_size': PROD_TICKER_BATCH_SIZE,
        'test_mode_enabled': TEST_MODE_ENABLED,
        'test_total_tickers': TEST_MODE_TOTAL_TICKERS,
        'test_batch_size': TEST_MODE_BATCH_SIZE,
        'max_retries': MAX_RETRIES_PER_BATCH,
        'retry_delay': RETRY_DELAY_SECONDS,
        'delay_between_batches': DELAY_BETWEEN_BATCHES_SECONDS,
        'target_exchanges': TARGET_EXCHANGES
    }

    orchestrator = StockPriceOrchestrator(config_params)
    orchestrator.run_etl()


# df["Date"] = df.Date.dt.date
# bq load \
#     --source_format=PARQUET \
#     --replace \
#     --time_partitioning_field=Date \
#     --time_partitioning_type=YEAR \
#     --clustering_fields=Ticker \
#     "<bq_project>:sec.stock_prices" \
#     "gs://<bucket>/sec/stock_prices_all_prices_20250527_185456.parquet" \
#     "Date:DATE,Ticker:STRING,AdjClosePrice:FLOAT64"