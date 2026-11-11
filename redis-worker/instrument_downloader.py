import logging
import os
import requests
import pandas as pd
from io import StringIO

# --- Configuration ---
MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_FILE_1 = "instruments_1.csv"
OUTPUT_FILE_2 = "instruments_2.csv"
SPLIT_SIZE = 5000  # Hardcoded split size

# Define the columns you want to filter by
FILTER_COLS = {
    'SEM_EXM_EXCH_ID': 'NSE',        # Value for NSE
    'SEM_INSTRUMENT_NAME': 'EQUITY'  # Value for Equity
}

# Define the exact columns you want to keep in the final files
COLUMNS_TO_KEEP = [
    'SEM_SMST_SECURITY_ID',
    'SEM_TRADING_SYMBOL',
    'SM_SYMBOL_NAME',
    'SEM_EXM_EXCH_ID',
    'SEM_INSTRUMENT_NAME'
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_and_split_instruments():
    """
    Downloads the master instrument list from DhanHQ,
    filters for NSE Equity, keeps only specific columns,
    and splits the result into two files.
    """
    logging.info("Starting instrument download...")
    
    try:
        # 1. Download the master file
        response = requests.get(MASTER_URL)
        response.raise_for_status()  # Raise an error for bad responses
        
        logging.info("Master file downloaded. Processing...")

        # 2. Read the file with pandas
        csv_data = StringIO(response.content.decode('utf-8'))
        df = pd.read_csv(csv_data)
        logging.info(f"Loaded {len(df)} total instruments from master file.")

        # 3. Check if all required columns exist
        required_cols_for_processing = list(FILTER_COLS.keys()) + COLUMNS_TO_KEEP
        
        # Check for missing columns
        missing_cols = [col for col in required_cols_for_processing if col not in df.columns]
        if missing_cols:
            logging.error(f"Critical: The master CSV is missing required columns: {missing_cols}. Aborting.")
            return

        # 4. Filter for NSE Equity
        df_filtered = df[
            (df['SEM_EXM_EXCH_ID'] == FILTER_COLS['SEM_EXM_EXCH_ID']) &
            (df['SEM_INSTRUMENT_NAME'] == FILTER_COLS['SEM_INSTRUMENT_NAME'])
        ].copy() # Use .copy() to avoid pandas warnings
        
        if df_filtered.empty:
            logging.error("No instruments found after filtering for NSE and EQUITY. Aborting.")
            return

        logging.info(f"Filtered down to {len(df_filtered)} total NSE Equity instruments.")

        # 5. Keep only the specified columns
        df_final = df_filtered[COLUMNS_TO_KEEP]
        
        # 6. Split the Final DataFrame (Hardcoded)
        df_1 = df_final.iloc[:SPLIT_SIZE]  # First 5000 rows
        df_2 = df_final.iloc[SPLIT_SIZE:]  # All rows from 5000 onwards
        
        # 7. Save the two files
        df_1.to_csv(OUTPUT_FILE_1, index=False)
        df_2.to_csv(OUTPUT_FILE_2, index=False)
        
        logging.info(f"Successfully saved {len(df_1)} instruments to {OUTPUT_FILE_1}")
        logging.info(f"Successfully saved {len(df_2)} instruments to {OUTPUT_FILE_2}")
        logging.info("Download and split complete.")


    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download file: {e}")
    except pd.errors.EmptyDataError:
        logging.error("Downloaded file is empty.")
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Running manual instrument download...")
    download_and_split_instruments()
    logging.info("Script finished.")