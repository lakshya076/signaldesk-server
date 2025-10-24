import json
import time
from datetime import timedelta, date

from dhanhq import dhanhq
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
import redis
from typing import Optional, Dict
import logging
import asyncio

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration and Initialization ---
supabase_url: str = os.environ.get("URL")
supabase_key: str = os.environ.get("SERVICE_KEY")
supabase_anon_key: str = os.environ.get("ANON_KEY")
dhan_client_id: str = os.environ.get("DHAN_CLIENT_ID")
dhan_access_token: str = os.environ.get("DHAN_ACCESS_TOKEN")

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
MARKET_DATA_PREFIX = "marketdata:"

MARKET_STATUS_PROXY_ID = "1333"
MARKET_OPEN_THRESHOLD_SECONDS = 600

if not supabase_url or not supabase_key or not supabase_anon_key:
    raise RuntimeError("Supabase environment variables are not set. Please check your .env file.")

if not dhan_client_id or not dhan_access_token:
    raise RuntimeError("DhanHQ Client ID or Access Token not set in .env file.")

# Initialize
supabase: Client = create_client(supabase_url, supabase_key)
dhan = dhanhq(dhan_client_id, dhan_access_token)

# Initialize FastAPI
app = FastAPI()

# Initialize Redis
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True
    )
    redis_client.ping()
    logger.info(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"✗ Failed to connect to Redis: {e}")
    logger.warning("API will start but market data features will not work!")
    redis_client = None

# Load instruments CSV
instrument_df = pd.read_csv("instruments.csv")

# Mapping
symbol_to_security_id = {}
security_id_to_symbol = {}
stock_metadata = {}

for index, row in instrument_df.iterrows():
    symbol = row['SEM_TRADING_SYMBOL']
    name = row['SM_SYMBOL_NAME']
    security_id = str(row.get('SEM_SMST_SECURITY_ID', ''))  # Get security_id from CSV

    if security_id:
        symbol_to_security_id[symbol] = security_id
        security_id_to_symbol[security_id] = symbol

    stock_metadata[symbol] = {
        "name": name,
        "security_id": security_id
    }

logger.info(f"Loaded {len(stock_metadata)} stocks from instruments.csv")
logger.info(f"Mapped {len(symbol_to_security_id)} symbols to security IDs")


async def update_closing_price():
    """
    Scheduler endpoint to fetch the latest daily closing price from DhanHQ
    for all unique symbols in open trades and watchlist, and update the
    'last_price' for open trades using bulk OHLC Market Quote API.
    """
    logger.info("Updating Closing Prices...")

    try:
        # Get distinct symbols from trade_cards (open)
        open_trades_query = supabase.table('trade_cards').select('stock_symbol').eq('status', 'open').execute()
        open_trades_symbols = {item['stock_symbol'] for item in open_trades_query.data}
        logger.info(f"Found {len(open_trades_symbols)} unique symbols in open trades.")

        # Get distinct symbols from watchlists
        watchlists_query = supabase.table('watchlists').select('stock_symbol').execute()
        watchlist_symbols = {item['stock_symbol'] for item in watchlists_query.data}
        logger.info(f"Found {len(watchlist_symbols)} unique symbols in watchlists.")

        # Combine into one unique set
        all_symbols = list(open_trades_symbols.union(watchlist_symbols))
        logger.info(f"Total {len(all_symbols)} unique symbols to check.")

        if not all_symbols:
            return {"status": "success", "message": "No symbols to update."}

        # Prepare security IDs and create mapping
        security_ids = []
        sid_to_symbol = {}  # Map security_id back to symbol
        failed_symbols = []
        
        for symbol in all_symbols:
            security_id = symbol_to_security_id.get(symbol)
            if security_id:
                security_ids.append(security_id)
                sid_to_symbol[str(security_id)] = symbol
            else:
                logger.warning(f"No security_id for {symbol}, skipping.")
                failed_symbols.append(symbol)

        if not security_ids:
            return {
                "status": "success",
                "message": "No valid security IDs found.",
                "failed_symbols": failed_symbols
            }

        updated_count = 0
        
        # Process in batches of 1000 (API limit)
        batch_size = 1000
        for i in range(0, len(security_ids), batch_size):
            batch_ids = security_ids[i:i + batch_size]
            
            try:
                # Prepare request body with NSE_EQ exchange
                securities = {
                    "NSE_EQ": [int(sid) for sid in batch_ids]
                }
                
                # Use ohlc_data() method for bulk OHLC fetching
                response = dhan.ohlc_data(securities=securities)
                
                if response and response.get('status') == 'success' and response.get('data'):
                    data = response['data']
                    print(data)
                    
                    # Process NSE_EQ data
                    nse_eq_data = data.get('NSE_EQ', {})
                    
                    for security_id_str, ohlc_info in nse_eq_data.items():
                        symbol = sid_to_symbol.get(security_id_str)
                        if not symbol:
                            continue
                        
                        # Get closing price from OHLC data
                        ohlc = ohlc_info.get('ohlc', {})
                        closing_price = ohlc.get('close')
                        
                        # Fallback to last_price if close is not available
                        if not closing_price:
                            closing_price = ohlc_info.get('last_price')
                        
                    # Log any symbols in batch that didn't return data
                    for sid in batch_ids:
                        if str(sid) not in nse_eq_data:
                            symbol = sid_to_symbol.get(str(sid))
                            if symbol:
                                logger.warning(f"No OHLC data returned for {symbol} (ID: {sid})")
                                failed_symbols.append(symbol)
                else:
                    logger.warning(f"No data returned for batch {i//batch_size + 1}")
                    failed_symbols.extend([sid_to_symbol[str(sid)] for sid in batch_ids if str(sid) in sid_to_symbol])
                
                # Rate limit: 1 request per second
                if i + batch_size < len(security_ids):
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                failed_symbols.extend([sid_to_symbol[str(sid)] for sid in batch_ids if str(sid) in sid_to_symbol])

        logger.info(
            f"Job finished. Updated {updated_count} open trades. Failed to get data for {len(failed_symbols)} symbols.")
        return {
            "status": "success",
            "updated_open_trades": updated_count,
            "total_symbols_processed": len(all_symbols),
            "failed_symbols_count": len(failed_symbols),
            "failed_symbols": failed_symbols
        }

    except Exception as e:
        logger.error(f"Critical error in update_closing_prices: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Scheduler job error: {str(e)}")

async def main():
    await update_closing_price()

if __name__ == "__main__":
     # This is how you run the async main function
     asyncio.run(main())