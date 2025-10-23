import json
import time
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from data import TradeCard, LivePriceRequest, SubscriptionRequest
from services import (
    verify_supabase_jwt,
    get_subscribers_for_advisor,
    send_mock_fcm_notification,
    fetch_all_paginated_data
)
import pandas as pd
import redis
from typing import Optional, Dict
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration and Initialization ---
supabase_url: str = os.environ.get("URL")
supabase_key: str = os.environ.get("SERVICE_KEY")
supabase_anon_key: str = os.environ.get("ANON_KEY")

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

# Initialize
supabase: Client = create_client(supabase_url, supabase_key)

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


# --- Redis Helper Functions

def get_redis_key(security_id: str) -> str:
    """
    Generate Redis key for a security ID
    """
    return f"{MARKET_DATA_PREFIX}{security_id}"


def get_stock_data_from_redis(symbol: str) -> Optional[Dict]:
    """
    Fetch real-time stock data from Redis
    Returns: Dict with ltp, prev_close, or None if not found
    """
    if not redis_client:
        logger.warning("Redis client not available")
        return None

    # Get security_id
    security_id = symbol_to_security_id.get(symbol)
    if not security_id:
        logger.warning(f"No security_id found for symbol: {symbol}")
        return None

    try:
        redis_key = get_redis_key(security_id)
        data = redis_client.hgetall(redis_key)

        if not data:
            logger.debug(f"No Redis data for {symbol} (security_id: {security_id})")
            return None

        # Convert string values to floats
        result = {}
        if 'ltp' in data:
            result['ltp'] = float(data['ltp'])
        if 'prev_close' in data:
            result['prev_close'] = float(data['prev_close'])
        if 'ltp_timestamp' in data:
            result['ltp_timestamp'] = int(data['ltp_timestamp'])
        if 'prev_close_timestamp' in data:
            result['prev_close_timestamp'] = int(data['prev_close_timestamp'])

        return result if result else None

    except Exception as e:
        logger.error(f"Error fetching Redis data for {symbol}: {e}")
        return None


def get_live_price(symbol: str) -> Optional[float]:
    """
    Get current live price (LTP) for a symbol
    """
    data = get_stock_data_from_redis(symbol)
    return data.get('ltp') if data else None


def get_prev_close_price(symbol: str) -> Optional[float]:
    """
    Get previous close price for a symbol
    """
    data = get_stock_data_from_redis(symbol)
    return data.get('prev_close') if data else None


def get_multiple_live_prices(symbols: list) -> Dict[str, float]:
    """
    Efficiently fetch multiple live prices using Redis pipeline
    """
    if not redis_client:
        return {}

    results = {}

    try:
        # Build pipeline with security IDs
        pipeline = redis_client.pipeline()
        symbol_to_key = {}

        for symbol in symbols:
            security_id = symbol_to_security_id.get(symbol)
            if security_id:
                redis_key = get_redis_key(security_id)
                symbol_to_key[symbol] = redis_key
                pipeline.hget(redis_key, 'ltp')

        # Execute pipeline
        responses = pipeline.execute()

        # Map responses back to symbols
        for symbol, ltp in zip(symbol_to_key.keys(), responses):
            if ltp:
                try:
                    results[symbol] = float(ltp)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid LTP value for {symbol}: {ltp}")

        return results

    except Exception as e:
        logger.error(f"Error fetching multiple prices: {e}")
        return {}


def get_stock_full_data(symbol: str) -> Optional[Dict]:
    """
    Get complete stock data including metadata and live prices
    """
    metadata = stock_metadata.get(symbol)
    if not metadata:
        return None

    redis_data = get_stock_data_from_redis(symbol)

    result = {
        "symbol": symbol,
        "name": metadata['name'],
        "security_id": metadata['security_id']
    }

    if redis_data:
        result.update({
            "ltp": redis_data.get('ltp'),
            "prev_close": redis_data.get('prev_close'),
            "entry_price": redis_data.get('prev_close'),
            "last_price": redis_data.get('prev_close'),
            "ltp_timestamp": redis_data.get('ltp_timestamp'),
        })

        # Calculate change percentage
        if result.get('ltp') and result.get('prev_close'):
            change = ((result['ltp'] - result['prev_close']) / result['prev_close']) * 100
            result['change_pct'] = round(change, 2)
            result['change_abs'] = round(result['ltp'] - result['prev_close'], 2)

    return result


# --- API Endpoints

@app.on_event("startup")
async def startup_event():
    """
    Check connections on startup
    """
    logger.info("=" * 60)
    logger.info("Starting FastAPI Backend...")

    # Check Redis
    if redis_client:
        try:
            redis_client.ping()
            logger.info("✓ Redis connection: OK")
        except:
            logger.warning("✗ Redis connection: FAILED")
    else:
        logger.warning("✗ Redis client not initialized")

    logger.info("=" * 60)


@app.get("/")
def root():
    """
    API root with health info
    """
    redis_status = "connected" if redis_client else "disconnected"
    try:
        if redis_client:
            redis_client.ping()
            redis_status = "connected"
    except:
        redis_status = "error"

    return {
        "service": "Trading Platform API",
        "version": "2.0.0",
        "redis_status": redis_status,
        "endpoints": {
            "trading": ["/create-trade", "/check-trade-cards"],
            "market_data": ["/get-live-prices", "/get-all-stocks"],
            "admin": ["/admin/stats", "/admin/dashboard-stats"]
        }
    }


@app.get("/health")
def health_check():
    """
    Health check endpoint
    """
    redis_ok = False
    try:
        if redis_client:
            redis_client.ping()
            redis_ok = True
    except:
        pass

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": redis_ok,
        "supabase": True  # Assume OK if API started
    }


@app.post("/create-trade")
async def create_trade(trade_data: TradeCard, request: Request):
    """
    Endpoint to create a new trade card.
    Uses REAL-TIME prices from Redis.
    """
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Market data connection is down. Trade creation is disabled."
        )

    try:
        market_proxy_key = get_redis_key(MARKET_STATUS_PROXY_ID)

        # Get all data
        market_data = redis_client.hgetall(market_proxy_key)

        if not market_data or 'ltp_timestamp' not in market_data:
            # No data at all, or no timestamp
            logger.warning("Market status check failed (no data or ltp_timestamp). Assuming market is closed.")
            raise HTTPException(
                status_code=503,  # 503 Service Unavailable
                detail="Market is closed. Trade creation is disabled outside of market hours."
            )

        # Check how old the last update is
        last_update_timestamp = int(market_data['ltp_timestamp'])
        current_timestamp = int(time.time())
        seconds_since_last_update = current_timestamp - last_update_timestamp

        if seconds_since_last_update > MARKET_OPEN_THRESHOLD_SECONDS:
            logger.warning(
                f"Market status check failed (LTP timestamp is {seconds_since_last_update}s old). "
                f"Assuming market is closed."
            )
            raise HTTPException(
                status_code=503,
                detail="Market is closed. Trade creation is disabled outside of market hours."
            )

    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error during market check: {e}")
        raise HTTPException(status_code=503, detail="Market data connection is down.")
    except Exception as e:
        logger.error(f"Error checking market status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Could not verify market status: {e}"
        )

    # 1. Authenticate
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header missing or invalid")

    token = auth_header.split(" ")[1]
    user = await verify_supabase_jwt(token, supabase)
    if not user or user.get('role') != 'advisor':
        raise HTTPException(status_code=403, detail="Permission denied. Advisor role required.")

    # Get real-time stock data from Redis
    stock_data = get_stock_full_data(trade_data.stock_symbol)
    if not stock_data:
        raise HTTPException(
            status_code=404,
            detail=f"Stock '{trade_data.stock_symbol}' not found or no live data available."
        )

    # Check if we have live prices
    if not stock_data.get('ltp'):
        raise HTTPException(
            status_code=503,
            detail=f"Live price data not available for {trade_data.stock_symbol}. Market may be closed or data feed inactive."
        )

    # Prepare trade data
    new_trade = {
        "advisor_id": user.get('id'),
        "stock_symbol": trade_data.stock_symbol,
        "stock_name": stock_data['name'],
        "entry_price": stock_data.get('ltp'),  # Current LTP as entry (or could use prev_close)
        "last_price": stock_data.get('prev_close'),  # ✅ FIXED: Use prev_close for reference
        "stop_loss": trade_data.stop_loss,
        "target": trade_data.target,
        "status": "open",
        "trade_direction": trade_data.trade_direction,
        "trade_category": trade_data.trade_category,
        "trade_segment": trade_data.trade_segment,
        "entry_zone": trade_data.entry_zone,
        "exit_zone": trade_data.exit_zone,
    }

    # Insert into database
    try:
        response = supabase.table('trade_cards').insert(new_trade).execute()
        if response.data:
            return {
                "status": "success",
                "message": "Trade card created successfully with live prices.",
                "data": response.data[0]
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create trade card in database.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# Notification Endpoint
@app.post("/notify-users")
async def handle_notification_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint triggered by Supabase webhook on trade inserts/updates.
    """
    payload = await request.json()

    if payload.get('type') in ['INSERT', 'UPDATE'] and payload.get('table') == 'trade_cards':
        record = payload.get('record')
        background_tasks.add_task(process_and_send_notifications, record, payload.get('type'))
        return {"status": "success", "message": "Notification task queued."}

    return {"status": "ignored", "message": "Webhook event was not a relevant trade card event."}


# Notification Endpoint
async def process_and_send_notifications(trade_record: dict, event_type: str):
    """
    Send notifications based on trade events.
    """
    advisor_id = trade_record.get('advisor_id')
    stock_symbol = trade_record.get('stock_symbol')

    if not advisor_id or not stock_symbol:
        logger.error("Missing advisor_id or stock_symbol in webhook payload.")
        return

    fcm_tokens = await get_subscribers_for_advisor(advisor_id, supabase)

    if fcm_tokens:
        message_title = ""
        message_body = ""

        if event_type == 'INSERT':
            message_title = "New Trade Signal!"
            message_body = f"A new trade for {stock_symbol} has been posted by your advisor."
        elif event_type == 'UPDATE':
            status = trade_record.get('status')
            if status == 'closed_profit':
                message_title = "Trade Closed: Profit!"
                message_body = f"The trade for {stock_symbol} has hit its target."
            elif status == 'closed_loss':
                message_title = "Trade Closed: Stop Loss"
                message_body = f"The trade for {stock_symbol} has hit its stop loss."
            elif status == 'closed_cost':
                message_title = "Trade Closed at Cost"
                message_body = f"The trade for {stock_symbol} has been closed at its entry price."
            else:
                return
        else:
            return

        await send_mock_fcm_notification(fcm_tokens, message_title, message_body)
        logger.info(f"Sent notifications to {len(fcm_tokens)} subscribers for {stock_symbol}")
    else:
        logger.info(f"No subscribers found for advisor {advisor_id}")


# Scheduler Endpoint
@app.get("/check-trade-cards")
async def check_trade_cards():
    """
    Scheduler endpoint: Check open trades against REAL-TIME prices from Redis.
    Closes trades when target/stop-loss is hit.
    """
    try:
        # Fetch all open trades
        query = supabase.table('trade_cards').select("*").eq('status', 'open')
        open_trades = fetch_all_paginated_data(query)

        if not open_trades:
            return {"status": "success", "message": "No open trades to check."}

        # Get unique symbols
        symbols = list(set([trade['stock_symbol'] for trade in open_trades]))

        # Fetch all live prices efficiently
        live_prices = get_multiple_live_prices(symbols)

        if not live_prices:
            logger.warning("No live prices available from Redis. Market may be closed.")
            return {"status": "warning", "message": "No live prices available."}

        closed_trades_count = 0

        # Check each trade
        for trade in open_trades:
            symbol = trade['stock_symbol']
            live_price = live_prices.get(symbol)

            if not live_price:
                logger.debug(f"No live price for {symbol}, skipping")
                continue

            new_status = None

            if trade['trade_direction'] == 'Buy':
                if live_price >= trade['target']:
                    new_status = 'closed_profit'
                elif live_price <= trade['stop_loss']:
                    new_status = 'closed_loss'
            elif trade['trade_direction'] == 'Sell':
                if live_price <= trade['target']:
                    new_status = 'closed_profit'
                elif live_price >= trade['stop_loss']:
                    new_status = 'closed_loss'

            # Update if condition met
            if new_status:
                supabase.table('trade_cards').update({
                    'status': new_status,
                    'closed_at': 'now()',
                    'last_price': live_price  # Update with closing price
                }).eq('id', trade['id']).execute()

                closed_trades_count += 1
                logger.info(f"Closed trade {trade['id']} for {symbol} at ₹{live_price} - Status: {new_status}")

        return {
            "status": "success",
            "message": f"Checked {len(open_trades)} trades. Closed {closed_trades_count}.",
            "prices_checked": len(live_prices)
        }

    except Exception as e:
        logger.error(f"Error in check_trade_cards: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Trade check error: {str(e)}")


# Scheduler Endpoint
@app.get("/update-last-price")
async def update_last_price():
    """
    Scheduler endpoint: Update last_price for all open trades using REAL-TIME data.
    Run this once daily after market close.
    """
    try:
        query = supabase.table('trade_cards').select("id, stock_symbol").eq('status', 'open')
        open_trades = fetch_all_paginated_data(query)

        if not open_trades:
            return {"status": "success", "message": "No open trades to update."}

        # Get unique symbols
        symbols = list(set([trade['stock_symbol'] for trade in open_trades]))

        # Fetch closing prices (use prev_close as it's set after market close)
        updated_count = 0

        for trade in open_trades:
            symbol = trade['stock_symbol']

            # Get latest prev_close (which becomes the closing price)
            data = get_stock_data_from_redis(symbol)

            if data and data.get('prev_close'):
                new_close_price = data['prev_close']

                supabase.table('trade_cards').update({
                    'last_price': new_close_price
                }).eq('id', trade['id']).execute()

                updated_count += 1
                logger.info(f"Updated last_price for {symbol} to ₹{new_close_price}")

        return {
            "status": "success",
            "message": f"Updated closing prices for {updated_count}/{len(open_trades)} trades."
        }

    except Exception as e:
        logger.error(f"Error in update_last_price: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Price update error: {str(e)}")


@app.post("/get-live-prices")
async def get_live_prices(request_data: LivePriceRequest):
    """
    Fetch REAL-TIME live prices from Redis for multiple symbols.
    Called by Flutter app periodically.
    """
    live_prices = get_multiple_live_prices(request_data.symbols)

    if not live_prices:
        raise HTTPException(
            status_code=503,
            detail="Live prices not available. Market may be closed or data feed inactive."
        )

    return {"status": "success", "data": live_prices}


@app.get("/get-all-stocks")
async def get_all_stocks():
    """
    Fetch complete stock list with REAL-TIME prices from Redis.
    Used for search screen in Flutter app.
    """
    all_stocks_data = {}

    # Get all symbols
    all_symbols = list(stock_metadata.keys())

    # Fetch live prices for all
    live_prices_batch = get_multiple_live_prices(all_symbols[:500])  # Limit to 500 for performance

    for symbol, metadata in stock_metadata.items():
        stock_data = {
            "name": metadata['name'],
            "security_id": metadata.get('security_id', '')
        }

        # Try to get live data
        redis_data = get_stock_data_from_redis(symbol)

        if redis_data:
            stock_data["entry_price"] = redis_data.get('prev_close', 0.0)
            stock_data["last_price"] = redis_data.get('prev_close', 0.0)
        else:
            stock_data["entry_price"] = 0.0
            stock_data["last_price"] = 0.0

        all_stocks_data[symbol] = stock_data

    return {
        "status": "success",
        "data": all_stocks_data,
        "total_stocks": len(all_stocks_data),
        "live_data_count": len(live_prices_batch)
    }


@app.post("/subscribe-instrument")
async def subscribe_instrument(req: SubscriptionRequest, request: Request):
    """
    Endpoint for Flutter app to request a live data subscription.
    This publishes a message to the dhan_worker via Redis Pub/Sub.
    """

    # Find Security ID
    security_id = symbol_to_security_id.get(req.symbol)
    if not security_id:
        raise HTTPException(
            status_code=404,
            detail=f"Security ID not found for symbol: {req.symbol}"
        )

    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis client not available."
        )

    # Create Pub/Sub Message
    message = {
        "action": req.action,
        "security_id": security_id
    }

    try:
        # Publish to the worker
        channel = "signaldesk:subscriptions"
        redis_client.publish(channel, json.dumps(message))

        logger.info(f"Published {req.action} request for {req.symbol} ({security_id}) to {channel}")

        return {
            "status": "success",
            "message": f"Subscription request for {req.symbol} queued."
        }

    except Exception as e:
        logger.error(f"Failed to publish subscription request: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue subscription.")


@app.get("/get-stock-data/{symbol}")
async def get_stock_data(symbol: str):
    """
    Fetch complete stock data including live prices and prev_close from Redis.
    Used by Flutter app for detailed stock information.
    """
    stock_data = get_stock_full_data(symbol)

    if not stock_data:
        raise HTTPException(
            status_code=404,
            detail=f"Stock '{symbol}' not found or no data available."
        )

    return {
        "status": "success",
        "data": stock_data
    }


@app.get("/check-market-status")
async def check_market_status():
    """
    A simple endpoint to check if the market is open.
    It checks the ltp_timestamp of a proxy stock.
    """
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Market data connection is down."
        )

    try:
        market_proxy_key = get_redis_key(MARKET_STATUS_PROXY_ID)
        market_data = redis_client.hgetall(market_proxy_key)

        if not market_data or 'ltp_timestamp' not in market_data:
            logger.warning("Market status check failed (no data or ltp_timestamp).")
            raise HTTPException(
                status_code=503,
                detail="Market is closed."
            )

        # Check how old the last update is
        last_update_timestamp = int(market_data['ltp_timestamp'])
        current_timestamp = int(time.time())
        seconds_since_last_update = current_timestamp - last_update_timestamp

        if seconds_since_last_update > MARKET_OPEN_THRESHOLD_SECONDS:
            logger.warning(
                f"Market status check failed (LTP timestamp is {seconds_since_last_update}s old)."
            )
            raise HTTPException(
                status_code=503,
                detail="Market is closed."
            )

        # If we passed all checks, the market is open
        return {"status": "open", "message": "Market is open."}

    except redis.exceptions.ConnectionError:
        logger.error("Redis connection error during market check.")
        raise HTTPException(status_code=503, detail="Market data connection is down.")
    except Exception as e:
        logger.error(f"Error checking market status: {e}")
        raise HTTPException(status_code=500, detail="Could not verify market status.")


# Statistical Endpoint
@app.get("/admin/stats")
async def get_admin_stats(request: Request):
    """
    Admin dashboard stats.
    Requires admin JWT.
    """
    auth_header = request.headers.get('Authorization')
    token = auth_header.split(" ")[1]
    user = await verify_supabase_jwt(token, supabase)
    if not user or user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Permission denied. Admin role required.")

    try:
        total_users = supabase.table('profiles').select('id', count='exact').execute().count
        total_advisors = supabase.table('profiles').select('id', count='exact').eq('role', 'advisor').execute().count
        total_trades = supabase.table('trade_cards').select('id', count='exact').execute().count

        return {
            "status": "success",
            "data": {
                "total_users": total_users,
                "total_advisors": total_advisors,
                "total_trades": total_trades,
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Statistical Endpoint
@app.get("/admin/dashboard-stats")
async def get_dashboard_stats(request: Request):
    """
    Fetches pre-aggregated statistics for admin panel.
    """
    auth_header = request.headers.get('Authorization')
    token = auth_header.split(" ")[1]
    user = await verify_supabase_jwt(token, supabase)
    if not user or user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Permission denied. Admin role required.")

    try:
        stats_res = supabase.table('platform_statistics').select('*').eq('id', True).single().execute()
        stats = stats_res.data

        if not stats:
            raise HTTPException(status_code=404, detail="Platform statistics not found.")

        return {
            "status": "success",
            "data": {
                "user_and_advisor_growth": {
                    "total_users": stats['total_users'],
                    "total_advisors": stats['total_advisors'],
                    "new_users_last_30d": stats['new_users_last_30d'],
                    "new_advisors_last_30d": stats['new_advisors_last_30d'],
                    "total_subscriptions": stats['total_subscriptions'],
                    "user_registrations_last_7d": stats['user_registrations_last_7d'],
                    "subscription_activity_last_7d": stats['subscription_activity_last_7d']
                },
                "trade_and_performance": {
                    "total_trades": stats['total_trades'],
                    "trade_status_breakdown": {
                        "open": stats['open_trades'],
                        "profit": stats['closed_trades_profit'],
                        "loss": stats['closed_trades_loss'],
                        "cost": stats['closed_trades_cost']
                    },
                    "top_performing_advisors": stats['top_performing_advisors'],
                    "most_traded_stocks": stats['most_traded_stocks']
                },
                "platform_activity": {
                    "trades_created_last_7d": stats['trades_created_last_7d']
                }
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
