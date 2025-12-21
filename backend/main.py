import asyncio
import json
import time
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from data import TradeCard, LivePriceRequest, SubscriptionRequest, BulkSubscribeRequest
from services import (
    verify_supabase_jwt,
    get_subscribers_for_advisor,
    send_mock_fcm_notification,
    fetch_all_paginated_data
)
import pandas as pd
import redis, redis.asyncio as aioredis
from typing import Optional, Dict, Set, List
import logging
import zoneinfo
import datetime
from dhanhq import dhanhq

IST = zoneinfo.ZoneInfo("Asia/Kolkata")
MARKET_OPEN_TIME = datetime.time(9, 15)
MARKET_CLOSE_TIME = datetime.time(15, 30)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration and Initialization ---
supabase_url: str = os.environ.get("URL")
supabase_key: str = os.environ.get("SERVICE_KEY")
supabase_anon_key: str = os.environ.get("ANON_KEY")

# Dhan Configuration
dhan_client_id: str = os.environ.get("DHAN_CLIENT_ID")
dhan_access_token: str = os.environ.get("DHAN_ACCESS_TOKEN")

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_DATA_TTL = int(os.environ.get("REDIS_DATA_TTL", 345600))
MARKET_DATA_PREFIX = "marketdata:"
MARKET_DATA_UPDATES_CHANNEL = f"{MARKET_DATA_PREFIX}updates"

MARKET_STATUS_PROXY_ID = "1333"
MARKET_OPEN_THRESHOLD_SECONDS = 600

if not supabase_url or not supabase_key or not supabase_anon_key:
    raise RuntimeError("Supabase environment variables are not set. Please check your .env file.")

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

# Initialize async redis client
try:
    # Note: This client is used only for the async WebSocket endpoint
    async_redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    logger.info(f"✓ Initialized Async Redis client for WebSockets")
except Exception as e:
    logger.error(f"✗ Failed to initialize Async Redis client: {e}")
    async_redis_client = None

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
        else:
            # --- ADDED THIS ---
            result['change_pct'] = 0.0
            result['change_abs'] = 0.0

    else:
        # No live data, return a default structure
        result.update({
            "ltp": None,
            "prev_close": 0.0,
            "entry_price": 0.0,
            "last_price": 0.0,
            "ltp_timestamp": None,
            "change_pct": 0.0,
            "change_abs": 0.0
        })

    return result


active_subscriptions: Dict[WebSocket, Set[str]] = {}
symbol_to_clients: Dict[str, Set[WebSocket]] = {}


def _publish_to_worker(action: str, symbol: str):
    """Helper to tell dhan_worker to subscribe/unsubscribe via Redis."""
    security_id = symbol_to_security_id.get(symbol)
    if not security_id or not redis_client:
        logger.warning(f"WS: Cannot publish {action} for {symbol}: No security_id or redis_client")
        return

    message = {"action": action, "security_id": security_id}
    try:
        redis_client.publish("signaldesk:subscriptions", json.dumps(message))
        logger.info(f"WS: Published {action} for {symbol} ({security_id}) to worker")
    except Exception as e:
        logger.error(f"WS: Failed to publish to worker: {e}")


async def _subscribe_client(ws: WebSocket, symbols: List[str]):
    """Subscribe a client to a list of symbols."""
    if ws not in active_subscriptions:
        active_subscriptions[ws] = set()

    new_subscriptions = 0
    for symbol in symbols:
        if symbol not in symbol_to_security_id:
            logger.warning(f"WS: Unknown symbol {symbol} in subscribe request")
            continue

        # Add to client's subscription set
        active_subscriptions[ws].add(symbol)

        # Add client to symbol's listener set
        if symbol not in symbol_to_clients:
            symbol_to_clients[symbol] = set()

        # Only publish to worker if this is the FIRST client for this symbol
        if not symbol_to_clients[symbol]:
            _publish_to_worker("add", symbol)

        if ws not in symbol_to_clients[symbol]:
            symbol_to_clients[symbol].add(ws)
            new_subscriptions += 1

    logger.info(f"WS: Client {ws.client.host} subscribed to {new_subscriptions} new symbols.")


async def _unsubscribe_client(ws: WebSocket, symbols: List[str]):
    """Unsubscribe a client from a list of symbols."""
    if ws not in active_subscriptions:
        return

    for symbol in symbols:
        active_subscriptions[ws].discard(symbol)

        if symbol in symbol_to_clients:
            symbol_to_clients[symbol].discard(ws)

            # If this was the LAST client for this symbol, tell worker to unsubscribe
            if not symbol_to_clients[symbol]:
                logger.info(f"WS: Last client for {symbol}. Telling worker to remove.")
                _publish_to_worker("remove", symbol)
                del symbol_to_clients[symbol]  # Clean up


async def _disconnect_client(ws: WebSocket):
    """Handle client disconnection."""
    if ws in active_subscriptions:
        symbols_to_remove = list(active_subscriptions.get(ws, set()))
        await _unsubscribe_client(ws, symbols_to_remove)
        del active_subscriptions[ws]
        logger.info(f"WS: Client {ws.client.host} disconnected. Cleaned up {len(symbols_to_remove)} subscriptions.")


# --- Redis Pub/Sub Listener for WebSocket ---

async def redis_websocket_listener():
    """
    Listens to Redis Pub/Sub 'marketdata:updates' and forwards to WebSockets.
    """
    if not async_redis_client:
        logger.error("WS Listener: Async Redis client not available. Cannot start listener.")
        return

    logger.info("WS Listener: Starting...")
    while True:
        try:
            pubsub = async_redis_client.pubsub()
            await pubsub.subscribe(MARKET_DATA_UPDATES_CHANNEL)
            logger.info(f"WS Listener: Subscribed to Redis channel '{MARKET_DATA_UPDATES_CHANNEL}'")

            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None:
                    await asyncio.sleep(0.01)  # Prevent busy-looping
                    continue

                if message.get("type") == "message":
                    try:
                        data = json.loads(message["data"])
                        security_id = data.get("security_id")
                        update_data = data.get("data")  # This is the {'ltp': 123, ...}

                        if not security_id or not update_data:
                            continue

                        symbol = security_id_to_symbol.get(str(security_id))
                        if not symbol or symbol not in symbol_to_clients:
                            continue

                        clients_to_notify = list(symbol_to_clients[symbol])  # Copy set for safe iteration

                        # Format the payload for the client
                        payload = {"symbol": symbol}
                        if 'ltp' in update_data:
                            payload['ltp'] = update_data['ltp']
                        if 'prev_close' in update_data:
                            payload['prev_close'] = update_data['prev_close']

                        tasks = [client.send_json(payload) for client in clients_to_notify if
                                 client in active_subscriptions]
                        if tasks:
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            for res, client in zip(results, clients_to_notify):
                                if isinstance(res, Exception):
                                    logger.warning(
                                        f"WS Listener: Failed to send to {client.client.host}: {res}. Removing.")
                                    await _disconnect_client(client)

                    except Exception as e:
                        logger.error(f"WS Listener: Error processing message: {e}")

        except (aioredis.exceptions.ConnectionError, asyncio.TimeoutError) as e:
            logger.error(f"WS Listener: Connection error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.critical(f"WS Listener: CRITICAL error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)


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

    logger.info("Starting WebSocket Redis listener task...")
    asyncio.create_task(redis_websocket_listener())

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


@app.websocket("/ws/prices")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"WS: Client connected from {websocket.client.host}")
    active_subscriptions[websocket] = set()  # Register client

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            symbols = data.get("symbols", [])

            if not isinstance(symbols, list):
                symbols = [symbols]  # Handle single symbol case

            if action == "subscribe" or action == "bulk_subscribe":
                await _subscribe_client(websocket, symbols)
                await websocket.send_json({"status": "success", "message": f"Subscribed to {len(symbols)} symbols"})

            elif action == "unsubscribe":
                await _unsubscribe_client(websocket, symbols)
                await websocket.send_json({"status": "success", "message": f"Unsubscribed from {len(symbols)} symbols"})

    except WebSocketDisconnect:
        logger.info(f"WS: Client {websocket.client.host} disconnected (WebSocketDisconnect).")
    except Exception as e:
        logger.error(f"WS: Error in websocket_endpoint: {e}")
    finally:
        await _disconnect_client(websocket)


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
    Fetch complete stock list with data primarily from Redis.
    """
    if not redis_client:
        logger.warning("get_all_stocks: Redis client not available")
        raise HTTPException(status_code=503, detail="Market data connection is down.")

    all_stocks_data = {}

    redis_batch_data = {}  # To store results from pipeline
    processed_count = 0  # To count stocks with Redis data

    logger.info("get_all_stocks: Starting fetch...")

    try:
        # Prepare pipeline for HGETALL
        pipeline = redis_client.pipeline()
        security_id_to_symbol = {}

        logger.info(f"get_all_stocks: Preparing pipeline for {len(stock_metadata)} stocks...")
        for symbol, metadata in stock_metadata.items():
            security_id = metadata.get('security_id')
            if security_id:
                redis_key = get_redis_key(security_id)
                pipeline.hgetall(redis_key)
                security_id_to_symbol[security_id] = symbol  # Store mapping

        # Execute the pipeline to get all hash data at once
        logger.info("get_all_stocks: Executing Redis pipeline...")
        pipeline_results = pipeline.execute()
        logger.info(f"get_all_stocks: Pipeline execution complete. Received {len(pipeline_results)} results.")

        # Process pipeline results into a usable dictionary
        expected_keys = list(security_id_to_symbol.keys())  # Order should match pipeline queue order

        if len(pipeline_results) != len(expected_keys):
            logger.warning(
                f"Pipeline result count ({len(pipeline_results)}) doesn't match expected key count ({len(expected_keys)}).")

        # Iterate using the order of added keys to the pipeline
        for i, data in enumerate(pipeline_results):
            if i < len(expected_keys):  # Safety check
                security_id = expected_keys[i]
                if data:  # If Redis returned data for this key
                    redis_batch_data[security_id] = data  # Store the hash data
                    processed_count += 1
            else:
                logger.warning(f"Received more pipeline results ({i + 1}) than expected keys ({len(expected_keys)}).")
                break

        logger.info(f"get_all_stocks: Processed Redis data for {processed_count} security IDs.")

    except redis.exceptions.ConnectionError as e:
        logger.error(f"get_all_stocks: Redis connection error during pipeline: {e}")
        raise HTTPException(status_code=503, detail="Market data connection error.")
    except Exception as e:
        logger.error(f"get_all_stocks: Error during Redis pipeline execution: {e}")
        raise HTTPException(status_code=500, detail="Error fetching market data.")

    logger.info("get_all_stocks: Building final response...")  # Added log
    # Loop through metadata as before
    for symbol, metadata in stock_metadata.items():
        stock_data = {
            "name": metadata['name'],
            "security_id": metadata.get('security_id', '')
        }

        security_id = metadata.get('security_id')
        redis_data = redis_batch_data.get(security_id)  # Get data from batch dictionary

        prev_close = 0.0
        if redis_data and 'prev_close' in redis_data:
            try:
                prev_close = float(redis_data['prev_close'])
            except (ValueError, TypeError):
                logger.warning(f"Invalid prev_close '{redis_data['prev_close']}' for {symbol}, using 0.0")

        stock_data["entry_price"] = prev_close
        stock_data["last_price"] = prev_close

        all_stocks_data[symbol] = stock_data

    logger.info(f"get_all_stocks: Finished building response for {len(all_stocks_data)} stocks.")  # Added log

    return {
        "status": "success",
        "data": all_stocks_data,
        "total_stocks": len(all_stocks_data),
        "live_data_count": processed_count
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


@app.post("/backfill-subscribe")
async def backfill_subscribe(req: SubscriptionRequest, request: Request):
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

    logger.info(f"Market closed. Backfilling historical data for {req.symbol}...")

    ltp = 0.0
    close = 0.0

    try:
        today = datetime.date.today()
        from_date = str(today - datetime.timedelta(days=7))
        to_date = str(today)

        # --- Call both APIs ---
        response_daily = dhan.historical_daily_data(
            security_id=str(security_id),
            exchange_segment='NSE_EQ',
            instrument_type='EQUITY',
            from_date=from_date,
            to_date=to_date
        )

        response_minute = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment='NSE_EQ',
            instrument_type='EQUITY',
            from_date=from_date,
            to_date=to_date,
            interval=60
        )

        # Get Prev_Close from daily data (this is more reliable)
        if response_daily and response_daily.get('status') == 'success' and response_daily.get('data'):
            try:
                close = float(response_daily['data']['close'][-1])
            except (IndexError, KeyError, TypeError, ValueError) as e:
                logger.warning(f"Could not parse Prev_Close from daily data for {req.symbol}: {e}")
                close = 0.0

        # Get LTP (Last Traded Price) from minute data if available
        if response_minute and response_minute.get('status') == 'success' and response_minute.get('data'):
            try:
                ltp = float(response_minute['data']['close'][-1])
            except (IndexError, KeyError, TypeError, ValueError) as e:
                logger.warning(f"Could not parse LTP from minute data for {req.symbol}: {e}")
                ltp = 0.0

        # Fallback Logic
        # If we couldn't get LTP (e.g., weekend), use the latest daily close as a fallback
        if ltp == 0.0 and close != 0.0:
            logger.info(f"Using daily close ({close}) as LTP fallback for {req.symbol}")
            ltp = close

        elif close == 0.0 and ltp != 0.0:
            logger.warning(f"Using LTP ({ltp}) as Prev_Close fallback for {req.symbol}")
            close = ltp

        if ltp == 0.0 and close == 0.0:
            logger.error(f"Failed to get *any* price data for {req.symbol}. Writing 0.0.")

        # Write to Redis
        redis_key = get_redis_key(security_id)
        pipe = redis_client.pipeline()
        current_timestamp = int(time.time())

        data_to_set = {
            "prev_close": close,
            "ltp": ltp,
            "prev_close_timestamp": current_timestamp,
            "ltp_timestamp": current_timestamp
        }

        pipe.hset(redis_key, mapping=data_to_set)
        pipe.expire(redis_key, REDIS_DATA_TTL)
        pipe.execute()
        logger.info(f"✓ Backfilled {req.symbol} (LTP: {ltp}, Prev_Close: {close})")

    except Exception as e:
        logger.error(f"CRITICAL: Failed to backfill data for {req.symbol}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to backfill market data for {req.symbol}: {str(e)}"
        )

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


@app.post("/bulk-subscribe")
async def bulk_subscribe_instruments(request: BulkSubscribeRequest):
    """
    Endpoint for Flutter app to request multiple live data subscriptions.
    Publishes ONE message with a LIST of security IDs.
    """
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis client not available. Cannot process bulk subscriptions."
        )

    successful_symbols = []
    failed_symbols = []
    security_ids_to_process = []
    channel = "signaldesk:subscriptions"  # The channel worker listens to

    logger.info(f"Received bulk request: Action='{request.action}', Symbols={request.symbols}")

    for symbol in request.symbols:
        security_id = symbol_to_security_id.get(symbol)
        if not security_id:
            logger.warning(f"Security ID not found for symbol: {symbol} during bulk subscribe. Skipping.")
            failed_symbols.append({"symbol": symbol, "reason": "Security ID not found"})
            continue

        security_ids_to_process.append(security_id)
        successful_symbols.append(symbol)

    if not security_ids_to_process:
        logger.warning("Bulk request had no valid symbols to process.")
        return {
            "status": "failed",
            "message": "No valid symbols found to process.",
            "failed": failed_symbols
        }

    # Create one message with a LIST of security IDs
    message = {
        "action": request.action,
        "security_ids": security_ids_to_process  # <-- PLURAL KEY
    }

    try:
        redis_client.publish(channel, json.dumps(message))
        logger.info(f"Published '{request.action}' request for {len(security_ids_to_process)} instruments to {channel}")

    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error publishing bulk request: {e}")
        raise HTTPException(status_code=503, detail="Redis connection failed during bulk publish.")
    except Exception as e:
        logger.error(f"Failed to publish bulk subscription request for {symbol}: {e}")
        failed_symbols.append({"symbol": "N/A (Batch)", "reason": str(e)})

    # Construct the response based on results
    if not failed_symbols:
        return {
            "status": "success",
            "message": f"Successfully queued {request.action} requests for {len(successful_symbols)} instruments."
        }
    else:
        # Partial success (some symbols failed mapping)
        return {
            "status": "partial_success",
            "message": f"Queued {request.action} for {len(successful_symbols)} instruments. Failed to find {len(failed_symbols)}.",
            "successful": successful_symbols,
            "failed": failed_symbols
        }


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
    now_ist = datetime.datetime.now(IST)
    current_time = now_ist.time()
    current_day = now_ist.weekday()

    is_weekday = current_day < 5

    # Check if current time is within market hours
    is_within_hours = MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME

    if not (is_weekday and is_within_hours):
        logger.info(
            f"Market status check failed (Time-based): Day={current_day}, Time={current_time}. Market is closed.")
        raise HTTPException(
            status_code=503,
            detail="Market is closed (outside trading hours/days)."
        )

    # If inside market hours then check redis database which accounts for holidays too
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
                detail="Market is closed (Checked Redis database)"
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


@app.get("/get-instrument-list")
async def get_instrument_list():
    """
    Fetch the complete static list of instruments (symbol, name, security_id).
    This endpoint is lightweight and does NOT query Redis for live prices.
    It's designed for client-side caching (e.g., search screen).
    """
    if not stock_metadata:
        raise HTTPException(
            status_code=503,
            detail="Instrument list is not loaded."
        )

    return {
        "status": "success",
        "data": stock_metadata,
        "total_stocks": len(stock_metadata)
    }


# Approving Advisors
@app.get("/admin/pending-advisors")
async def get_pending_advisors(request: Request):
    """Fetches advisors who are not yet verified."""
    auth_header = request.headers.get('Authorization')
    token = auth_header.split(" ")[1]
    user = await verify_supabase_jwt(token, supabase)

    if not user or user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        # Join profiles with advisor_details to get license info
        response = supabase.table('profiles').select(
            "id, email, first_name, last_name, advisor_details(license_type, registration_number)"
        ).eq('role', 'advisor').eq('is_verified', False).execute()

        return {"status": "success", "data": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/approve-advisor/{advisor_id}")
async def approve_advisor(advisor_id: str, request: Request):
    """Sets is_verified to true for a specific advisor."""
    auth_header = request.headers.get('Authorization')
    token = auth_header.split(" ")[1]
    user = await verify_supabase_jwt(token, supabase)

    if not user or user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        response = supabase.table('profiles').update(
            {"is_verified": True}
        ).eq('id', advisor_id).execute()

        return {"status": "success", "message": "Advisor approved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))