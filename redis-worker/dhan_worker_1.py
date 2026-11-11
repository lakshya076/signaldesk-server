import json
import os
import threading
import time
import logging
import asyncio
import redis
import pandas as pd
from typing import Dict, List, Set, Optional
from dhanhq import marketfeed
from dotenv import load_dotenv

load_dotenv()

DHAN_CLIENT_ID: str = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN: str = os.environ.get("DHAN_ACCESS_TOKEN")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_DATA_TTL = int(os.environ.get("REDIS_DATA_TTL", 345600))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise RuntimeError("DhanHQ Client ID or Access Token not set.")

# Set up logging for this specific worker
logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER-1 - %(levelname)s - %(message)s')

# --- Redis Key Prefix ---
MARKET_DATA_PREFIX = "marketdata:"


class DhanRedisWorker:
    def __init__(self, client_id: str, access_token: str, redis_host: str, redis_port: int, 
                 redis_password: str = None, redis_db: int = 0, data_ttl: int = 86400):
        self._client_id = client_id
        self._access_token = access_token
        self._feed_client = None
        self._data_ttl = data_ttl

        # Redis connection
        try:
            self._redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30
            )
            self._redis_client.ping()
            logging.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except redis.exceptions.ConnectionError as e:
            logging.error(f"Failed to connect to Redis: {e}")
            raise
        
        self._lock = threading.Lock()
        self._subscriptions: Set[str] = set()

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._loop = None  # To store the event loop

        # Statistics
        self._stats = {
            'total_updates': 0,
            'redis_writes': 0,
            'redis_errors': 0,
            'connection_attempts': 0,
            'last_update_time': None
        }

    def _get_redis_key(self, security_id: str) -> str:
        """Generates the Redis key for a given security ID."""
        return f"{MARKET_DATA_PREFIX}{security_id}"

    def _process_data(self, data: Dict):
        """Extract LTP/prev_close and write to Redis."""
        try:
            sec_id = str(data.get('security_id', ''))

            redis_key = self._get_redis_key(sec_id)
            update_data = {}
            current_timestamp = int(time.time())

            if data.get('type') == 'Ticker Data' and 'LTP' in data:
                try:
                    new_ltp = float(data['LTP'])
                    update_data['ltp'] = new_ltp
                    update_data['ltp_timestamp'] = current_timestamp
                except (ValueError, TypeError):
                    logging.warning(f"Invalid LTP format for {sec_id}: {data['LTP']}")

            elif data.get('type') == 'Previous Close' and 'prev_close' in data:
                try:
                    new_prev_close = float(data['prev_close'])
                    update_data['prev_close'] = new_prev_close
                    update_data['prev_close_timestamp'] = current_timestamp
                except (ValueError, TypeError):
                    logging.warning(f"Invalid prev_close format for {sec_id}: {data['prev_close']}")

            if update_data:
                try:
                    # Use pipeline for atomic operation
                    pipe = self._redis_client.pipeline()
                    pipe.hset(redis_key, mapping=update_data)
                    pipe.expire(redis_key, self._data_ttl)  # Set TTL
                    pipe.publish(f"{MARKET_DATA_PREFIX}updates", json.dumps({"security_id": sec_id, "data": update_data, "ts": time.time()}))
                    pipe.execute()
                    
                    self._stats['redis_writes'] += 1
                    self._stats['total_updates'] += 1
                    self._stats['last_update_time'] = time.time()
                    
                except redis.exceptions.ConnectionError:
                    logging.error(f"Redis connection lost while writing {sec_id}")
                    self._stats['redis_errors'] += 1
                except redis.exceptions.RedisError as e:
                    logging.error(f"Redis write error for {sec_id}: {e}")
                    self._stats['redis_errors'] += 1
            else:
                logging.info(f"No update_data generated for {sec_id}. Data type: {data.get('type')}")

        except Exception as e:
            logging.warning(f"Error processing data: {e} - Data: {data}")

    def _worker(self):
        """Main worker thread loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        logging.info("Worker thread started.")

        try:
            while not self._stop_event.is_set():
                current_instruments = []
                try:
                    with self._lock:
                        current_instruments = [(marketfeed.NSE, sec_id, marketfeed.Ticker)
                                               for sec_id in self._subscriptions]

                    if not current_instruments:
                        logging.warning("No instruments loaded. Worker is idle. Retrying in 5s...")
                        if self._stop_event.wait(timeout=5):
                            break
                        continue

                    self._stats['connection_attempts'] += 1
                    logging.info(f"Attempting connection for {len(current_instruments)} instruments...")
                    
                    self._feed_client = marketfeed.DhanFeed(
                        self._client_id, self._access_token, current_instruments, "v2"
                    )
                    logging.info(f"✓ Connected successfully with {len(current_instruments)} instruments")

                    # Inner loop to process data
                    while not self._stop_event.is_set():
                        self._feed_client.run_forever()
                        response = self._feed_client.get_data()
                        if response:
                            self._process_data(response)
                        time.sleep(0.001)

                    if self._stop_event.is_set():
                        logging.info("Stop signaled. Disconnecting...")

                except ConnectionRefusedError:
                    logging.error("Connection refused. Retrying in 30s...")
                    if self._stop_event.wait(30): break
                except Exception as e:
                    if not self._stop_event.is_set():
                        logging.error(f"Worker loop error: {e}. Retrying in 10s...")
                        if self._stop_event.wait(10): break
                finally:
                    if self._feed_client:
                        try:
                            loop.run_until_complete(self._feed_client.disconnect())
                            logging.info("Disconnected feed client.")
                        except Exception as e:
                            logging.warning(f"Error during disconnect: {e}")
                        self._feed_client = None

        finally:
            logging.info("Cleaning up worker thread asyncio loop...")
            if self._loop:
                try:
                    loop = self._loop
                    pending = asyncio.all_tasks(loop)
                    if pending:
                        for task in pending: task.cancel()
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()
                    logging.info("Event loop closed.")
                except Exception as e:
                    logging.warning(f"Error closing loop: {e}")
            
            logging.info("Worker thread finished.")

    def start(self, initial_security_ids: List[str] = None):
        """Start the worker thread."""
        if self._thread and self._thread.is_alive():
            logging.warning("Worker thread already running.")
            return

        if initial_security_ids:
            with self._lock:
                self._subscriptions.update(initial_security_ids)
            logging.info(f"Initial subscriptions set: {len(initial_security_ids)} instruments.")

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker, name="DhanRedisWorker-1", daemon=True)
        self._thread.start()
        logging.info("Worker thread started successfully.")

    def stop(self):
        """Stop the worker thread gracefully."""
        if not self._thread or not self._thread.is_alive():
            logging.warning("Worker thread not running.")
            return

        logging.info("Stopping worker thread...")
        self._stop_event.set()
        
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            logging.warning("Worker thread did not terminate gracefully.")
        else:
            logging.info("Worker thread stopped successfully.")
        
        self._thread = None
        
        try:
            self._redis_client.close()
            logging.info("Redis connection closed.")
        except Exception as e:
            logging.warning(f"Error closing Redis connection: {e}")

    def get_subscriptions(self) -> Set[str]:
        with self._lock:
            return self._subscriptions.copy()

    def get_stats(self) -> Dict:
        with self._lock:
            stats = self._stats.copy()
            stats['active_subscriptions'] = len(self._subscriptions)
            stats['worker_running'] = self._thread and self._thread.is_alive()
            if stats['last_update_time']:
                stats['seconds_since_last_update'] = time.time() - stats['last_update_time']
            return stats

    def health_check(self) -> Dict:
        try:
            redis_ok = self._redis_client.ping()
        except:
            redis_ok = False
        
        # Get the base stats dictionary (which includes 'redis_errors')
        stats = self.get_stats() 
        
        # Add the new health-specific info
        stats['redis_connected'] = redis_ok
        stats['status'] = 'healthy' if redis_ok and stats['worker_running'] else 'unhealthy'
        
        # Return the complete dictionary
        return stats
    
# --- Main execution block for the worker ---
if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Starting DhanHQ Redis WORKER 1...")
    logging.info("=" * 60)

    initial_subs = []
    try:
        # Load the instruments for this worker
        instrument_df = pd.read_csv("instruments_1.csv")
        
        # Get the security ID column
        initial_subs = instrument_df['SEM_SMST_SECURITY_ID'].astype(str).tolist()
        
        logging.info(f"Loaded {len(initial_subs)} instruments from instruments_1.csv")
        if not initial_subs:
            raise ValueError("No instruments found in instruments_1.csv")
            
    except FileNotFoundError:
        logging.error("FATAL: instruments_1.csv not found. Worker cannot start.")
        exit(1)
    except Exception as e:
        logging.error(f"FATAL: Error loading instruments: {e}")
        exit(1)


    worker = DhanRedisWorker(
        DHAN_CLIENT_ID, 
        DHAN_ACCESS_TOKEN, 
        REDIS_HOST, 
        REDIS_PORT,
        redis_password=REDIS_PASSWORD,
        redis_db=REDIS_DB,
        data_ttl=REDIS_DATA_TTL
    )
    
    worker.start(initial_subs)

    try:
        # Main monitoring loop
        while True:
            time.sleep(60)
            health = worker.health_check()
            logging.info(f"Health: {health['status']} | "
                        f"Subs: {health['active_subscriptions']} | "
                        f"Updates: {health['total_updates']} | "
                        f"Errors: {health['redis_errors']}")
            
    except KeyboardInterrupt:
        logging.info("\nKeyboardInterrupt received.")
    finally:
        logging.info("Shutting down worker...")
        worker.stop()
        logging.info("=" * 60)
        logging.info("Worker 1 shut down complete.")
        logging.info("=" * 60)