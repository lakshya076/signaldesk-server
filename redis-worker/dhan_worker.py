import json
import os
import threading
import time
import logging
import asyncio
import redis
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

# --- Redis Key Prefix ---
MARKET_DATA_PREFIX = "marketdata:"


class DhanRedisWorker:
    def __init__(self, client_id: str, access_token: str, redis_host: str, redis_port: int, 
                 redis_password: str = None, redis_db: int = 0, data_ttl: int = 86400):
        self._client_id = client_id
        self._access_token = access_token
        self._feed_client = None
        self._data_ttl = data_ttl

        # Redis connection with connection pooling and retry
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
        
        self._pubsub = self._redis_client.pubsub(ignore_subscribe_messages=True)
        self._listener_thread = None
        
        self._lock = threading.Lock()
        
        # --- MODIFIED: Subscription State ---
        # _subscriptions: Holds the *current* state of what is *actually* subscribed to.
        # _pending_additions/removals: Queue for changes from other threads.
        self._subscriptions: Set[str] = set()
        self._pending_additions: Set[str] = set()
        self._pending_removals: Set[str] = set()
        # --- END MODIFICATION ---

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event() # Still used for initial/major failures

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
            if not sec_id:
                return

            redis_key = self._get_redis_key(sec_id)
            update_data = {}
            current_timestamp = int(time.time())

            if data.get('type') == 'Ticker Data' and 'LTP' in data:
                try:
                    new_ltp = float(data['LTP'])
                    
                    # Check if LTP actually changed from what's in Redis
                    existing_data = self._redis_client.hgetall(redis_key)
                    existing_ltp = existing_data.get('ltp')
                    
                    # Only update timestamp if LTP changed or doesn't exist
                    if existing_ltp is None or float(existing_ltp) != new_ltp:
                        update_data['ltp'] = new_ltp
                        update_data['ltp_timestamp'] = current_timestamp
                        logging.debug(f"LTP changed for {sec_id}: {existing_ltp} -> {new_ltp}")
                    else:
                        # LTP unchanged - this is likely stale/cached data
                        logging.debug(f"LTP unchanged for {sec_id}, skipping timestamp update")
                        return
                        
                except (ValueError, TypeError):
                    logging.warning(f"Invalid LTP format for {sec_id}: {data['LTP']}")

            elif data.get('type') == 'Previous Close' and 'prev_close' in data:
                try:
                    new_prev_close = float(data['prev_close'])
                    
                    # Check if prev_close actually changed
                    existing_data = self._redis_client.hgetall(redis_key)
                    existing_prev_close = existing_data.get('prev_close')
                    
                    if existing_prev_close is None or float(existing_prev_close) != new_prev_close:
                        update_data['prev_close'] = new_prev_close
                        update_data['prev_close_timestamp'] = current_timestamp
                        logging.debug(f"Prev close changed for {sec_id}: {existing_prev_close} -> {new_prev_close}")
                    else:
                        logging.debug(f"Prev close unchanged for {sec_id}, skipping timestamp update")
                        return
                        
                except (ValueError, TypeError):
                    logging.warning(f"Invalid prev_close format for {sec_id}: {data['prev_close']}")

            if update_data:
                try:
                    # Use pipeline for atomic operation
                    pipe = self._redis_client.pipeline()
                    pipe.hset(redis_key, mapping=update_data)
                    pipe.expire(redis_key, self._data_ttl)  # Set TTL
                    pipe.execute()
                    
                    self._stats['redis_writes'] += 1
                    self._stats['total_updates'] += 1
                    self._stats['last_update_time'] = time.time()

                    # Websocket migration
                    hotline_message = {
                        "security_id": sec_id,
                        "data": update_data
                    }
                    # Publish the update to the new channel
                    self._loop.run_in_executor(
                        None,
                        self._redis_client.publish,
                        f"{MARKET_DATA_PREFIX}updates",
                        json.dumps(hotline_message)
                    )
                    
                except redis.exceptions.ConnectionError:
                    logging.error(f"Redis connection lost while writing {sec_id}")
                    self._stats['redis_errors'] += 1
                    # Attempt reconnection
                    try:
                        self._loop.run_in_executor(None, self._redis_client.ping)
                    except:
                        logging.error("Redis reconnection failed")
                except redis.exceptions.RedisError as e:
                    logging.error(f"Redis write error for {sec_id}: {e}")
                    self._stats['redis_errors'] += 1

        except Exception as e:
            logging.warning(f"Error processing data: {e} - Data: {data}")

    # --- NEW METHOD ---
    def _process_subscription_changes(self):
        """
        Processes pending subscription additions and removals dynamically.
        This method MUST be called from the _worker thread.
        """
        to_add_ids = set()
        to_remove_ids = set()
        
        # 1. Atomically get pending changes and clear sets
        with self._lock:
            if self._pending_additions:
                to_add_ids = self._pending_additions.copy()
                self._pending_additions.clear()
            if self._pending_removals:
                to_remove_ids = self._pending_removals.copy()
                self._pending_removals.clear()

        if not to_add_ids and not to_remove_ids:
            return

        # 2. Sanity check: 'add' wins over 'remove' if both were queued
        to_remove_ids.difference_update(to_add_ids)
        
        # 3. Create instrument lists
        add_instruments = []
        if to_add_ids:
            add_instruments = [(marketfeed.NSE, sec_id, marketfeed.Ticker) for sec_id in to_add_ids]

        remove_instruments = []
        if to_remove_ids:
            remove_instruments = [(marketfeed.NSE, sec_id, marketfeed.Ticker) for sec_id in to_remove_ids]

        # 4. Call DhanHQ API
        if add_instruments:
            try:
                logging.info(f"Subscribing to {len(add_instruments)} new instruments...")
                self._feed_client.subscribe_symbols(add_instruments)
                # Update our internal state *after* successful API call
                with self._lock:
                    self._subscriptions.update(to_add_ids)
                logging.info(f"Subscribed. Total active: {len(self._subscriptions)}")
            except Exception as e:
                logging.error(f"Failed to subscribe to symbols: {e}. Re-queueing...")
                # Put them back to be retried
                with self._lock:
                    self._pending_additions.update(to_add_ids)

        if remove_instruments:
            try:
                logging.info(f"Unsubscribing from {len(remove_instruments)} instruments...")
                self._feed_client.unsubscribe_symbols(remove_instruments)
                # Update internal state *after* successful API call
                with self._lock:
                    self._subscriptions.difference_update(to_remove_ids)
                logging.info(f"Unsubscribed. Total active: {len(self._subscriptions)}")
            except Exception as e:
                logging.error(f"Failed to unsubscribe from symbols: {e}. Re-queueing...")
                # Put them back to be retried
                with self._lock:
                    self._pending_removals.update(to_remove_ids)

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
                        # _subscriptions holds the *actual* state
                        current_instruments = [(marketfeed.NSE, sec_id, marketfeed.Ticker)
                                               for sec_id in self._subscriptions]

                    if not current_instruments:
                        logging.info("No instruments subscribed. Waiting...")
                        # Wait for stop or reconnect signal (e.g., initial subs)
                        if self._reconnect_event.wait(timeout=5):
                            self._reconnect_event.clear()
                        continue

                    self._stats['connection_attempts'] += 1
                    logging.info(f"Attempting connection for {len(current_instruments)} instruments...")
                    
                    self._feed_client = marketfeed.DhanFeed(
                        self._client_id, self._access_token, current_instruments, "v2"
                    )
                    logging.info(f"✓ Connected successfully with {len(current_instruments)} instruments")
                    self._reconnect_event.clear()

                    # Inner loop to process data
                    while not self._stop_event.is_set() and not self._reconnect_event.is_set():
                        
                        # --- MODIFIED: Process dynamic sub changes ---
                        # Check for and process any pending subscription changes
                        # This calls subscribe_symbols/unsubscribe_symbols on the fly
                        self._process_subscription_changes()
                        # --- END MODIFICATION ---

                        self._feed_client.run_forever()
                        response = self._feed_client.get_data()
                        if response:
                            self._process_data(response)
                        time.sleep(0.001)

                    if self._reconnect_event.is_set():
                        logging.info("Reconnect signaled. Disconnecting...")
                    elif self._stop_event.is_set():
                        logging.info("Stop signaled. Disconnecting...")

                except ConnectionRefusedError:
                    logging.error("Connection refused. Is DhanHQ feed server reachable? Retrying in 30s...")
                    if not self._stop_event.wait(30):
                        continue
                except Exception as e:
                    if not self._stop_event.is_set():
                        logging.error(f"Worker loop error: {e}. Retrying in 10s...")
                        if not self._stop_event.wait(10):
                            continue
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
            try:
                time.sleep(0.1)  # Give time for cleanup
                
                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    try:
                        loop.run_until_complete(
                            asyncio.wait_for(
                                asyncio.gather(*pending, return_exceptions=True),
                                timeout=2.0
                            )
                        )
                    except asyncio.TimeoutError:
                        logging.warning("Some tasks did not cancel in time")
                    except Exception as e:
                        logging.warning(f"Task cancellation error: {e}")
                
                loop.run_until_complete(asyncio.sleep(0.1))
                
            except Exception as e:
                logging.warning(f"Error during loop cleanup: {e}")
            finally:
                try:
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
        self._reconnect_event.clear()
        self._thread = threading.Thread(target=self._worker, name="DhanRedisWorker", daemon=True)
        self._start_listener_thread()
        self._thread.start()
        logging.info("Worker thread started successfully.")

    def stop(self):
        """Stop the worker thread gracefully."""
        if not self._thread or not self._thread.is_alive():
            logging.warning("Worker thread not running.")
            return

        logging.info("Stopping worker thread...")
        self._stop_event.set()
        self._reconnect_event.set()  # Wake up if waiting
        
        # Wait for graceful shutdown
        self._pubsub.unsubscribe()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            logging.warning("Worker thread did not terminate gracefully within timeout.")
        else:
            logging.info("Worker thread stopped successfully.")
        
        self._thread = None
        
        # Close Redis connection
        try:
            self._redis_client.close()
            logging.info("Redis connection closed.")
        except Exception as e:
            logging.warning(f"Error closing Redis connection: {e}")

    # --- MODIFIED: add_subscription ---
    def add_subscription(self, security_id: str):
        """Add a subscription to the pending queue."""
        with self._lock:
            # If it's pending removal, just cancel the removal
            if security_id in self._pending_removals:
                self._pending_removals.discard(security_id)
            
            # Only add if it's not already active or pending add
            if security_id not in self._subscriptions and security_id not in self._pending_additions:
                self._pending_additions.add(security_id)
                logging.info(f"Queued ADD for {security_id}")
            else:
                logging.info(f"Already subscribed or queued for {security_id}, nothing to queue.")
        # NO reconnect event, NO sleep

    # --- MODIFIED: remove_subscription ---
    def remove_subscription(self, security_id: str):
        """Add a removal to the pending queue."""
        with self._lock:
            # If it's pending addition, just cancel the addition
            if security_id in self._pending_additions:
                self._pending_additions.discard(security_id)
            
            # Only remove if it's currently active and not already pending removal
            if security_id in self._subscriptions and security_id not in self._pending_removals:
                self._pending_removals.add(security_id)
                logging.info(f"Queued REMOVE for {security_id}")
            else:
                logging.info(f"Not subscribed or already queued for removal: {security_id}")
        # NO reconnect event, NO sleep

    def get_subscriptions(self) -> Set[str]:
        """Get current subscriptions (thread-safe)."""
        with self._lock:
            return self._subscriptions.copy()

    def get_stats(self) -> Dict:
        """Get worker statistics."""
        with self._lock:
            stats = self._stats.copy()
            stats['active_subscriptions'] = len(self._subscriptions)
            stats['pending_additions'] = len(self._pending_additions)
            stats['pending_removals'] = len(self._pending_removals)
            stats['worker_running'] = self._thread and self._thread.is_alive()
            
            # Calculate uptime if last update exists
            if stats['last_update_time']:
                stats['seconds_since_last_update'] = time.time() - stats['last_update_time']
            
            return stats

    def health_check(self) -> Dict:
        """Health check for monitoring."""
        try:
            redis_ok = self._redis_client.ping()
        except:
            redis_ok = False
        
        stats = self.get_stats()
        
        return {
            'status': 'healthy' if redis_ok and stats['worker_running'] else 'unhealthy',
            'redis_connected': redis_ok,
            'worker_running': stats['worker_running'],
            'active_subscriptions': stats['active_subscriptions'],
            'pending_additions': stats['pending_additions'],
            'pending_removals': stats['pending_removals'],
            'total_updates': stats['total_updates'],
            'redis_writes': stats['redis_writes'],
            'redis_errors': stats['redis_errors']
        }
    
    def _pubsub_listener(self):
        """A listener thread to handle pub/sub messages for subscriptions."""
        RETRY_DELAY = 10

        while not self._stop_event.is_set():
            try:
                self._pubsub.subscribe("signaldesk:subscriptions")
                logging.info("✓ Subscribed to 'signaldesk:subscriptions' channel.")

                for message in self._pubsub.listen():
                    if self._stop_event.is_set():
                        break

                    if message['type'] == 'message':
                        try:
                            data = json.loads(message['data'])
                            action = data.get('action')
                            
                            # --- NEW BATCH LOGIC ---
                            security_ids_to_process = []
                            
                            # Check for new "security_ids" (plural) key
                            if "security_ids" in data:
                                ids = data.get("security_ids")
                                if isinstance(ids, list):
                                    security_ids_to_process.extend(ids)
                            
                            # Check for old "security_id" (singular) key
                            elif "security_id" in data:
                                ids = data.get("security_id")
                                if isinstance(ids, str):
                                    security_ids_to_process.append(ids)
                            
                            if not security_ids_to_process:
                                logging.warning("PubSub message had no valid security_id or security_ids")
                                continue
                            
                            # --- MODIFIED: Just call batch methods ---
                            if action == 'add':
                                logging.info(f"[PubSub] Received ADD request for {len(security_ids_to_process)} IDs")
                                self._add_subscriptions_batch(security_ids_to_process)
                            elif action == 'remove':
                                logging.info(f"[PubSub] Received REMOVE request for {len(security_ids_to_process)} IDs")
                                self._remove_subscriptions_batch(security_ids_to_process)
                            
                            # NO reconnect event is signaled. The worker will pick up changes.
                            # --- END MODIFICATION ---
                            
                        except Exception as e:
                            logging.error(f"Error processing pub/sub message: {e}")

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                if not self._stop_event.is_set():
                    logging.error(f"PubSub connection error: {e}. Retrying in {RETRY_DELAY}s...")
                    self._stop_event.wait(RETRY_DELAY)
            except Exception as e:
                if not self._stop_event.is_set():
                    logging.error(f"PubSub listener crashed unexpectedly: {e}. Retrying in {RETRY_DELAY}s...")
                    self._stop_event.wait(RETRY_DELAY)

        logging.info("PubSub listener shutting down.")


    def _start_listener_thread(self):
        """Starts the pubsub listener thread."""
        if self._listener_thread and self._listener_thread.is_alive():
            return

        self._listener_thread = threading.Thread(
            target=self._pubsub_listener, 
            name="DhanSubListener", 
            daemon=True
        )
        self._listener_thread.start()
    
    # --- MODIFIED: _add_subscriptions_batch ---
    def _add_subscriptions_batch(self, security_ids: List[str]) -> bool:
        """
        Add a batch of subscriptions to the pending queue.
        Returns True if changes were queued, False otherwise.
        """
        with self._lock:
            new_adds = 0
            ids_to_add = set(security_ids)
            
            # Cancel pending removals for these IDs
            removals_to_cancel = self._pending_removals.intersection(ids_to_add)
            if removals_to_cancel:
                self._pending_removals.difference_update(removals_to_cancel)
                logging.debug(f"Cancelled {len(removals_to_cancel)} pending removals.")

            # Queue up new additions (ones not already active and not pending)
            for sec_id in ids_to_add:
                if sec_id not in self._subscriptions and sec_id not in self._pending_additions:
                    self._pending_additions.add(sec_id)
                    new_adds += 1
            
            if new_adds > 0:
                logging.info(f"Batch Queued {new_adds} new subscriptions.")
                return True
            else:
                logging.debug(f"Batch add: No new IDs to queue from {len(security_ids)} requested.")
                return False

    # --- MODIFIED: _remove_subscriptions_batch ---
    def _remove_subscriptions_batch(self, security_ids: List[str]) -> bool:
        """
        Add a batch of removals to the pending queue.
        Returns True if changes were queued, False otherwise.
        """
        with self._lock:
            new_removals = 0
            ids_to_remove = set(security_ids)
            
            # Cancel pending additions for these IDs
            additions_to_cancel = self._pending_additions.intersection(ids_to_remove)
            if additions_to_cancel:
                self._pending_additions.difference_update(additions_to_cancel)
                logging.debug(f"Cancelled {len(additions_to_cancel)} pending additions.")

            # Queue up removals (ones that are currently active and not pending)
            for sec_id in ids_to_remove:
                if sec_id in self._subscriptions and sec_id not in self._pending_removals:
                    self._pending_removals.add(sec_id)
                    new_removals += 1
                    
            if new_removals > 0:
                logging.info(f"Batch Queued {new_removals} removals.")
                return True
            else:
                logging.debug(f"Batch remove: No active IDs to queue for removal from {len(security_ids)} requested.")
                return False


# --- Main execution block for the worker ---
if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Starting DhanHQ Redis Worker...")
    logging.info("=" * 60)

    # Initial subscriptions - can load from DB/config
    initial_subs = ["1333", "11915"]  # HDFC Bank, Infosys

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
            time.sleep(60)  # Check every 60secs
            
            # Print health stats
            health = worker.health_check()
            logging.info(f"Health: {health['status']} | "
                         f"Subs: {health['active_subscriptions']} | "
                         f"Pending +{health['pending_additions']}/-{health['pending_removals']} | "
                         f"Updates: {health['total_updates']} | "
                         f"Errors: {health['redis_errors']}")
            
    except KeyboardInterrupt:
        logging.info("\nKeyboardInterrupt received.")
    finally:
        logging.info("Shutting down worker...")
        worker.stop()
        logging.info("=" * 60)
        logging.info("Worker shut down complete.")
        logging.info("=" * 60)