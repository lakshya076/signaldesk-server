"""Drop-in stand-in for dhanhq.marketfeed.DhanFeed that generates simulated ticks.

Used when FEED_MODE=mock, so the whole pipeline (worker -> Redis -> backend -> WebSocket clients)
can run and be load-tested without a Dhan data subscription. Prices are a random walk; the
packets have the same shape DhanFeed.get_data() returns.

MOCK_TICKS_PER_SEC sets the total tick rate across all subscribed instruments (default 50).
"""
import os
import random
import time

TICKS_PER_SEC = float(os.environ.get("MOCK_TICKS_PER_SEC", 50))


class MockFeed:
    def __init__(self, client_id, access_token, instruments, version="v2"):
        self._prices = {}
        self._pending_prev_close = []
        self._next_tick = time.monotonic()
        self.subscribe_symbols(instruments)

    def run_forever(self):
        pass  # nothing to connect to

    def subscribe_symbols(self, symbols):
        for _, sec_id, _ in symbols:
            if sec_id not in self._prices:
                self._prices[sec_id] = random.uniform(100, 3000)
                self._pending_prev_close.append(sec_id)

    def unsubscribe_symbols(self, symbols):
        for _, sec_id, _ in symbols:
            self._prices.pop(sec_id, None)

    async def disconnect(self):
        pass

    def get_data(self):
        # Like DhanFeed, send each instrument's previous close once, right after it is subscribed.
        while self._pending_prev_close:
            sec_id = self._pending_prev_close.pop()
            if sec_id in self._prices:
                return {"type": "Previous Close", "security_id": int(sec_id),
                        "prev_close": f"{self._prices[sec_id]:.2f}"}

        if not self._prices:
            time.sleep(0.05)
            return None

        # Pace ticks to TICKS_PER_SEC, the way recv() blocks on a real socket.
        delay = self._next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._next_tick = max(self._next_tick, time.monotonic() - 1) + 1 / TICKS_PER_SEC

        sec_id = random.choice(list(self._prices))
        self._prices[sec_id] = round(self._prices[sec_id] * (1 + random.gauss(0, 0.0005)), 2)
        return {"type": "Ticker Data", "security_id": int(sec_id),
                "LTP": f"{self._prices[sec_id]:.2f}", "LTT": time.strftime("%H:%M:%S")}
