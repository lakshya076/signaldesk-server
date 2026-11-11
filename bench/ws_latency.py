"""Measure tick latency from the redis-worker to a WebSocket client.

Each price message carries "ts", the time the worker received the tick. Latency is
(time the client receives it - ts), so it covers Redis write + pub/sub + backend fan-out + WebSocket.
Run the worker, backend and this script on one machine so they share a clock.

    python bench/ws_latency.py --symbols 200 --clients 5 --seconds 30
"""
import argparse
import asyncio
import csv
import json
import statistics
import time
from pathlib import Path

import websockets

INSTRUMENTS = Path(__file__).resolve().parent.parent / "backend" / "instruments.csv"


async def client(url, symbols, deadline, latencies):
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"action": "bulk_subscribe", "symbols": symbols}))
        while time.time() < deadline:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), deadline - time.time()))
            except asyncio.TimeoutError:
                break
            if "ts" in msg:
                latencies.append((time.time() - msg["ts"]) * 1000)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="ws://localhost:8000/ws/prices")
    ap.add_argument("--symbols", type=int, default=100, help="instruments each client subscribes to")
    ap.add_argument("--clients", type=int, default=1, help="concurrent WebSocket clients")
    ap.add_argument("--seconds", type=float, default=20)
    args = ap.parse_args()

    with open(INSTRUMENTS, newline="", encoding="utf-8") as f:
        symbols = [row["SEM_TRADING_SYMBOL"] for row in csv.DictReader(f)][: args.symbols]

    latencies = []
    deadline = time.time() + args.seconds
    await asyncio.gather(*(client(args.url, symbols, deadline, latencies) for _ in range(args.clients)))

    if not latencies:
        raise SystemExit("No ticks received. Is the worker running with FEED_MODE=mock?")
    q = statistics.quantiles(latencies, n=100)
    print(f"{len(latencies)} messages to {args.clients} clients over {args.seconds:.0f}s "
          f"({len(latencies) / args.seconds:.0f} msg/s)")
    print(f"latency ms: p50 {q[49]:.2f} | p95 {q[94]:.2f} | p99 {q[98]:.2f} | max {max(latencies):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
