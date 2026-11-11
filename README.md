# SignalDesk Server

Backend and market-data worker for SignalDesk, a stock-advisory platform. Advisors publish trade signals (entry, target, stop loss), users follow those trades with live prices, and admins approve advisors. The Flutter apps live in [signaldesk-apps](https://github.com/lakshya076/signaldesk-apps).

| Folder | What it is |
|---|---|
| `backend/` | FastAPI service: REST endpoints, role checks with Supabase Auth, WebSocket price fan-out at `/ws/prices` |
| `redis-worker/` | Market-data worker: reads the Dhan live feed and writes ticks to Redis, then publishes them over Redis pub/sub |
| `bench/` | WebSocket latency benchmark |

## How live prices flow

```
Dhan feed ──► redis-worker ──► Redis (hash per stock + pub/sub "marketdata:updates") ──► backend ──► WebSocket clients
                   ▲                                                                       │
                   └──────────── "signaldesk:subscriptions" (add / remove stock) ◄─────────┘
```

The backend tells the worker to subscribe to a stock when the first client asks for it, and to unsubscribe when the last client leaves.

## Run locally

Needs Python 3.12 and Redis.

```bash
docker run -d --rm --name redis -p 6379:6379 redis:7-alpine

# Backend: needs URL, SERVICE_KEY and ANON_KEY (Supabase) in backend/.env.
# DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are optional; only /backfill-subscribe uses them.
cd backend && pip install -r requirements.txt && uvicorn main:app --port 8000

# Worker: real Dhan feed (DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in redis-worker/.env) ...
cd redis-worker && pip install -r requirements.txt && python dhan_worker.py
# ... or simulated ticks, no Dhan account needed
FEED_MODE=mock MOCK_TICKS_PER_SEC=500 python dhan_worker.py
```

## Rate limits

Every endpoint is limited per client IP, counted in Redis (`RATE_LIMITS` in `backend/main.py`): 120 requests/min by default, lower for expensive or scheduler endpoints, 20 new WebSocket connections/min and 60 messages/min per connection. Over the limit, HTTP gets `429` with `Retry-After` and WebSockets close with code `1008`. Behind a proxy, start uvicorn with `--proxy-headers --forwarded-allow-ips=<proxy IP>` so the limit applies to real client IPs. Tests: `cd backend && pytest test_rate_limit.py` (needs Redis).

## Latency benchmark

With the worker running in mock mode and the backend up:

```bash
python bench/ws_latency.py --symbols 200 --clients 10 --seconds 30
```

Each message carries the time the worker received the tick, so the reported latency covers Redis write, pub/sub, backend fan-out and WebSocket delivery. It does not include the network hop from Dhan to the worker.

Result on one Windows laptop, everything local: 116,537 messages to 10 clients in 30 s (about 3,900 msg/s), p50 1.01 ms, p95 1.56 ms, p99 2.12 ms, max 87 ms.
