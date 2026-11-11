# SignalDesk Server

[![CI](https://github.com/lakshya076/signaldesk-server/actions/workflows/ci.yml/badge.svg)](https://github.com/lakshya076/signaldesk-server/actions/workflows/ci.yml)

Backend and market-data worker for **SignalDesk**, a stock-advisory platform for the Indian market (NSE equities). Advisors publish trade signals with an entry, target and stop loss; subscribers follow those trades with live prices; admins approve advisors and watch platform statistics.

The three Flutter clients (advisor, user, admin) live in [signaldesk-apps](https://github.com/lakshya076/signaldesk-apps).

## Market data: Dhan then, simulated feed now

SignalDesk was built on the [Dhan](https://dhanhq.co) market feed: the worker held a WebSocket to Dhan, received NSE ticks and pushed them through Redis to the apps.

**The project no longer has a Dhan API key, so live market data is off.** To keep the system runnable and testable, the worker has a mock mode (`FEED_MODE=mock`) that produces simulated ticks in the same packet format as Dhan's feed. Everything after the feed (Redis, the backend, WebSocket fan-out, the apps) is the same code either way. With a Dhan client ID and access token, the worker connects to the real feed again with no code changes.

## Architecture

```
Dhan feed / mock feed ──► redis-worker ──► Redis ──► backend (FastAPI) ──► WebSocket ──► Flutter apps
                               ▲                          │                    REST ◄──┘
                               └── subscription requests ─┘          Supabase (Auth + Postgres)
```

- **redis-worker** keeps one feed connection and writes each tick to a Redis hash (`marketdata:<security_id>`), then publishes it on `marketdata:updates` in the same round trip.
- **backend** listens on that channel and forwards each tick only to WebSocket clients subscribed to that stock. When the first client asks for a stock it tells the worker to subscribe (`signaldesk:subscriptions`); when the last client leaves it unsubscribes, so the feed only carries stocks someone is watching.
- **Supabase** handles sign-in and stores profiles, trade cards and subscriptions. The backend checks each caller's Supabase JWT and role (`advisor` or `admin`) before protected actions.
- Scheduler endpoints close open trades when live prices cross their target or stop loss.

## Repository layout

| Folder | What it is |
|---|---|
| `backend/` | FastAPI service: REST endpoints, role checks, rate limiting, WebSocket price fan-out |
| `redis-worker/` | Market-data worker. `dhan_worker.py` subscribes on demand; `dhan_worker_1.py` / `_2.py` cover the whole NSE list split across two feed connections (Dhan allows 5,000 instruments per connection) |
| `bench/` | WebSocket latency benchmark |

## Run locally

Needs Python 3.12 and Redis.

```bash
docker run -d --rm --name redis -p 6379:6379 redis:7-alpine

cd backend
pip install -r requirements.txt
uvicorn main:app --port 8000

cd redis-worker
pip install -r requirements.txt
FEED_MODE=mock python dhan_worker.py      # simulated ticks
python dhan_worker.py                      # real Dhan feed (needs DHAN_* variables)
```

Settings come from environment variables or a `.env` file in each folder:

| Variable | Used by | Purpose |
|---|---|---|
| `URL`, `SERVICE_KEY`, `ANON_KEY` | backend | Supabase project URL and keys (required) |
| `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN` | both | Dhan credentials. Optional for the backend (only `/backfill-subscribe`); required by the worker unless `FEED_MODE=mock` |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB` | both | Redis connection (default `localhost:6379`, db 0) |
| `REDIS_DATA_TTL` | both | Seconds a stock's price hash lives in Redis (default 4 days) |
| `FEED_MODE` | worker | `dhan` (default) or `mock` |
| `MOCK_TICKS_PER_SEC` | worker | Total simulated ticks per second (default 50) |
| `RATE_LIMIT_PER_MIN` | backend | Default requests per minute per client IP (default 120) |

## API

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /`, `GET /health` | – | Service info and Redis health |
| `WS /ws/prices` | – | Send `{"action": "subscribe", "symbols": [...]}`; receive `{"symbol", "ltp", "prev_close", "ts"}` per tick |
| `POST /create-trade` | advisor | Create a trade card at the live price (market hours only) |
| `GET /check-market-status` | – | Open or closed, from trading hours and feed freshness |
| `POST /get-live-prices` | – | Latest prices for a list of symbols |
| `GET /get-stock-data/{symbol}` | – | Price, previous close and change for one stock |
| `GET /get-all-stocks`, `GET /get-instrument-list` | – | Full NSE equity list, with or without prices |
| `POST /subscribe-instrument`, `POST /bulk-subscribe` | – | Ask the worker to start or stop streaming stocks |
| `POST /backfill-subscribe` | – | Fill a price from Dhan history when the market is closed (needs Dhan credentials) |
| `GET /check-trade-cards`, `GET /update-last-price` | – | Scheduler jobs: close trades that hit target or stop loss; store closing prices |
| `POST /notify-users` | – | Supabase webhook on trade changes; builds subscriber notifications (delivery is still a stub) |
| `GET /admin/stats`, `GET /admin/dashboard-stats` | admin | Platform statistics |
| `GET /admin/pending-advisors`, `POST /admin/approve-advisor/{id}` | admin | Advisor approval |

## Rate limits

Every endpoint is limited per client IP, counted in Redis so all server processes share one count (`RATE_LIMITS` in `backend/main.py`): 120 requests/min by default, 10–20/min for expensive or scheduler endpoints, 20 new WebSocket connections/min and 60 messages/min per connection. Over the limit, HTTP returns `429` with `Retry-After` and WebSockets close with code `1008`. Behind a proxy, start uvicorn with `--proxy-headers --forwarded-allow-ips=<proxy IP>` so limits apply to real client IPs.

## Latency

Each tick carries `ts`, the time the worker received it. `bench/ws_latency.py` opens WebSocket clients, subscribes them to stocks and measures `receive time − ts`, which covers the Redis write, pub/sub, backend fan-out and WebSocket delivery. It does not include the network hop from Dhan to the worker.

```bash
FEED_MODE=mock MOCK_TICKS_PER_SEC=500 python redis-worker/dhan_worker.py   # in redis-worker/
python bench/ws_latency.py --symbols 200 --clients 10 --seconds 30
```

Measured on one Windows laptop with everything local and the simulated feed: 116,537 messages to 10 clients in 30 s (about 3,900 msg/s), **p50 1.01 ms, p95 1.56 ms, p99 2.12 ms**, max 87 ms.

## Tests and CI

`.github/workflows/ci.yml` runs on every push and pull request:

1. Byte-compiles all Python code.
2. Runs the backend tests (`backend/test_rate_limit.py`) against a Redis service container.
3. Starts the worker in mock mode and the backend, runs the latency benchmark, and fails if p99 latency is above 1 second.

Run the tests locally with Redis up: `cd backend && pytest test_rate_limit.py`.
