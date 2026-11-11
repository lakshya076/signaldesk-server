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
| `supabase/` | Database schema and migrations (see [Database](#database-supabase)) |

## Run the whole system

SignalDesk is two repos: [signaldesk-server](https://github.com/lakshya076/signaldesk-server) (backend + market-data worker) and [signaldesk-apps](https://github.com/lakshya076/signaldesk-apps) (Flutter apps). Clone both side by side. You need Python 3.12, Docker (for Redis), Flutter 3.47 or newer, and a Supabase project.

Start things in this order: Supabase (once), Redis, backend, worker, then any of the apps.

### 1. Supabase (once)

1. Create a project at [supabase.com](https://supabase.com).
2. In the SQL editor, run `signaldesk-server/supabase/schema.sql`, then each file in `signaldesk-server/supabase/migrations/` in name order.
3. From **Project Settings → API keys**, copy the project URL, the publishable key and the secret (service role) key.
4. Create `signaldesk-server/backend/.env`:

   ```
   URL=https://<project-ref>.supabase.co
   SERVICE_KEY=<secret key>
   ANON_KEY=<publishable key>
   ```

   The secret key bypasses row level security, so keep it on the server; `.env` is gitignored.

### 2. Redis, backend and worker

Use one terminal per long-running command.

**Linux / macOS (bash)**

```bash
# Terminal 1: Redis, then the backend
docker run -d --rm --name redis -p 6379:6379 redis:7-alpine
cd signaldesk-server/backend
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt pytest
.venv/bin/uvicorn main:app --port 8000

# Terminal 2: market-data worker on the simulated feed
cd signaldesk-server/redis-worker
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
FEED_MODE=mock MOCK_TICKS_PER_SEC=50 .venv/bin/python dhan_worker.py

# Check: should print "status":"healthy"
curl localhost:8000/health
```

**Windows (PowerShell)**

```powershell
# Window 1: Redis, then the backend
docker run -d --rm --name redis -p 6379:6379 redis:7-alpine
cd signaldesk-server\backend
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt pytest
.venv\Scripts\python -m uvicorn main:app --port 8000

# Window 2: market-data worker on the simulated feed
cd signaldesk-server\redis-worker
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
$env:FEED_MODE = "mock"; $env:MOCK_TICKS_PER_SEC = "50"
.venv\Scripts\python dhan_worker.py

# Check: should print "status":"healthy" (curl.exe, because "curl" is a different command in Windows PowerShell)
curl.exe localhost:8000/health
```

Use Python 3.12: some dependencies (pandas) may not ship packages for newer versions yet. If `py` isn't available, `uv venv -p 3.12 .venv` works too. Calling `.venv\Scripts\python` directly avoids PowerShell's script-execution policy blocking `Activate.ps1`.

`MOCK_TICKS_PER_SEC=50` feels like a real market in the apps; raise it (for example to 500) for load testing.

### 3. The apps

```bash
cd signaldesk-apps/user          # or advisor / admin  (Windows: cd signaldesk-apps\user)
flutter pub get
flutter run -d windows --dart-define=API_BASE_URL=http://localhost:8000
```

- Device: `-d windows`, `-d linux`, `-d macos` for a desktop app, `-d chrome` for the browser (no extra setup), or a connected phone or emulator. Desktop builds need the platform toolchain (on Windows, Visual Studio's "Desktop development with C++"); `flutter doctor` shows what's missing.
- Android emulator: use `API_BASE_URL=http://10.0.2.2:8000` (the emulator's address for your computer).
- Your own Supabase project: add `--dart-define=SUPABASE_URL=https://<project-ref>.supabase.co --dart-define=SUPABASE_ANON_KEY=<publishable key>`.
- Run several apps at once by starting each from its own terminal.

### 4. Accounts

1. Sign up in the **advisor** app and in the **user** app (confirm the email if your Supabase project requires it).
2. There is no admin sign-up, on purpose. Sign up any account, then in the Supabase table editor set its `role` in `profiles` to `admin`.
3. Sign in to the **admin** app and approve the advisor. Advisors can't create trades until approved.
4. In the **user** app, subscribe to the advisor to see their trades.

### 5. Market hours

The backend treats the market as open Monday to Friday, 9:15 to 15:30 IST (NSE hours), and only allows creating trades then. Outside those hours, adding a stock to the watchlist asks Dhan for the last prices, which needs Dhan credentials; without them that step fails. Test the trading flows during market hours.

### 6. Tests and benchmark

```bash
# Backend tests (Redis must be running). Windows: .venv\Scripts\python -m pytest test_rate_limit.py
cd signaldesk-server/backend && .venv/bin/pytest test_rate_limit.py

# Latency benchmark (backend and worker running). Windows: backend\.venv\Scripts\python bench\ws_latency.py ...
cd signaldesk-server && backend/.venv/bin/python bench/ws_latency.py --symbols 200 --clients 10 --seconds 30

# App tests, in each of advisor/, user/, admin/
cd signaldesk-apps/user && flutter test
```

Stop everything with Ctrl+C in each terminal, then `docker stop redis`.

## Settings

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

## Database (Supabase)

`supabase/schema.sql` is the full public schema (tables, constraints, functions, triggers, row level security policies), pulled from the production project. `supabase/migrations/` holds changes on top of it; apply them in name order.

| Table | Holds |
|---|---|
| `profiles` | One row per account: role (`user`, `advisor`, `admin`), name, contact details, `is_verified` for advisors |
| `advisor_details`, `user_details` | Role-specific details (advisor licence type and registration number) |
| `trade_cards` | Trade signals: stock, direction, entry, target, stop loss, status, close price |
| `subscriptions` | Which users follow which advisors |
| `watchlists` | Each advisor's watched stocks |
| `advisor_statistics` | Follower count, closed trades and hit rate per advisor, kept up to date by triggers |
| `platform_statistics` | Single-row dashboard numbers for the admin app, kept up to date by triggers |

Access rules (row level security):

- Signed-out clients can't read or write any table; sign-up goes through Supabase Auth, and a trigger creates the profile.
- Sign-up can only create `user` or `advisor` accounts. Admins are promoted by hand.
- Signed-in users see their own profile and advisors' public profile fields, never other people's phone numbers. They can edit only their name and phone number, not their role or verification.
- Advisors see and manage only their own trades and watchlist; only advisors approved by an admin can create trades.
- Users see trades only from advisors they subscribe to.
- Statistics tables are read-only for clients and written by triggers.

The backend uses the secret key, which bypasses these rules, so it checks roles and advisor approval itself before acting.

## API

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /`, `GET /health` | – | Service info and Redis health |
| `WS /ws/prices` | – | Send `{"action": "subscribe", "symbols": [...]}`; receive `{"symbol", "ltp", "prev_close", "ts"}` per tick |
| `POST /create-trade` | approved advisor | Create a trade card at the live price (market hours only) |
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
