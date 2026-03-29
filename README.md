# API Ingestion Server — MongoDB Edition

A continuously running data ingestion server that fetches live weather
and stock market data from public APIs and stores every record into MongoDB.

---

## How it works

```
server.py (runs forever)
    │
    ├── every 10 min → OpenWeatherMap API → MongoDB weather_current
    │
    └── every 60 min → Alpha Vantage API  → MongoDB stock_daily
```

Unlike a batch pipeline, this server runs continuously — fetching data,
storing it, waiting, then fetching again. Every fetch adds new documents
to MongoDB, building a growing time-series dataset.

---

## Stack

| Tool | Purpose |
|---|---|
| Python + schedule | Continuous server loop |
| requests | API calls |
| pymongo | MongoDB driver |
| MongoDB 7 | NoSQL document storage |
| Mongo Express | Visual DB browser (localhost:8081) |
| Docker | Container for MongoDB |

---

## Collections

### weather_current
One document per city per fetch (every 10 minutes).
Builds a time-series of weather observations.

```json
{
  "city": "New York",
  "country": "US",
  "recorded_at": "2026-03-27T14:00:00",
  "fetched_at": "2026-03-27T14:00:05",
  "temp_celsius": 23.03,
  "feels_like_celsius": 22.1,
  "humidity_pct": 65,
  "weather_main": "Smoke",
  "weather_desc": "smoke",
  "wind_speed_ms": 3.6
}
```

### stock_daily
One document per symbol per trading day.
Unique index on (symbol, trade_date) prevents duplicates.

```json
{
  "symbol": "AAPL",
  "trade_date": "2026-03-25",
  "open_price": 254.10,
  "high_price": 255.30,
  "low_price": 251.80,
  "close_price": 252.62,
  "volume": 45123400,
  "daily_change": -1.48,
  "daily_change_pct": -0.58
}
```

### pipeline_logs
Audit log of every job run.

```json
{
  "job": "weather_job",
  "status": "success",
  "rows_inserted": 5,
  "duration_sec": 3.2,
  "run_at": "2026-03-27T14:00:05"
}
```

---

## Setup

### 1. Install dependencies
```bash
conda activate de-pipeline
pip install -r requirements.txt
```

### 2. Configure API keys
```bash
cp .env.example .env
# Edit .env with your real API keys
```

### 3. Start MongoDB
```bash
docker compose up -d
```

### 4. Start the server
```bash
python server.py
```

You'll see:
```
╔══════════════════════════════════════════╗
║   API INGESTION SERVER STARTING          ║
╚══════════════════════════════════════════╝
Weather fetch interval : every 10 minutes
Stocks fetch interval  : every 60 minutes
Press Ctrl+C to stop

Running initial fetch on startup...
  New York: 23.03°C, smoke
  London: 8.3°C, overcast clouds
  ...
WEATHER DONE — inserted: 5, cities: 5
```

### 5. Browse your data visually
Open http://localhost:8081 in your browser — Mongo Express shows
all your collections and documents in a visual interface.

### 6. Check data from terminal
```bash
python check_db.py
```

---

## Adjusting fetch intervals

Edit these lines in `server.py`:
```python
WEATHER_INTERVAL_MINUTES = 10    # change to any number
STOCKS_INTERVAL_MINUTES  = 60    # change to any number
```

---

## Resume Bullet Point

> Built a continuously running data ingestion server in Python that fetches
> live weather data (5 cities, every 10 minutes) and stock market OHLCV
> (5 symbols, hourly) from public APIs, storing every record into MongoDB
> with deduplication, audit logging, and automatic restart on failure —
> containerized with Docker.
