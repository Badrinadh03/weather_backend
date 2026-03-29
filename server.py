"""
server.py
─────────
Data ingestion server running on uvicorn + FastAPI.
Fetches weather + stock data continuously in the background
and stores every record into MongoDB.

Run:  python server.py
      OR: uvicorn server:app --host 0.0.0.0 --port 9000 --reload

Endpoints:
  GET /          — server status + next run times
  GET /health    — health check
  GET /api/weather/latest, /api/weather/history — dashboard (MongoDB reads)
  GET /api/stocks/latest, /api/stocks/history
  GET /api/status — pipeline stats + recent runs
  GET /trigger/weather — manually trigger a weather fetch
  GET /trigger/stocks  — manually trigger a stock fetch
"""

import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import uvicorn

load_dotenv()
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/server.log"),
    ]
)
logger = logging.getLogger(__name__)

from utils.weather import fetch_all_weather
from utils.stocks  import fetch_all_stocks
from utils.db      import get_db, log_run


def _serialize_doc(doc: dict) -> dict:
    """MongoDB document → JSON-safe dict (datetimes → ISO strings)."""
    out = {}
    for k, v in doc.items():
        if k == "_id":
            continue
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out

# ── Intervals ──────────────────────────────────────────────────────────────────
WEATHER_INTERVAL_SECONDS = 10 * 60    # 10 minutes
STOCKS_INTERVAL_SECONDS  = 60 * 60   # 60 minutes

# ── Shared state — track last run times ───────────────────────────────────────
state = {
    "started_at":        None,
    "weather_last_run":  None,
    "weather_next_run":  None,
    "stocks_last_run":   None,
    "stocks_next_run":   None,
    "weather_total":     0,
    "stocks_total":      0,
}


# ── Job functions ──────────────────────────────────────────────────────────────
def run_weather_job():
    logger.info("=" * 50)
    logger.info("WEATHER JOB STARTED")
    start = time.time()
    try:
        result   = fetch_all_weather()
        duration = round(time.time() - start, 2)
        logger.info(
            f"WEATHER DONE — inserted: {result['inserted']}, "
            f"cities: {result['cities']}, "
            f"errors: {result['errors']}, "
            f"duration: {duration}s"
        )
        log_run("weather_job", "success", result["inserted"], duration=duration)
        state["weather_last_run"] = datetime.utcnow().isoformat()
        state["weather_next_run"] = (
            datetime.utcnow() + timedelta(seconds=WEATHER_INTERVAL_SECONDS)
        ).isoformat()
        state["weather_total"] += result["inserted"]
    except Exception as e:
        duration = round(time.time() - start, 2)
        logger.error(f"WEATHER JOB FAILED: {e}")
        log_run("weather_job", "failed", 0, error=str(e), duration=duration)


def run_stocks_job():
    logger.info("=" * 50)
    logger.info("STOCKS JOB STARTED")
    start = time.time()
    try:
        result   = fetch_all_stocks()
        duration = round(time.time() - start, 2)
        logger.info(
            f"STOCKS DONE — inserted: {result['inserted']}, "
            f"symbols: {result['symbols']}, "
            f"errors: {result['errors']}, "
            f"duration: {duration}s"
        )
        log_run("stocks_job", "success", result["inserted"], duration=duration)
        state["stocks_last_run"] = datetime.utcnow().isoformat()
        state["stocks_next_run"] = (
            datetime.utcnow() + timedelta(seconds=STOCKS_INTERVAL_SECONDS)
        ).isoformat()
        state["stocks_total"] += result["inserted"]
    except Exception as e:
        duration = round(time.time() - start, 2)
        logger.error(f"STOCKS JOB FAILED: {e}")
        log_run("stocks_job", "failed", 0, error=str(e), duration=duration)


# ── Background loop tasks ──────────────────────────────────────────────────────
async def weather_loop():
    """Fetch weather immediately, then every WEATHER_INTERVAL_SECONDS."""
    logger.info("Weather loop started")
    run_weather_job()           # run immediately on startup
    while True:
        await asyncio.sleep(WEATHER_INTERVAL_SECONDS)
        run_weather_job()


async def stocks_loop():
    """Fetch stocks immediately, then every STOCKS_INTERVAL_SECONDS."""
    logger.info("Stocks loop started")
    run_stocks_job()            # run immediately on startup
    while True:
        await asyncio.sleep(STOCKS_INTERVAL_SECONDS)
        run_stocks_job()


# ── FastAPI lifespan — starts background tasks when uvicorn boots ──────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   INGESTION SERVER STARTING (uvicorn)    ║")
    logger.info("╚══════════════════════════════════════════╝")
    logger.info(f"Weather interval : every {WEATHER_INTERVAL_SECONDS // 60} minutes")
    logger.info(f"Stocks interval  : every {STOCKS_INTERVAL_SECONDS  // 60} minutes")

    state["started_at"] = datetime.utcnow().isoformat()

    # Start both loops as background tasks
    weather_task = asyncio.create_task(weather_loop())
    stocks_task  = asyncio.create_task(stocks_loop())

    yield   # server is running — handle requests here

    # Shutdown — cancel background tasks cleanly
    weather_task.cancel()
    stocks_task.cancel()
    logger.info("Server shutting down — background tasks cancelled")


# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Data Ingestion Server",
    description="Continuously fetches weather + stock data into MongoDB",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    """Server status and next scheduled run times."""
    return {
        "status":           "running",
        "started_at":       state["started_at"],
        "weather": {
            "last_run":     state["weather_last_run"],
            "next_run":     state["weather_next_run"],
            "total_inserted": state["weather_total"],
            "interval_min": WEATHER_INTERVAL_SECONDS // 60,
        },
        "stocks": {
            "last_run":     state["stocks_last_run"],
            "next_run":     state["stocks_next_run"],
            "total_inserted": state["stocks_total"],
            "interval_min": STOCKS_INTERVAL_SECONDS // 60,
        },
    }


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# ── Dashboard API (proxied from React as /api/*) ───────────────────────────────
@app.get("/api/weather/latest")
def api_weather_latest():
    """Latest weather snapshot per city — shape expected by the frontend."""
    db = get_db()
    pipeline = [
        {"$sort": {"recorded_at": -1}},
        {"$group": {
            "_id": "$city",
            "city":               {"$first": "$city"},
            "country":            {"$first": "$country"},
            "temp_celsius":       {"$first": "$temp_celsius"},
            "feels_like_celsius": {"$first": "$feels_like_celsius"},
            "humidity_pct":       {"$first": "$humidity_pct"},
            "wind_speed_ms":      {"$first": "$wind_speed_ms"},
            "weather_main":       {"$first": "$weather_main"},
            "weather_desc":       {"$first": "$weather_desc"},
            "cloud_cover_pct":    {"$first": "$cloud_cover_pct"},
            "recorded_at":        {"$first": "$recorded_at"},
        }},
        {"$sort": {"city": 1}},
    ]
    rows = [_serialize_doc(r) for r in db.weather_current.aggregate(pipeline)]
    return {"data": rows}


@app.get("/api/weather/history")
def api_weather_history(
    city: str = Query(..., description="City name"),
    hours: int = Query(24, ge=1, le=168),
):
    since = datetime.utcnow() - timedelta(hours=hours)
    db = get_db()
    docs = list(db.weather_current.find(
        {"city": city, "recorded_at": {"$gte": since}},
        {"_id": 0, "city": 1, "temp_celsius": 1,
         "humidity_pct": 1, "recorded_at": 1},
    ).sort("recorded_at", 1))
    return {"data": [_serialize_doc(d) for d in docs]}


@app.get("/api/stocks/latest")
def api_stocks_latest():
    db = get_db()
    pipeline = [
        {"$sort": {"trade_date": -1}},
        {"$group": {
            "_id": "$symbol",
            "symbol":           {"$first": "$symbol"},
            "trade_date":       {"$first": "$trade_date"},
            "open_price":       {"$first": "$open_price"},
            "high_price":       {"$first": "$high_price"},
            "low_price":        {"$first": "$low_price"},
            "close_price":      {"$first": "$close_price"},
            "volume":           {"$first": "$volume"},
            "daily_change":     {"$first": "$daily_change"},
            "daily_change_pct": {"$first": "$daily_change_pct"},
        }},
        {"$sort": {"symbol": 1}},
    ]
    rows = [_serialize_doc(r) for r in db.stock_daily.aggregate(pipeline)]
    return {"data": rows}


@app.get("/api/stocks/history")
def api_stocks_history(
    symbol: str = Query(..., description="Ticker symbol"),
    days: int = Query(30, ge=1, le=365),
):
    db = get_db()
    docs = list(db.stock_daily.find(
        {"symbol": symbol.upper()},
        {"_id": 0, "symbol": 1, "trade_date": 1,
         "close_price": 1, "volume": 1, "daily_change_pct": 1},
    ).sort("trade_date", -1).limit(days))
    docs.reverse()
    return {"data": [_serialize_doc(d) for d in docs]}


@app.get("/api/status")
def api_pipeline_status():
    db = get_db()
    weather_count = db.weather_current.count_documents({})
    stock_count = db.stock_daily.count_documents({})
    total_runs = db.pipeline_logs.count_documents({})
    success_count = db.pipeline_logs.count_documents({"status": "success"})
    success_rate = round(100 * success_count / total_runs) if total_runs else 0

    last_weather = db.weather_current.find_one(
        {}, {"recorded_at": 1},
        sort=[("recorded_at", -1)],
    )
    last_weather_run = None
    if last_weather and last_weather.get("recorded_at"):
        ra = last_weather["recorded_at"]
        last_weather_run = ra.isoformat() if isinstance(ra, datetime) else str(ra)

    runs = list(db.pipeline_logs.find().sort("run_at", -1).limit(20))
    recent_runs = []
    for r in runs:
        ra = r.get("run_at")
        recent_runs.append({
            "job":           r.get("job"),
            "status":        r.get("status"),
            "rows_inserted": r.get("rows_inserted"),
            "duration_sec":  r.get("duration_sec"),
            "run_at":        ra.isoformat() if isinstance(ra, datetime) else ra,
            "error":         r.get("error"),
        })

    return {
        "stats": {
            "total_weather_records": weather_count,
            "total_stock_records":   stock_count,
            "total_runs":            total_runs,
            "success_rate":          success_rate,
            "last_weather_run":      last_weather_run,
        },
        "recent_runs": recent_runs,
    }


@app.get("/trigger/weather")
def trigger_weather():
    """Manually trigger a weather fetch right now."""
    logger.info("Manual weather trigger via API")
    run_weather_job()
    return {"status": "triggered", "time": datetime.utcnow().isoformat()}


@app.get("/trigger/stocks")
def trigger_stocks():
    """Manually trigger a stock fetch right now."""
    logger.info("Manual stocks trigger via API")
    run_stocks_job()
    return {"status": "triggered", "time": datetime.utcnow().isoformat()}


# ── Run with uvicorn ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=False,    # reload=True breaks background tasks
        log_level="info",
    )
