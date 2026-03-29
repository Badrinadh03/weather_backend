"""
api.py
──────
Flask REST API that reads from MongoDB and serves data to the frontend.
Runs on http://localhost:5001

Endpoints:
  GET /api/weather          — latest weather for all cities
  GET /api/stocks           — latest stock prices for all symbols
  GET /api/stocks/<symbol>  — historical data for one symbol
  GET /api/logs             — pipeline run history
  GET /api/stats            — summary stats (total docs, last run time)
"""

from flask import Flask, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from utils.db import get_db
from datetime import datetime, timedelta
import logging

load_dotenv()

app = Flask(__name__)
CORS(app)   # allow frontend to call this API

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def serialize(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable dict."""
    doc.pop("_id", None)
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.strftime("%Y-%m-%d %H:%M:%S")
    return doc


# ── Weather endpoints ──────────────────────────────────────────────────────────

@app.route("/api/weather")
def get_weather():
    """Latest weather snapshot for each city."""
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
    results = list(db.weather_current.aggregate(pipeline))
    return jsonify([serialize(r) for r in results])


@app.route("/api/weather/history/<city>")
def get_weather_history(city):
    """Last 24 hours of weather snapshots for one city."""
    db = get_db()
    since = datetime.utcnow() - timedelta(hours=24)
    docs = list(db.weather_current.find(
        {"city": city, "recorded_at": {"$gte": since}},
        {"_id": 0, "city": 1, "temp_celsius": 1,
         "humidity_pct": 1, "recorded_at": 1}
    ).sort("recorded_at", 1))
    return jsonify([serialize(d) for d in docs])


# ── Stock endpoints ────────────────────────────────────────────────────────────

@app.route("/api/stocks")
def get_stocks():
    """Latest price for each symbol."""
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
    results = list(db.stock_daily.aggregate(pipeline))
    return jsonify([serialize(r) for r in results])


@app.route("/api/stocks/<symbol>")
def get_stock_history(symbol):
    """Last 30 days of OHLCV for one symbol."""
    db = get_db()
    docs = list(db.stock_daily.find(
        {"symbol": symbol.upper()},
        {"_id": 0, "symbol": 1, "trade_date": 1,
         "close_price": 1, "volume": 1, "daily_change_pct": 1}
    ).sort("trade_date", -1).limit(30))
    docs.reverse()   # oldest → newest for charting
    return jsonify([serialize(d) for d in docs])


# ── Pipeline logs ──────────────────────────────────────────────────────────────

@app.route("/api/logs")
def get_logs():
    """Last 20 pipeline run records."""
    db = get_db()
    docs = list(db.pipeline_logs.find(
        {}, {"_id": 0}
    ).sort("run_at", -1).limit(20))
    return jsonify([serialize(d) for d in docs])


# ── Stats ──────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def get_stats():
    """Summary stats for the dashboard header."""
    db = get_db()

    weather_count = db.weather_current.count_documents({})
    stock_count   = db.stock_daily.count_documents({})
    log_count     = db.pipeline_logs.count_documents({})

    last_weather = db.weather_current.find_one(
        {}, {"recorded_at": 1, "_id": 0},
        sort=[("recorded_at", -1)]
    )
    last_stock = db.stock_daily.find_one(
        {}, {"fetched_at": 1, "_id": 0},
        sort=[("fetched_at", -1)]
    )

    return jsonify({
        "weather_docs":    weather_count,
        "stock_docs":      stock_count,
        "pipeline_runs":   log_count,
        "last_weather_at": last_weather["recorded_at"].strftime("%Y-%m-%d %H:%M:%S")
                           if last_weather else None,
        "last_stock_at":   last_stock["fetched_at"].strftime("%Y-%m-%d %H:%M:%S")
                           if last_stock else None,
    })


if __name__ == "__main__":
    logger.info("API server starting on http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=True)
