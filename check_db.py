"""
check_db.py
───────────
Inspect what's stored in MongoDB.
Run: python check_db.py

Shows:
  - Collection counts
  - Latest weather records
  - Latest stock records
  - Pipeline run history
"""

import logging
from datetime import datetime
from utils.db import get_db

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def check():
    db = get_db()

    print("\n" + "="*55)
    print("  MongoDB — pipeline_db")
    print("="*55)

    # ── Collection counts ──────────────────────────────────────
    collections = ["weather_current", "stock_daily", "pipeline_logs"]
    for col in collections:
        count = db[col].count_documents({})
        print(f"  {col:<20} {count:>6,} documents")

    # ── Latest weather ─────────────────────────────────────────
    print("\n── Latest weather records ──────────────────────────")
    weather = db.weather_current.find(
        {}, {"city": 1, "temp_celsius": 1, "weather_desc": 1,
             "recorded_at": 1, "_id": 0}
    ).sort("recorded_at", -1).limit(5)

    for w in weather:
        print(
            f"  {w['city']:<12} "
            f"{w['temp_celsius']:>6.1f}°C  "
            f"{w['weather_desc']:<25} "
            f"@ {w['recorded_at']}"
        )

    # ── Latest stocks ──────────────────────────────────────────
    print("\n── Latest stock records ────────────────────────────")
    stocks = db.stock_daily.find(
        {}, {"symbol": 1, "trade_date": 1, "close_price": 1,
             "daily_change_pct": 1, "_id": 0}
    ).sort("trade_date_parsed", -1).limit(10)

    for s in stocks:
        chg = s.get("daily_change_pct", 0) or 0
        arrow = "▲" if chg >= 0 else "▼"
        print(
            f"  {s['symbol']:<6} "
            f"{s['trade_date']}  "
            f"close: ${s['close_price']:>8.2f}  "
            f"{arrow} {abs(chg):.2f}%"
        )

    # ── Pipeline logs ──────────────────────────────────────────
    print("\n── Pipeline run history ────────────────────────────")
    logs = db.pipeline_logs.find(
        {}, {"job": 1, "status": 1, "rows_inserted": 1,
             "duration_sec": 1, "run_at": 1, "_id": 0}
    ).sort("run_at", -1).limit(10)

    for log in logs:
        status_icon = "✓" if log["status"] == "success" else "✗"
        print(
            f"  {status_icon} {log['job']:<15} "
            f"status: {log['status']:<8} "
            f"rows: {log.get('rows_inserted', 0):>4}  "
            f"@ {log['run_at'].strftime('%Y-%m-%d %H:%M:%S')}"
        )

    print("="*55 + "\n")


if __name__ == "__main__":
    check()
