"""
utils/db.py
───────────
MongoDB connection helper using pymongo.
All jobs use get_db() to get a database handle.

Local MongoDB runs on mongodb://localhost:27017
Collections:
  - weather_current   — live weather snapshots
  - stock_daily       — daily OHLCV per symbol
  - pipeline_logs     — run history + audit trail
"""

import os
import logging
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "pipeline_db")

_client = None


def get_client() -> MongoClient:
    """Return a singleton MongoClient — reuses connection across calls."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        try:
            _client.admin.command("ping")
            logger.info(f"Connected to MongoDB at {MONGO_URI}")
        except ConnectionFailure as e:
            logger.error(
                f"Cannot connect to MongoDB at {MONGO_URI}. "
                f"Is MongoDB running? Error: {e}"
            )
            raise
    return _client


def get_db():
    """Return the pipeline database handle."""
    return get_client()[DB_NAME]


def ensure_indexes():
    """
    Create indexes on startup for fast queries and dedup.
    Safe to call multiple times — MongoDB skips existing indexes.
    """
    db = get_db()

    # Weather: unique on (city, recorded_at) — no duplicate snapshots
    db.weather_current.create_index(
        [("city", ASCENDING), ("recorded_at", ASCENDING)],
        unique=True,
        name="city_time_unique"
    )
    db.weather_current.create_index(
        [("recorded_at", DESCENDING)],
        name="recorded_at_desc"
    )

    # Stocks: unique on (symbol, trade_date) — no duplicate daily records
    db.stock_daily.create_index(
        [("symbol", ASCENDING), ("trade_date", ASCENDING)],
        unique=True,
        name="symbol_date_unique"
    )
    db.stock_daily.create_index(
        [("trade_date", DESCENDING)],
        name="trade_date_desc"
    )

    # Logs: index on job + timestamp
    db.pipeline_logs.create_index(
        [("job", ASCENDING), ("run_at", DESCENDING)],
        name="job_time"
    )

    logger.info("MongoDB indexes ensured")


def insert_many_ignore_dupes(collection, documents: list) -> int:
    """
    Bulk insert documents, silently ignoring duplicates.
    Returns the number of newly inserted documents.
    """
    if not documents:
        return 0

    inserted = 0
    try:
        result = collection.insert_many(documents, ordered=False)
        inserted = len(result.inserted_ids)
    except Exception as e:
        # BulkWriteError contains partial success info
        if hasattr(e, "details"):
            inserted = e.details.get("nInserted", 0)
        else:
            logger.warning(f"Insert warning (likely duplicates): {e}")
    return inserted


def log_run(job: str, status: str, rows_inserted: int = 0,
            error: str = None, duration: float = None):
    """Write a run record to pipeline_logs collection."""
    try:
        db = get_db()
        db.pipeline_logs.insert_one({
            "job":           job,
            "status":        status,
            "rows_inserted": rows_inserted,
            "error":         error,
            "duration_sec":  duration,
            "run_at":        datetime.utcnow(),
        })
    except Exception as e:
        logger.warning(f"Could not write to pipeline_logs: {e}")
