"""
utils/stocks.py
───────────────
Fetches daily OHLCV stock data from Alpha Vantage API.
Stores into MongoDB stock_daily collection.
Deduplication handled by unique index on (symbol, trade_date).
"""

import os
import time
import logging
import requests
from datetime import datetime
from utils.db import get_db, insert_many_ignore_dupes

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"
API_KEY  = os.getenv("ALPHA_VANTAGE_API_KEY", "")

SYMBOLS = [
    {"symbol": "AAPL",  "name": "Apple Inc."},
    {"symbol": "MSFT",  "name": "Microsoft Corporation"},
    {"symbol": "GOOGL", "name": "Alphabet Inc."},
    {"symbol": "AMZN",  "name": "Amazon.com Inc."},
    {"symbol": "NVDA",  "name": "NVIDIA Corporation"},
]


def fetch_stock_for_symbol(symbol: str) -> list[dict]:
    """
    Fetch last 100 days of OHLCV for one symbol.
    Returns list of document dicts ready for MongoDB insert.
    """
    if not API_KEY:
        raise EnvironmentError(
            "ALPHA_VANTAGE_API_KEY not set. "
            "Get free key at alphavantage.co/support/#api-key"
        )

    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",   # last 100 trading days
        "apikey":     API_KEY,
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.error(f"Stock API error for {symbol}: {e}")
        return []

    if "Note" in raw:
        logger.warning(f"Alpha Vantage rate limit for {symbol}: {raw['Note']}")
        return []
    if "Error Message" in raw:
        logger.error(f"Alpha Vantage error for {symbol}: {raw['Error Message']}")
        return []

    time_series = raw.get("Time Series (Daily)", {})
    documents = []

    for date_str, ohlcv in time_series.items():
        open_p  = float(ohlcv["1. open"])
        close_p = float(ohlcv["4. close"])
        documents.append({
            "symbol":            symbol,
            "trade_date":        date_str,
            "trade_date_parsed": datetime.strptime(date_str, "%Y-%m-%d"),
            "fetched_at":        datetime.utcnow(),
            "open_price":        open_p,
            "high_price":        float(ohlcv["2. high"]),
            "low_price":         float(ohlcv["3. low"]),
            "close_price":       close_p,
            "volume":            int(ohlcv["5. volume"]),
            "daily_change":      round(close_p - open_p, 4),
            "daily_change_pct":  round(
                                     (close_p - open_p) / open_p * 100, 4
                                 ) if open_p else None,
        })

    logger.info(f"  {symbol}: {len(documents)} days fetched")
    return documents


def fetch_all_stocks() -> dict:
    """
    Fetch stock data for all symbols and insert into MongoDB.
    Returns summary dict.
    """
    db = get_db()
    collection = db.stock_daily

    all_documents = []
    errors = []

    for entry in SYMBOLS:
        docs = fetch_stock_for_symbol(entry["symbol"])
        if docs:
            all_documents.extend(docs)
        else:
            errors.append(entry["symbol"])
        time.sleep(13)   # Alpha Vantage free tier: ~5 req/min

    inserted = insert_many_ignore_dupes(collection, all_documents)

    return {
        "inserted": inserted,
        "symbols":  len(SYMBOLS),
        "errors":   errors,
    }
