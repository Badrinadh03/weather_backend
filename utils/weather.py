"""
utils/weather.py
────────────────
Fetches current weather for all tracked cities from OpenWeatherMap.
Stores every fetch as a new document in MongoDB weather_current collection.

Each run = new snapshot (no overwrite) so you build a time-series
of weather observations over time.
"""

import os
import time
import logging
import requests
from datetime import datetime, timezone
from utils.db import get_db, insert_many_ignore_dupes

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")

CITIES = [
    {"city": "New York",  "country": "US"},
    {"city": "London",    "country": "GB"},
    {"city": "Tokyo",     "country": "JP"},
    {"city": "Sydney",    "country": "AU"},
    {"city": "Berlin",    "country": "DE"},
]


def fetch_weather_for_city(city: str, country: str) -> dict | None:
    """Call OWM API for one city. Returns document dict or None on error."""
    if not API_KEY:
        raise EnvironmentError(
            "OPENWEATHER_API_KEY not set. "
            "Get free key at openweathermap.org/api"
        )

    params = {
        "q":     f"{city},{country}",
        "appid": API_KEY,
        "units": "metric",
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.error(f"Weather API error for {city}: {e}")
        return None

    return {
        "city":               city,
        "country":            country,
        "recorded_at":        datetime.fromtimestamp(
                                  raw["dt"], tz=timezone.utc
                              ).replace(tzinfo=None),
        "fetched_at":         datetime.utcnow(),     # exact time we fetched
        "temp_celsius":       raw["main"]["temp"],
        "feels_like_celsius": raw["main"]["feels_like"],
        "temp_min_celsius":   raw["main"]["temp_min"],
        "temp_max_celsius":   raw["main"]["temp_max"],
        "humidity_pct":       raw["main"]["humidity"],
        "pressure_hpa":       raw["main"]["pressure"],
        "visibility_m":       raw.get("visibility"),
        "wind_speed_ms":      raw.get("wind", {}).get("speed"),
        "wind_direction_deg": raw.get("wind", {}).get("deg"),
        "cloud_cover_pct":    raw.get("clouds", {}).get("all"),
        "weather_main":       raw["weather"][0]["main"],
        "weather_desc":       raw["weather"][0]["description"],
    }


def fetch_all_weather() -> dict:
    """
    Fetch weather for all cities and insert into MongoDB.
    Returns summary dict with inserted count and errors.
    """
    db = get_db()
    collection = db.weather_current

    documents = []
    errors = []

    for entry in CITIES:
        doc = fetch_weather_for_city(entry["city"], entry["country"])
        if doc:
            documents.append(doc)
            logger.info(
                f"  {entry['city']}: {doc['temp_celsius']}°C, {doc['weather_desc']}"
            )
        else:
            errors.append(entry["city"])
        time.sleep(0.5)   # stay within free tier rate limit

    inserted = insert_many_ignore_dupes(collection, documents)

    return {
        "inserted": inserted,
        "cities":   len(CITIES),
        "errors":   errors,
    }
