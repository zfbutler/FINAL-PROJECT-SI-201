"""
gather_weather_daily.py

Uses OpenWeather One Call 3.0 *Daily Aggregation* API to populate a SQLite
database with daily precipitation totals for New York City and Chicago.

- Two tables: NYCWeather and ChicagoWeather
- Columns in each table:
    id           INTEGER  (YYYYMMDD, shared across tables for the same date)
    date         TEXT     ('YYYY-MM-DD')
    precip_mm    REAL     (total liquid-equivalent precipitation for that day)

Main function:
    populate_weather_for_dates(date_list, max_days=25) -> list[str]

    - date_list: list of datetime.date objects
    - max_days:  maximum number of *new* dates to fetch per run
    - returns:   list of 'YYYY-MM-DD' strings for dates actually inserted

You can import `populate_weather_for_dates` in other files and use the
returned date strings to drive your crash-data API calls.
"""

import sqlite3
from datetime import date, datetime, timedelta
import requests

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

DB_NAME = "project.db"

# Put your real OpenWeather key here or import from a separate file
OPENWEATHER_KEY = "YOUR_REAL_KEY_HERE"

# Daily aggregation endpoint (One Call 3.0)
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall/day_summary"

# Representative points for each city (central locations)
# These are just good "city weather" points – OpenWeather aggregates
# from their own grid internally.
NYC_LAT, NYC_LON = 40.7812, -73.9665   # Central Park area, NYC
CHI_LAT, CHI_LON = 41.8781, -87.6298   # Downtown Chicago


# -------------------------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------------------------

def init_db() -> None:
    """
    Create the two weather tables if they don't exist.
    Each row represents one city's weather for one date.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS NYCWeather (
            id INTEGER PRIMARY KEY,
            date TEXT UNIQUE,      -- 'YYYY-MM-DD'
            precip_mm REAL         -- total daily precipitation in mm
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ChicagoWeather (
            id INTEGER PRIMARY KEY,
            date TEXT UNIQUE,
            precip_mm REAL
        )
    """)

    conn.commit()
    conn.close()


# -------------------------------------------------------------------
# DATE / ID HELPERS
# -------------------------------------------------------------------

def date_to_id(d: date) -> int:
    """
    Turn a date into a unique integer id, e.g.
        2023-10-02 -> 20231002
    This id will be the same for NYC and Chicago for the same date.
    """
    return int(d.strftime("%Y%m%d"))


def mondays_between(start: date, end: date) -> list[date]:
    """
    Example helper: return all Mondays between start and end (inclusive).
    You can ignore this if you want to supply your own date list.
    """
    days: list[date] = []
    d = start
    while d <= end:
        if d.weekday() == 0:  # Monday == 0
            days.append(d)
        d += timedelta(days=1)
    return days


# -------------------------------------------------------------------
# OPENWEATHER DAILY AGGREGATION HELPERS
# -------------------------------------------------------------------

def fetch_daily_weather(lat: float, lon: float, d: date) -> dict:
    """
    Call OpenWeather One Call 3.0 Daily Aggregation endpoint for a single
    latitude/longitude and calendar date.

    The API expects date as 'YYYY-MM-DD'.
    """
    date_str = d.isoformat()  # 'YYYY-MM-DD'

    params = {
        "lat": lat,
        "lon": lon,
        "date": date_str,
        "appid": OPENWEATHER_KEY,
        "units": "metric"  # affects temp/wind; precip is still mm
    }

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.json()


def precip_from_daily_json(daily_json: dict) -> float:
    """
    Given the JSON from the Daily Aggregation endpoint, return the total
    precipitation for that day in millimetres.

    Example response snippet:

        "precipitation": {
            "total": 0
        }

    If precipitation.total is missing, we treat it as 0.0.
    """
    precip_obj = daily_json.get("precipitation", {})
    total = precip_obj.get("total", 0.0)
    try:
        return float(total)
    except (TypeError, ValueError):
        return 0.0


# -------------------------------------------------------------------
# MAIN POPULATION FUNCTION
# -------------------------------------------------------------------

def populate_weather_for_dates(date_list: list[date],
                               max_days: int = 25) -> list[str]:
    """
    For each date in date_list, fetch daily aggregated weather for NYC
    and Chicago and insert into their respective tables.

    - id = YYYYMMDD, shared across both tables for that date
    - precip_mm = total precipitation for that date in that city (mm)

    We:
      * skip dates that are already present in NYCWeather
      * process at most max_days *new* dates per run
      * always fetch weather for both cities when we process a date

    Returns:
        processed_dates: list of 'YYYY-MM-DD' strings for dates where we
                         actually inserted weather rows. This list can be
                         passed to other API files so they only gather
                         crash data for matching dates.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Find which dates we already have for NYC (assume Chicago is in sync)
    cur.execute("SELECT date FROM NYCWeather")
    existing_dates = {row[0] for row in cur.fetchall()}

    processed = 0
    processed_dates: list[str] = []

    for d in sorted(date_list):
        date_str = d.isoformat()  # 'YYYY-MM-DD'

        # If NYC already has this date, assume we don't need to redo it
        if date_str in existing_dates:
            continue

        if processed >= max_days:
            break

        day_id = date_to_id(d)

        # --- NYC ---
        nyc_json = fetch_daily_weather(NYC_LAT, NYC_LON, d)
        nyc_precip = precip_from_daily_json(nyc_json)

        cur.execute("""
            INSERT OR IGNORE INTO NYCWeather (id, date, precip_mm)
            VALUES (?, ?, ?)
        """, (day_id, date_str, nyc_precip))

        # --- CHICAGO ---
        chi_json = fetch_daily_weather(CHI_LAT, CHI_LON, d)
        chi_precip = precip_from_daily_json(chi_json)

        cur.execute("""
            INSERT OR IGNORE INTO ChicagoWeather (id, date, precip_mm)
            VALUES (?, ?, ?)
        """, (day_id, date_str, chi_precip))

        processed += 1
        processed_dates.append(date_str)

    conn.commit()
    conn.close()

    return processed_dates


# -------------------------------------------------------------------
# EXAMPLE USAGE WHEN RUN DIRECTLY
# -------------------------------------------------------------------

if __name__ == "__main__":
    # 1. Ensure tables exist
    init_db()

    # 2. Choose the dates you care about
    #    (here, all Mondays between Oct 1 and Dec 31, 2023)
    start_date = date(2023, 10, 1)
    end_date = date(2023, 12, 31)
    monday_dates = mondays_between(start_date, end_date)

    # 3. Populate weather tables and get back the list of dates used
    used_dates = populate_weather_for_dates(monday_dates, max_days=25)

    print("Weather tables populated for these dates:")
    for ds in used_dates:
        print(ds)