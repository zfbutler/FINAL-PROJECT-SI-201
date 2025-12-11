"""
gather_weather_daily.py

Uses OpenWeather *History API* (hourly historical data) to populate a SQLite
database with daily precipitation totals for New York City and Chicago.

Endpoint docs:
    https://openweathermap.org/history

We use the hourly history endpoint:
    https://history.openweathermap.org/data/2.5/history/city
        ?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API key}

For each date, we:
    - request all hourly observations for that 24-hour window
    - sum rain["1h"] + snow["1h"] across all hours to get daily precip in mm
    - store one row per city per date in SQLite.

Tables:
    NYCWeather(id INTEGER PRIMARY KEY,
               date TEXT UNIQUE,   -- 'YYYY-MM-DD'
               precip_mm REAL)     -- total daily precipitation in mm

    ChicagoWeather(id INTEGER PRIMARY KEY,
                   date TEXT UNIQUE,
                   precip_mm REAL)

Main public function:
    populate_weather_for_dates(date_list, max_days=25) -> list[str]

    - date_list: list[datetime.date]
    - max_days: maximum number of *new* dates to fetch per run
    - returns: list of 'YYYY-MM-DD' strings for which rows were actually
               inserted (can be used by other API files to align dates).
"""

import sqlite3
from datetime import date, datetime, timedelta, timezone
import requests

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

DB_NAME = "date_weather_info.db"

# Put your real OpenWeather key here (or import from a separate file)
OPENWEATHER_KEY = "2ac6306c04d893afe4ac7939620af863"

# History API (hourly) base URL
BASE_URL = "https://history.openweathermap.org/data/2.5/history/city"

# Representative points for each city (central locations)
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


def day_unix_range(d: date) -> tuple[int, int]:
    """
    Given a calendar date, return (start_ts, end_ts) as Unix timestamps
    for the 24-hour period [00:00, 24:00) in UTC.

    History API expects 'start' and 'end' as unix time, UTC.
    """
    start_dt = datetime(d.year, d.month, d.day, 0, 0, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


# -------------------------------------------------------------------
# OPENWEATHER HISTORY API HELPERS
# -------------------------------------------------------------------

def fetch_history_for_day(lat: float, lon: float, d: date) -> dict:
    """
    Call OpenWeather History API (hourly) for a single latitude/longitude
    and calendar date.

    We request all hourly data between start=midnight and end=next-midnight
    in UTC, then later sum up rain/snow to get daily precipitation.
    """
    start_ts, end_ts = day_unix_range(d)

    params = {
        "lat": lat,
        "lon": lon,
        "type": "hour",
        "start": start_ts,
        "end": end_ts,
        "appid": OPENWEATHER_KEY,
        "units": "metric",  # optional; precip is still mm
    }

    resp = requests.get(BASE_URL, params=params)

    if resp.status_code != 200:
        print("OpenWeather History error for", d.isoformat())
        print("Final URL:", resp.url)
        print("Status code:", resp.status_code)
        print("Response body:", resp.text)
        resp.raise_for_status()

    return resp.json()


def precip_from_history_json(history_json: dict) -> float:
    """
    Given the JSON from the History API hourly endpoint, return the total
    precipitation for that day in millimetres.

    Response structure (simplified):

        {
            "message": "Count: 24",
            "cod": "200",
            "cnt": 24,
            "list": [
                {
                    "dt": 1573838400,
                    "rain": { "1h": 0.9 },
                    "snow": { "1h": 0.0 },
                    ...
                },
                ...
            ]
        }

    We sum rain["1h"] + snow["1h"] for each hourly entry in "list".
    """
    total_precip = 0.0

    for entry in history_json.get("list", []):
        rain = entry.get("rain", {})
        snow = entry.get("snow", {})

        # Rain volume for the last 1 hour, mm
        r = rain.get("1h", 0.0) or 0.0
        # Snow volume for the last 1 hour, mm
        s = snow.get("1h", 0.0) or 0.0

        try:
            total_precip += float(r) + float(s)
        except (TypeError, ValueError):
            # If something is weird in the data, skip that hour
            continue

    return total_precip


# -------------------------------------------------------------------
# MAIN POPULATION FUNCTION
# -------------------------------------------------------------------

def populate_weather_for_dates(date_list: list[date],
                               max_days: int = 25) -> list[str]:
    """
    For each date in date_list, fetch historical hourly weather for NYC
    and Chicago and insert daily precipitation totals into their tables.

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
        nyc_json = fetch_history_for_day(NYC_LAT, NYC_LON, d)
        nyc_precip = precip_from_history_json(nyc_json)

        cur.execute("""
            INSERT OR IGNORE INTO NYCWeather (id, date, precip_mm)
            VALUES (?, ?, ?)
        """, (day_id, date_str, nyc_precip))

        # --- CHICAGO ---
        chi_json = fetch_history_for_day(CHI_LAT, CHI_LON, d)
        chi_precip = precip_from_history_json(chi_json)

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
    start_date = date(2025, 9, 1)
    end_date = date(2025, 11, 20)
    monday_dates = mondays_between(start_date, end_date)

    # 3. Populate weather tables and get back the list of dates used
    used_dates = populate_weather_for_dates(monday_dates, max_days=25)

    print("Weather tables populated for these dates:")
    for ds in used_dates:
        print(ds)