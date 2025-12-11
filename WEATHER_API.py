# Name: Andrew Kagan
# Student ID: 61021214
# Email: aakagan
# List who you have worked with on this file: Assistance from Zeke Butler
# List any AI tool (e.g. ChatGPT, GitHub Copilot): ChatGPT for help with the general structure of the program as well as 
# figuring out how to best implement the api and apply it a way that is cohesive with the data we get out of the other api

import sqlite3
from datetime import date, datetime, timedelta, timezone
import requests

DB_NAME = "weather_crashes.db"

OPENWEATHER_KEY = "2ac6306c04d893afe4ac7939620af863"

BASE_URL = "https://history.openweathermap.org/data/2.5/history/city"

NYC_LAT, NYC_LON = 40.7812, -73.9665   # Central Park Area NYC
CHI_LAT, CHI_LON = 41.8781, -87.6298   # Downtown Chicago

def init_db() -> None:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS DayIndex (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE          -- 'YYYY-MM-DD'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS NYCWeather (
            id        INTEGER PRIMARY KEY,  -- references DayIndex.id
            precip_mm REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ChicagoWeather (
            id        INTEGER PRIMARY KEY,  -- references DayIndex.id
            precip_mm REAL
        )
    """)

    conn.commit()
    conn.close()

def days_between(start: date, end: date) -> list[date]:
    days: list[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def day_unix_range(d: date) -> tuple[int, int]:
    start_dt = datetime(d.year, d.month, d.day, 0, 0, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp()), int(end_dt.timestamp())

def fetch_history_for_day(lat: float, lon: float, d: date) -> dict:
    start_ts, end_ts = day_unix_range(d)

    params = {
        "lat": lat,
        "lon": lon,
        "type": "hour",
        "start": start_ts,
        "end": end_ts,
        "appid": OPENWEATHER_KEY,
        "units": "metric",  
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
    total_precip = 0.0

    for entry in history_json.get("list", []):
        rain = entry.get("rain", {})
        snow = entry.get("snow", {})

        r = rain.get("1h", 0.0) or 0.0
        s = snow.get("1h", 0.0) or 0.0

        try:
            total_precip += float(r) + float(s)
        except (TypeError, ValueError):
            continue

    return total_precip

def populate_weather_for_dates(date_list: list[date],
                               max_days: int = 25) -> list[str]:
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT date FROM DayIndex")
    existing_dates = {row[0] for row in cur.fetchall()}

    processed = 0
    processed_dates: list[str] = []

    for d in sorted(date_list):
        date_str = d.isoformat()  # 'YYYY-MM-DD'

        if date_str in existing_dates:
            continue

        if processed >= max_days:
            break

        cur.execute("INSERT OR IGNORE INTO DayIndex (date) VALUES (?)", (date_str,))
        cur.execute("SELECT id FROM DayIndex WHERE date = ?", (date_str,))
        row = cur.fetchone()
        if row is None:
            continue
        shared_id = row[0]

        nyc_json = fetch_history_for_day(NYC_LAT, NYC_LON, d)
        nyc_precip = precip_from_history_json(nyc_json)

        chi_json = fetch_history_for_day(CHI_LAT, CHI_LON, d)
        chi_precip = precip_from_history_json(chi_json)

        cur.execute("""
            INSERT OR REPLACE INTO NYCWeather (id, precip_mm)
            VALUES (?, ?)
        """, (shared_id, nyc_precip))

        cur.execute("""
            INSERT OR REPLACE INTO ChicagoWeather (id, precip_mm)
            VALUES (?, ?)
        """, (shared_id, chi_precip))

        processed += 1
        processed_dates.append(date_str)

    conn.commit()
    conn.close()

    return processed_dates


def clear_weather_tables() -> None:
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS DayIndex (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS NYCWeather (
            id        INTEGER PRIMARY KEY,
            precip_mm REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ChicagoWeather (
            id        INTEGER PRIMARY KEY,
            precip_mm REAL
        )
    """)

    cur.execute("DELETE FROM DayIndex")
    cur.execute("DELETE FROM NYCWeather")
    cur.execute("DELETE FROM ChicagoWeather")

    conn.commit()
    conn.close()
    print("All rows cleared from DayIndex, NYCWeather, and ChicagoWeather.")


def drop_legacy_weather_tables() -> None:
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS WeatherDaily")
    cur.execute("DROP TABLE IF EXISTS DailyWeather")

    conn.commit()
    conn.close()
    print("Legacy weather tables (WeatherDaily/DailyWeather) dropped if they existed.")

def main() -> None:
    # drop_legacy_weather_tables()

    init_db()

    # clear_weather_tables()

    today = date.today()
    end_date = today - timedelta(days=1)          
    start_date = end_date - timedelta(days=99)    

    all_dates = days_between(start_date, end_date)

    used_dates = populate_weather_for_dates(all_dates, max_days=25)

    print("Weather tables populated for these dates:")
    for ds in used_dates:
        print(ds)

if __name__ == "__main__":
    main()
    