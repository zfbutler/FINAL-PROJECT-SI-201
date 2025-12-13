# Name: Isaac Abrams
# Student ID: 9683 2526
# Email: isaacab
# List who you have worked with on this file: Assistance from Zeke Butler
# List any AI tool (e.g. ChatGPT, GitHub Copilot): ChatGPT for help with the datetime library as well as the parameters for accessing the API

import sqlite3
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

def clear_chi_tables():
    """Drop all tables in the weather_crashes.db database."""
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    tables = ["chi_injury_stats", "chi_date_info", "chi_crash_data"]
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table}")

    conn.commit()
    conn.close()
    print("All tables cleared.")

def create_chi_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()


    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS chi_crash_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nycweather_id INTEGER UNIQUE,
            total_crashes INTEGER,
            total_injuries INTEGER,
            total_fatalities INTEGER,
            FOREIGN KEY(nycweather_id) REFERENCES NYCWeather(id)
            
        )
    """)

    conn.commit()
    conn.close()


API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
API_KEY = "Ftd7Qvrkm1OCOu5lR8kkBJSnb"

def fetch_chi_crashes(limit=1000, offset=0, where=None, select=None, order=None):
    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    if order:
        params["$order"] = order
    
    headers = {"X-App-Token": API_KEY}
    try:    
        response = requests.get(API_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print("Access is not granted!")
    return []

def collect_crash_data(api_data):
    total_crashes = 0
    total_injuries = 0 
    total_fatalities = 0

    for entry in api_data:
        try:
            #api date-time data excluding milliseconds
            pulled_crash_dt = entry.get("crash_date").split(".")[0]
            if not pulled_crash_dt:
                continue
            dt = datetime.strptime(pulled_crash_dt, "%Y-%m-%dT%H:%M:%S")

            #filter for only daytime hours
            if not (6 < dt.hour < 18):
                continue

            #increment crash count
            total_crashes += 1
            
            try:
                total_injuries += int(entry.get("injuries_total", 0))
            except:
                pass
            

            try:
                total_fatalities += int(entry.get("injuries_fatal", 0))
            except:
                pass

        except:
            continue

    return total_crashes, total_injuries, total_fatalities

#inserts data into the chi_data_data and returns a 
def insert_chi_crashdata(nycweather_id, total_crashes, total_injuries, total_fatalities):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO chi_crash_data (nycweather_id, total_crashes, total_injuries, total_fatalities)
        VALUES (?, ?, ?, ?)
    """, (nycweather_id, total_crashes, total_injuries, total_fatalities))

    conn.commit()
    conn.close()
    return

#helper function to ensure repeat dates are not added to db for each run
def nycweather_id_already_processed(nycweather_id):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM chi_crash_data WHERE nycweather_id = ?", (nycweather_id,))
    row = cur.fetchone()

    conn.close()
    return row is not None

def populate_chi_tables(target_dates, max_new_dates=25):
    create_chi_tables()
    new_dates_added = 0

    for date_str in target_dates:
        if new_dates_added >= max_new_dates:
            break

        conn = sqlite3.connect("weather_crashes.db")
        cur = conn.cursor()
        cur.execute("SELECT id FROM NYCWeather WHERE date = ?", (date_str,))
        row = cur.fetchone()
        conn.close()

        if row is None:
            continue

        nycweather_id = row[0]

        if nycweather_id_already_processed(nycweather_id):
            continue

        where_clause = (
            f"crash_date >= '{date_str}T00:00:00' "
            f"AND crash_date < '{date_str}T23:59:59'"
        )
        raw_data = fetch_chi_crashes(limit=1000, where=where_clause)

        if not raw_data:
            continue

        total_crashes, total_injuries, total_fatalities = collect_crash_data(raw_data)

        insert_chi_crashdata(
            nycweather_id,
            total_crashes,
            total_injuries,
            total_fatalities
        )

        new_dates_added += 1
        print(f"Inserted Chicago data for {date_str}")

    return new_dates_added

