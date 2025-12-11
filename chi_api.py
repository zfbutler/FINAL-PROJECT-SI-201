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

def create_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()


    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS chi_crash_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            total_crashes INTEGER,
            total_injuries INTEGER,
            total_fatalities INTEGER
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
def insert_chi_crashdata(date_str, total_crashes, total_injuries, total_fatalities):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO chi_crash_data (date, total_crashes, total_injuries, total_fatalities)
        VALUES (?, ?, ?, ?)
    """, (date_str, total_crashes, total_injuries, total_fatalities))

    conn.commit()
    conn.close()
    return

#helper function to ensure repeat dates are not added to db for each run
def date_already_processed(date_str):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM chi_crash_data WHERE date = ?", (date_str,))
    row = cur.fetchone()

    conn.close()
    return row is not None

def populate_tables(target_dates):
    create_tables()
    batch_size = 1000

    max_new_dates = 25
    new_dates_added = 0
    
    for date_str in target_dates:
        if new_dates_added >= max_new_dates:
            print("API population has been reached. Exiting...")
            break
        
        if date_already_processed(date_str):
            continue
        
        where_clause = f"crash_date >= '{date_str}T00:00:00' AND crash_date < '{date_str}T23:59:59'"
        raw_data = fetch_chi_crashes(limit=batch_size, where=where_clause)

        if not raw_data:
            print(f"No data returned from {date_str}.")
            continue

        total_crashes, total_injuries, total_fatalities = collect_crash_data(raw_data)
        insert_chi_crashdata(date_str, total_crashes, total_injuries, total_fatalities)

        new_dates_added += 1

        print(f"Inserted data for {date_str}")
    
    return

def main():
    #clear_chi_tables()
    populate_tables(target_dates)

if __name__ == "__main__":
    main()