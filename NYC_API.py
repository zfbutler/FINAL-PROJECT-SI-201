import sqlite3
import requests
import unittest
import os 
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime


#TABLE CREATION 

def clear_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    tables = ["nyc_crash_stats"] 
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table}")

    conn.commit()
    conn.close()
    print("All tables cleared.")



def create_table():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS nyc_crash_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            total_crashes INTEGER,
            total_injuries INTEGER,
            total_fatalities INTEGER
        )
    """)

    conn.commit()
    conn.close()
    print("Table 'nyc_crash_stats' ready.")




#API CALLS RIGHT BELOW
API_KEY = "6ayvS2Y0kYuSCGwdmTNcIjPuj"


url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"





def fetch_nyc_crashes(limit=1000,offset=0, where=None, select=None, order=None):
    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    if order:
        params["$order"] = order
    
    headers = {"X-App-Token": API_KEY}
    try:    
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"You dont have access kid")
    return []
    
#DATA CLEANING 
def check_crash_data(api_data):
    
    
    total_crashes = 0
    total_injuries = 0 
    total_fatalities = 0 

    for row in api_data:
        try:
            crash_date =row.get("crash_date")
            crash_time = row.get("crash_time")
            if not crash_date or not crash_time:
                continue 
            dp = crash_date.split("T")[0]
            dt = datetime.strptime(f"{dp} {crash_time}", "%Y-%m-%d %H:%M")
            if not (6<= dt.hour <18):
                continue 
            total_crashes +=1 

            try:
                total_injuries += int(row.get("number_of_persons_injured", 0))
            except:
                pass
            

            try:
                total_fatalities += int(row.get("number_of_persons_killed", 0))
            except:
                pass

        except Exception:
            continue 

    return total_crashes, total_injuries, total_fatalities


def insert_weather_stats(date_str, total_crashes, total_injuries, total_fatalities):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO nyc_crash_stats
        (date, total_crashes, total_injuries, total_fatalities)
        VALUES (?, ?, ?, ?)
    """, (date_str, total_crashes, total_injuries, total_fatalities))

    conn.commit()
    conn.close()





def date_already_processed(date_str):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM nyc_crash_stats WHERE date = ?", (date_str,))
    row = cur.fetchone()

    conn.close()
    return row is not None

def main():
    # Clear old table and create the merged table
    #clear_tables()
    create_table()

    batch_size = 1000
    target_dates = [
    "2025-11-01","2025-11-02","2025-11-03","2025-11-04","2025-11-05",
    "2025-11-06","2025-11-07","2025-11-08","2025-11-09","2025-11-10",
    "2025-11-11","2025-11-12","2025-11-13","2025-11-14","2025-11-15",
    "2025-11-16","2025-11-17","2025-11-18","2025-11-19","2025-11-20",
    "2025-11-21","2025-11-22","2025-11-23","2025-11-24","2025-11-25",
    "2025-11-26","2025-11-27","2025-11-28","2025-11-29","2025-11-30",
    "2025-12-01"
]

    

    max_new_dates = 25
    new_dates_added = 0

    for date_str in target_dates:
        if new_dates_added >= max_new_dates:
            print("API population limit reached. Exiting...")
            break

        if date_already_processed(date_str):
            continue

        where_clause = f"crash_date >= '{date_str}T00:00:00' AND crash_date < '{date_str}T23:59:59'"
        raw_data = fetch_nyc_crashes(limit=batch_size, where=where_clause)

        if not raw_data:
            print(f"No data returned for {date_str}.")
            continue

        total_crashes, total_injuries, total_fatalities = check_crash_data(raw_data)

        if total_crashes == 0:
            print(f"No crashes for {date_str}. Skipping insert.")
            continue

        insert_weather_stats(date_str, total_crashes, total_injuries, total_fatalities)
        new_dates_added += 1

        print(f"Inserted data for {date_str}")




if __name__ == "__main__":
    main()
