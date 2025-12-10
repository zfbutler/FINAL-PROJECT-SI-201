import sqlite3
import requests
import unittest
import os 
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime


#TABLE CREATION 

def clear_tables():
    """Drop all tables in the weather_crashes.db database."""
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    tables = ["injury_stats", "date_info", "crash_info", "crash_more" ]  # Add any future tables here
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table}")

    conn.commit()
    conn.close()
    print("All tables cleared.")



def create_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    # Drop tables if they exist
    cur.execute("DROP TABLE IF EXISTS injury_stats")
    cur.execute("DROP TABLE IF EXISTS date_info")

    # Table 1: date_info now also holds total_crashes
    cur.execute(""" 
        CREATE TABLE date_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            total_crashes INTEGER
        )
    """)

    # Table 2: injury_stats
    cur.execute("""
        CREATE TABLE injury_stats (
            date_id INTEGER PRIMARY KEY,
            total_injuries INTEGER,
            total_killed INTEGER,
            FOREIGN KEY(date_id) REFERENCES date_info(id)
        )
    """)

    conn.commit()
    conn.close()




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










#OLD CLEANING 
''' 
def check_crash_data(api_data):
    cleaned_rows = []
    for row in api_data:
        try: 
            collision_id = row.get("collision_id")
            location = row.get("location")
            crash_date = row.get("crash_date")
            crash_time = row.get("crash_time")

            if not collision_id or not location or not crash_date or not crash_time:
                continue

           
            date_part = crash_date.split("T")[0]      
            time_part = crash_time                    

            try:
                dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M")
            except ValueError:
                continue

            crash_date_str = date_part  

            if not (6 <= dt.hour < 18):
                continue

            try:
                number_of_persons_injured = int(row.get("number_of_persons_injured", 0))
                if number_of_persons_injured == 0:
                    continue 
            except (TypeError, ValueError):
                continue 
            
            try:
                number_of_persons_killed = int(row.get("number_of_persons_killed", 0))
            except (TypeError, ValueError):
                number_of_persons_killed = 0
            if isinstance(location, dict):
                lat = location.get("latitude")
                lon = location.get("longitude")
                location = f"{lat}, {lon}"

            cleaned_rows.append({
                "collision_id": collision_id,
                "location": location,
                "crash_date": crash_date_str,
                "number_of_persons_injured": number_of_persons_injured,
                "number_of_persons_killed": number_of_persons_killed
            })

        except Exception:
            continue
    return cleaned_rows'''

def get_or_create_date(date_str, total_crashes):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO date_info (date, total_crashes)
        VALUES (?, ?)
    """, (date_str, total_crashes))

    # Get the date_id
    cur.execute("SELECT id FROM date_info WHERE date = ?", (date_str,))
    date_id = cur.fetchone()[0]

    conn.commit()
    conn.close()
    return date_id

def insert_injury_stats(date_id, total_injuries, total_fatalities):
    
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO injury_stats (date_id, total_injuries, total_killed)
        VALUES (?, ?, ?)
    """, (date_id, total_injuries, total_fatalities))

    conn.commit()
    conn.close()








'''#DATA INSERTION
def insert_crash_info(crash):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    
    cur.execute(""" INSERT OR IGNORE INTO crash_info (collision_id,location,crash_date) VALUES (?, ?, ?)""",(crash['collision_id'], crash['location'], crash['crash_date']))
    
    conn.commit()
    
    crash_info_id = cur.lastrowid
    conn.close()
    
    return crash_info_id

def insert_crash_more(crash, crash_info_id):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    cur.execute(""" 
        INSERT OR IGNORE INTO crash_more (crash_info_id, number_of_persons_injured, number_of_persons_killed)
        VALUES (?,?,?)""",
        (crash_info_id, crash['number_of_persons_injured'], crash['number_of_persons_killed'])
    )
    
    conn.commit()
    conn.close()

def get_crashes_for_dates(dates):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    holders = ",".join("?" for _ in dates)
    query = f"""
        SELECT ci.id, ci.collision_id, ci.location, ci.crash_date,
               cm.number_of_persons_injured, cm.number_of_persons_killed
        FROM crash_info ci
        JOIN crash_more cm ON ci.id = cm.crash_info_id
        WHERE ci.crash_date IN ({holders})
    """
    cur.execute(query,dates)
    rows = cur.fetchall()
    conn.close()

    crashes = []
    for r in rows:
        crashes.append({
            "id": r[0],
            "collision_id": r[1],
            "location": r[2],
            "crash_date": r[3],
            "number_of_persons_injured": r[4],
            "number_of_persons_killed": r[5]
        })
    return crashes'''




def main():
    clear_tables()
    create_tables()

    
    batch_size = 1000
    target_dates = [
    "2025-11-15",
    "2025-11-16",
    "2025-11-17",
    "2025-11-18",
    "2025-11-19",
    "2025-11-20",
    "2025-11-21",
    "2025-11-22",
    "2025-11-23",
    "2025-11-24",
    "2025-11-25",
    "2025-11-26",
    "2025-11-27",
    "2025-11-28",
    "2025-11-29",
    "2025-11-30",
    "2025-12-01",
    "2025-12-02",
    "2025-12-03",
    "2025-12-04",
    "2025-12-05",
    "2025-12-06",
    "2025-12-07",
    "2025-12-08",
    "2025-12-09"]
    for date_str in target_dates[:25]:
        where_clause = f"crash_date >= '{date_str}T00:00:00' AND crash_date < '{date_str}T23:59:59'"
        raw_data = fetch_nyc_crashes(limit=batch_size, where=where_clause)

        if not raw_data:
            print(f"No data returned from {date_str}.")
            continue

        total_crashes, total_injuries, total_fatalities = check_crash_data(raw_data)
        date_id = get_or_create_date(date_str, total_crashes)
        insert_injury_stats(date_id, total_injuries, total_fatalities)

        print(f"Inserted data for {date_str}")



if __name__ == "__main__":
    main()
