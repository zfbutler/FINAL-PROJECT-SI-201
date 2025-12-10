import sqlite3
import requests
import unittest
import os 
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime

def clear_db():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS crash_more")
    cur.execute("DROP TABLE IF EXISTS crash_info")
    conn.commit()
    conn.close()

#TABLE CREATION 
def create_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute(""" 
            CREATE TABLE IF NOT EXISTS crash_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collision_id TEXT,
                location TEXT,
                crash_date TEXT
            )
    """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS crash_more(
            crash_info_id INTEGER,
            number_of_persons_injured INTEGER,
            number_of_persons_killed INTEGER,
            FOREIGN KEY(crash_info_id) REFERENCES crash_info(id)
        )
    """)

    conn.commit()
    conn.close()







#API CALLS RIGHT BELOW
API_KEY = "6ayvS2Y0kYuSCGwdmTNcIjPuj"


url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"





def fetch_nyc_crashes(limit=25,offset=0, where=None, select=None, order=None):
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
    return cleaned_rows











#DATA INSERTION
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
    return crashes




def main():
    clear_db()
    create_tables()

    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    cur.execute("SELECT collision_id FROM crash_info")
    existing_ids = set(row[0] for row in cur.fetchall())
    conn.close()

    batch_size = 100
    target_dates = ["2023-01-15", "2023-01-16", "2023-01-17"]
    all_raw_data = []
    for d in target_dates:
        where_clause = f"crash_date >= '{d}T00:00:00' AND crash_date < '{d}T23:59:59'"
        raw_data = fetch_nyc_crashes(limit=batch_size, where=where_clause)
        all_raw_data.extend(raw_data)
    if not all_raw_data:
        print("No data returned from API.")
        return

    cleaned_data = check_crash_data(all_raw_data)

    new_data = []
    skipped_dupes = 0
    skipped_dirty = 0

    for crash in cleaned_data:
        if crash['collision_id'] in existing_ids:
            skipped_dupes += 1
            continue
        new_data.append(crash)
        if len(new_data) >= 25:
            break

    skipped_dirty = len(cleaned_data) - skipped_dupes - len(new_data)

    for crash in new_data:
        crash_info_id = insert_crash_info(crash)
        if crash_info_id == 0:
            conn = sqlite3.connect("weather_crashes.db")
            cur = conn.cursor()
            cur.execute("SELECT id FROM crash_info WHERE collision_id=?", (crash['collision_id'],))
            crash_info_id = cur.fetchone()[0]
            conn.close()
        insert_crash_more(crash, crash_info_id)

    print(f"Put {len(new_data)} new records into the DB this run.")
    print(f"Skipped {skipped_dupes} duplicates and {skipped_dirty} unclean rows.")

    """# === Debug Info ===
    print("\n=== Debug Info ===")
    print(f"Total cleaned rows ready to insert: {len(new_data)}")
    for i, crash in enumerate(new_data[:5]):  # show first 5 for brevity
        print(f"{i+1}: {crash}")"""

if __name__ == "__main__":
    main()
