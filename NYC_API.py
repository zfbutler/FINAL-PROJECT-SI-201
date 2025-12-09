import sqlite3
import requests
import unittest
import os 
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime


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
API_KEY = "2o031k1p3cq0m8kauzmvfjk0c"
API_SECRET = "5nj1sx54lui1b33s5ive0odx58p7yxwhnxyb4gtq8cwuso51i6"

url = "https://data.cityofnewyork.us/api/v3/views/h9gi-nx95/query.json"




def fetch_nyc_crashes(limit=25, where=None, select=None, order=None):
    params = {"$limit": limit}
    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    if order:
        params["$order"] = order

    headers = {"X-App-Token": API_KEY}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    return response.json()

    
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
            dt_str = f"{crash_date}T{crash_time}:00.000"
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.")
                crash_date_str= dt.strftime("%Y-%m-%d")
            except ValueError:
                continue 

            if not(6 <= dt.hour < 18):
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
