import sqlite3
import requests
import unittest
import os 
import json
from requests.auth import HTTPBasicAuth


#TABLE CREATION 
def create_tables()
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    cur.execute(""" CREATE TABLE IF NOT EXISTS crash_info (id INTEGER PRIMARY KEY AUTOINCREMENT, collision_id TEXT, location TEXT, crash_date TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS crash_more(crash_info_id INTEGER, number_of_persons_injured INTEGER, number_of_persons_killed INTEGER, FOREIGN KEY(crash_info_id) REFERENCES crash_info(id))""")

    conn.commit()
    conn.close()







#API CALLS RIGHT BELOW
API_KEY = "2o031k1p3cq0m8kauzmvfjk0c"
API_SECRET = "5nj1sx54lui1b33s5ive0odx58p7yxwhnxyb4gtq8cwuso51i6"

url = "https://data.cityofnewyork.us/api/v3/views/h9gi-nx95/query.json"




def fetch_nyc_crashes(limit=25, where=None, select=None):
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
    
#DATA INSERTION
def insert_crash_info(crash):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    
    cur.execute(""" INSERT OR IGNORE INTO crash_info (collision_id,location,crash_date) VALUES (?, ?, ?)""",(crash['collision_id'], crash['location'], crash['crash_date']))
    
    conn.commit()
    
    crash_info_id = cur.lasrowid
    conn.close()
    
    return crash_info_id

def insert_crash_more(crash):
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()
    cur.execute(""" INSERT OR IGNORE INTO crash_extra (crash_info_id, number_of_persons_injured, number_of_persons_killed) VALUES (?,?,?)""", (crash_info_id, crash['number_of_persons_injured'], crash['number_of_persons_killed']))
    
    conn.commit()
    conn.close()
