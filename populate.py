# populate.py
import sqlite3
from datetime import date, timedelta
from WEATHER_API import populate_weather_for_dates, days_between, create_weather_tables
from NYC_API import populate_nyc_crashes
from chi_api import populate_chi_tables


def clear_all_tables():
    conn = sqlite3.connect("weather_crashes.db")
    cur = conn.cursor()

    tables = [
        "nyc_crash_stats",
        "chi_crash_data",
        "NYCWeather",
        "ChicagoWeather",
    ]

    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table}")

    conn.commit()
    conn.close()
    print("All tables cleared.")




def main(run_clear):
    if run_clear:
        clear_all_tables()
    create_weather_tables()

    # 2️⃣ Build master date list (last 100 days as example)
    today = date.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=99)
    master_dates = days_between(start_date, end_date)
    print(f"Found {len(master_dates)} candidate dates to process.")

    # 3️⃣ Populate weather first (returns dates successfully populated)
    new_weather_dates = populate_weather_for_dates(master_dates, max_days=25)
    if not new_weather_dates:
        print("No new weather dates to process.")
        return
    print(f"Weather populated for {len(new_weather_dates)} new dates: {new_weather_dates}")

    # 4️⃣ Populate NYC crashes using only the weather-populated dates
    new_nyc_count = populate_nyc_crashes(master_dates, max_new_dates=25)
    print(f"Processed {new_nyc_count} new NYC crash dates")

    # 5️⃣ Populate Chicago crashes using same dates
    new_chi_count = populate_chi_tables(master_dates, max_new_dates=25)
    print(f"Processed {new_chi_count} new Chicago crash dates")

    print("Database population complete!")

if __name__ == "__main__":
    # Set run_clear=True only when you want to wipe all tables
    main(run_clear=True)
