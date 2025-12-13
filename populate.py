
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

    
    today = date.today()
    end_date = today - timedelta(days=10)
    start_date = end_date - timedelta(days=99)
    master_dates = days_between(start_date, end_date)
    print(f"Found {len(master_dates)} days that work")

    
    new_weather_dates = populate_weather_for_dates(master_dates, max_days=6)
    if not new_weather_dates:
        print("No new weather dates")
        return
    print(f"Weather populated for {len(new_weather_dates)} new dates: {new_weather_dates}")

    
    new_nyc_count = populate_nyc_crashes(new_weather_dates, max_new_dates=6)
    print(f"Processed {new_nyc_count} new NYC crash dates")


    new_chi_count = populate_chi_tables(new_weather_dates, max_new_dates=6)
    print(f"Processed {new_chi_count} new Chicago crash dates")

    print("Database population complete!")

if __name__ == "__main__":
    main(run_clear=False)
