

from datetime import date, timedelta
from WEATHER_API import populate_weather_for_dates, days_between
from NYC_CRASH_API import populate_nyc_crashes
from CHI_CRASH_API import populate_chi_crashes  # optional

def main():
    today = date.today()
    end_date = today - timedelta(days=1)       
    start_date = end_date - timedelta(days=99) 
    all_dates = days_between(start_date, end_date)
    
    print(f"Found {len(all_dates)} candidate dates to process.")

   
    new_weather_dates = populate_weather_for_dates(all_dates, max_days=25)
    if not new_weather_dates:
        print("No new weather dates to process.")
        return
    print(f"Weather populated for {len(new_weather_dates)} new dates: {new_weather_dates}")

    new_nyc_crashes = populate_nyc_crashes(new_weather_dates, max_new_dates=25)
    print(f"Processed {new_nyc_crashes} new NYC crash dates")

    new_chi_crashes = populate_chi_crashes(new_weather_dates, max_new_dates=25)
    print(f"Processed {new_chi_crashes} new Chicago crash dates")

    print("Database population complete!")

if __name__ == "__main__":
    main()
