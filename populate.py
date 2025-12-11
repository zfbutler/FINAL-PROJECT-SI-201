import NYC_API
import chi_api
import WEATHER_API

def clear_all_tables():
    print("Clearing NYC tables...")
    NYC_API.clear_tables()

    print("Clearing Chicago tables...")
    chi_api.clear_chi_tables()

    print("Clearing Weather tables...")
    WEATHER_API.clear_weather_tables()

def run_all():
    print("Running NYC population...")
    NYC_API.main()

    print("Running Chicago population...")
    chi_api.main()

    print("Running Weather population...")
    WEATHER_API.main()

def main():
    clear_all_tables()
    run_all()
    print("Database repopulation complete!")

if __name__ == "__main__":
    main()