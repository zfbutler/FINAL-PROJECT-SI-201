import sqlite3
import matplotlib.pyplot as plt
import numpy as np

def load_data_for_analysis(db_path="weather_crashes.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    #select rows from databases using a JOIN
    cur.execute("""
                SELECT
                    nyw.date,
                    nyw.precip_mm AS nyc_precip,
                    chw.precip_mm AS chi_precip,
                    nyc.total_crashes AS nyc_crashes,
                    nyc.total_injuries AS nyc_injuries,
                    chi.total_crashes as chi_crashes,
                    chi.total_injuries AS chi_injuries
                FROM NYCWeather AS nyw
                JOIN ChicagoWeather AS chw
                    ON nyw.date = chw.date
                JOIN nyc_crash_stats AS nyc
                    ON nyc.date = nyw.date
                JOIN chi_crash_data AS chi
                    ON chi.date = chw.date
                ORDER BY chw.date;
                """)
    
    rows = cur.fetchall()
    conn.close()

    #create a database of info with keys for dates and cities, and sub-keys for stats
    data = {
        "dates": [],
        "nyc": {
            "precip":   [],
            "crashes":  [],
            "injuries": []
        },
        "chi": {
            "precip":   [],
            "crashes":  [],
            "injuries": []
        }
    }

    for date, nyc_precip, chi_precip, nyc_crash, nyc_inj, chi_crash, chi_inj in rows:
        data["dates"].append(date)
        data["nyc"]["precip"].append(nyc_precip)
        data["nyc"]["crashes"].append(nyc_crash)
        data["nyc"]["injuries"].append(nyc_inj)
        data["chi"]["precip"].append(chi_precip)
        data["chi"]["crashes"].append(chi_crash)
        data["chi"]["injuries"].append(chi_inj)

    print(data)
    return data

def nyc_crash_weather_corr(data):
    nyc_precip   = data["nyc"]["precip"]
    nyc_crashes  = data["nyc"]["crashes"]

    #convert to x and y data arrays for numpy
    x = np.array(nyc_precip, dtype=float)
    y = np.array(nyc_crashes, dtype=float)

    r = np.corrcoef(x, y)[0, 1]

    #for r correlation strength
    if r > 0.6:
        strength = "strong"
    elif r > 0.4:
        strength = "moderate"
    else:
        strength = "weak"

    #create a message and append it to an existing output file
    message = (
        f"The Pearson's r value for NYC is {r:.3f}. "
        f"This indicates a {strength} relationship between NYC Traffic Crashes and Precipitation.\n"
    )

    with open("results.txt", "a") as f:
        f.write(message)


    plt.figure(figsize = (10, 6))
    plt.scatter(nyc_precip, nyc_crashes)
    
    plt.xlim(min(nyc_precip), max(nyc_precip))

    #for a regression line
    if len(nyc_precip) > 1:
        beta_1, beta_0 = np.polyfit(nyc_precip, nyc_crashes, 1)
        x_line = np.linspace(min(nyc_precip), max(nyc_precip), 100)
        y_line = beta_1 * x_line + beta_0

        plt.plot(x_line, y_line)


    plt.xlabel("NYC Precipitation (mm)")
    plt.ylabel("NYC Crashes")
    plt.title("NYC Crashes vs Rainfall")

    plt.show()

def chi_crash_weather_corr(data):
    chi_precip   = data["chi"]["precip"]
    chi_crashes  = data["chi"]["crashes"]

    #compute r by turning predictors and response veriables into np matrices
    x = np.array(chi_precip, dtype=float)
    y = np.array(chi_crashes, dtype=float)

    r = np.corrcoef(x, y)[0, 1]

    # find strength of r
    if r > 0.6:
        strength = "strong"
    elif r > 0.4:
        strength = "moderate"
    else:
        strength = "weak"

    # build message and write to file
    message = (
        f"The Pearson's r value for Chicago is {r:.3f}. "
        f"This indicates a {strength} relationship between Chicago Traffic Crashes and Precipitation.\n"
    )

    with open("r_results.txt", "a") as f:
        f.write(message)

    print("Wrote:", message)

    
    plt.figure(figsize=(10, 6))
    plt.scatter(chi_precip, chi_crashes)
    
    plt.xlim(min(chi_precip), max(chi_precip))

    #for a regression line
    if len(chi_precip) > 1:
        beta_1, beta_0 = np.polyfit(chi_precip, chi_crashes, 1)
        x_line = np.linspace(min(chi_precip), max(chi_precip), 100)
        y_line = beta_1 * x_line + beta_0
        plt.plot(x_line, y_line)

    plt.xlabel("Chicago Precipitation (mm)")
    plt.ylabel("Chicago Crashes")
    plt.title("Chicago Crashes vs Rainfall")

    plt.show()

def main():
    loaded_data = load_data_for_analysis()
    chi_crash_weather_corr(loaded_data)
    nyc_crash_weather_corr(loaded_data)
    


if __name__ == "__main__":
    main()