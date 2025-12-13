# Name: Isaac Abrams
# Student ID: 9683 2526
# Email: isaacab
# List who you have worked with on this file:
# List any AI tool (e.g. ChatGPT, GitHub Copilot): ChatGPT 5.1 for numpy correlation matrices assistance as well as matplot lib for boxplots and histograms

import sqlite3
import matplotlib.pyplot as plt
import numpy as np


#WARNING: POPULATE ALL DATA (RUN 4 TIMES) BEFORE RUNNING ANALYSIS, NUMPY WILL GET ERROR OTHERWISE

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
                    ON chw.id = nyw.id
                JOIN nyc_crash_stats AS nyc
                    ON nyc.nycweather_id = nyw.id
                JOIN chi_crash_data AS chi
                    ON chi.nycweather_id = nyw.id
                ORDER BY nyw.date;
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
        f"\nThe Pearson's r value for NYC is {r:.3f}. "
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
    plt.savefig("nyc_crash_weather_scatter.png")

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

    with open("results.txt", "a") as f:
        f.write(message)
    
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
    plt.savefig("cho_crash_weather_scatter.png")

    plt.show()

def rainy_vs_dry_barchart(data):
    nyc_precip = data["nyc"]["precip"]
    nyc_crashes = data["nyc"]["crashes"]
    chi_precip = data["chi"]["precip"]
    chi_crashes = data["chi"]["crashes"]

    nyc_rainy = []
    nyc_dry = []

    for i in range(len(nyc_precip)):
        if nyc_precip[i] > 0:
            nyc_rainy.append(nyc_crashes[i])
        else:
            nyc_dry.append(nyc_crashes[i])

    #calculate nyc average crashes
    nyc_rain_avg = sum(nyc_rainy) / len(nyc_rainy) if len(nyc_rainy) > 0 else 0
    nyc_dry_avg  = sum(nyc_dry) / len(nyc_dry) if len(nyc_dry) > 0 else 0

    chi_rainy = []
    chi_dry = []

    #get Chicago rainy vs dry lists
    for i in range(len(chi_precip)):
        if chi_precip[i] > 0:
            chi_rainy.append(chi_crashes[i])
        else:
            chi_dry.append(chi_crashes[i])

    #average chicago rain amount
    chi_rain_avg = sum(chi_rainy) / len(chi_rainy) if len(chi_rainy) > 0 else 0
    chi_dry_avg  = sum(chi_dry) / len(chi_dry) if len(chi_dry) > 0 else 0

    #create message for analysis and write to results file
    message = (
        f"\nNYC Rain Avg: {nyc_rain_avg:.2f}, NYC Dry Avg: {nyc_dry_avg:.2f}\n"
        f"Chicago Rain Avg: {chi_rain_avg:.2f}, Chicago Dry Avg: {chi_dry_avg:.2f}\n"
    )

    with open("results.txt", "a") as f:
        f.write(message)

    #create bar graph plot
    labels = ["NYC Rain", "NYC Dry", "Chicago Rain", "Chicago Dry"]
    averages = [nyc_rain_avg, nyc_dry_avg, chi_rain_avg, chi_dry_avg]

    plt.figure(figsize=(10, 6))
    #AI used to suggest colors
    plt.bar(labels, averages, color=["skyblue", "steelblue", "lightcoral", "indianred"])

    plt.ylabel("Average Crashes")
    plt.title("Average Crash Count: Rainy vs Dry Days (NYC & Chicago)")
    plt.tight_layout()
    plt.savefig("rainy_vs_dry_bar.png")
    plt.show()

def rainy_vs_dry_boxplot(data):
    nyc_precip = data["nyc"]["precip"]
    nyc_crashes = data["nyc"]["crashes"]
    chi_precip = data["chi"]["precip"]
    chi_crashes = data["chi"]["crashes"]

    nyc_rainy = []
    nyc_dry = []

    #Fill NYC rainy vs dry lists
    for i in range(len(nyc_precip)):
        if nyc_precip[i] > 0:
            nyc_rainy.append(nyc_crashes[i])
        else:
            nyc_dry.append(nyc_crashes[i])
    
    chi_rainy = []
    chi_dry = []

    #Fill Chicago rainy vs dry lists
    for i in range(len(chi_precip)):
        if chi_precip[i] > 0:
            chi_rainy.append(chi_crashes[i])
        else:
            chi_dry.append(chi_crashes[i])

    nyc_dry_med = np.median(nyc_dry)
    nyc_wet_med = np.median(nyc_rainy)
    chi_dry_med = np.median(chi_dry)
    chi_wet_med = np.median(chi_rainy)

    #Create message and write to file
    message = (
    "\nDry vs Wet Crash Count Medians\n"
    f"NYC - Dry: {nyc_dry_med:.2f}, Wet: {nyc_wet_med:.2f}, Difference: {nyc_wet_med - nyc_dry_med:.2f}\n"
    f"Chicago - Dry: {chi_dry_med:.2f}, Wet: {chi_wet_med:.2f}, Difference: {chi_wet_med - chi_dry_med:.2f}\n") 

    with open("results.txt", "a") as f:
        f.write(message)

    #Create box plot
    box_data = [nyc_dry, nyc_rainy, chi_dry, chi_rainy]
    labels = ["NYC Dry", "NYC Wet", "Chicago Dry", "Chicago Wet"]

    plt.figure(figsize=(10, 6))
    plt.boxplot(box_data, labels=labels, showfliers=True)
    plt.title("Distribution of Daily Crash Counts on Dry vs Wet Days")
    plt.ylabel("Total Crashes")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig("rainy_vs_dry_boxplot.png")

    plt.show()

def crash_histogram(data):
    #Data is already ordered by date
    nyc_crashes = data["nyc"]["crashes"]
    chi_crashes = data["chi"]["crashes"]
    
    #Compute stats for analysis
    nyc_mean = np.mean(nyc_crashes)
    nyc_std  = np.std(nyc_crashes)

    chi_mean = np.mean(chi_crashes)
    chi_std  = np.std(chi_crashes)

    message = (
        "\nCrash Count Distribution Summary\n"
        f"NYC - Mean: {nyc_mean:.2f}, Std Dev: {nyc_std:.2f}\n"
        f"Chicago - Mean: {chi_mean:.2f}, Std Dev: {chi_std:.2f}\n"
    )

    with open("results.txt", "a") as f:
        f.write(message)

    #Create plot
    plt.figure(figsize=(10, 6))
    plt.hist(nyc_crashes, bins=15, alpha=0.6, label="NYC")
    plt.hist(chi_crashes, bins=15, alpha=0.6, label="Chicago")

    plt.xlabel("Daily Crash Count")
    plt.ylabel("Frequency")
    plt.title("Distribution of Daily Crash Counts")
    plt.legend()
    plt.tight_layout()
    plt.savefig("crash_histogram.png")

    plt.show()



def main():
    loaded_data = load_data_for_analysis()
    chi_crash_weather_corr(loaded_data)
    nyc_crash_weather_corr(loaded_data)
    rainy_vs_dry_barchart(loaded_data)
    rainy_vs_dry_boxplot(loaded_data)
    crash_histogram(loaded_data)

if __name__ == "__main__":
    main()