import glob
import importlib
import os
from pathlib import Path
import sys
from functools import reduce

import pandas as pd   

# Refs:
    # - https://support.atmotube.com/en/articles/10450299-sensor-accuracy-and-technical-specifications
    # - https://support.atmotube.com/en/articles/12621821-understanding-atmotube-pro-2-air-quality-score-aqs
    # - https://support.atmotube.com/en/articles/10365067-data-storage-and-collection
    # - https://support.atmotube.com/en/articles/13002682-history-mode-overview
    # - https://support.atmotube.com/en/articles/12629420-atmotube-pro-2-technical-specifications    

    # - https://manuals.plus/m/5680a4efbd277d95da9d611da9e9e4d4f685a0f3a5e72d9c7c815662d60fb8db
    # - https://www.airgradient.com/blog/explaining-voc-tvoc-and-voc-index/
    # - https://sensirion.com/media/documents/4B4D0E67/6520038C/GAS_AN_SGP4x_BuildingStandards_D1_1.pdf 
    # - https://sensirion.com/media/documents/9F289B95/6294DFFC/Info_Note_NOx_Index.pdf
    # - https://gnsssimulator.com/gnss-accuracy-metrics-dop-cn0-ttff/

# ======================== HOW TO USE ========================

# 1. Upload raw data (csv) in `parserTools/1.upload/atmoData`
# 2. Open a terminal in VS Code (Ctrl+Shift+`)
# 3. Set working directory to `cd parserTools`
# 4. Test run this script in terminal as `python 2.make/atmoData.py example_ext.csv`
# 5. Run it with your actual csv.

# ============================================================

### Grab uploaded raw data from 1.upload/atmoData

base_dir = Path(__file__).parent.parent # Ensures working in parserTools

os.chdir(base_dir / "1.upload/atmoData") # Move to parserTools/1.upload/atmoData
print("Upload directory:", os.getcwd())

if len(sys.argv) < 2:
    print("Error: Write only the CSV file name, not including file path")
    print("Usage: python 2.make/atmoData.py <filename.csv>")
    sys.exit(1)   

csv_path = sys.argv[1] # Get csv path from command line, relative to 1.upload/atmoData
df = pd.read_csv(csv_path)

os.chdir(base_dir / "2.make") # Back to parserTools/2.make
print("Make directory:", os.getcwd())

import dataParser.parse_utils as parse_utils # Imports custom utilities!
importlib.reload(parse_utils) # Reloads the utils module after edits

# ============================================================
### Filter for ENVIRONMENT Variables (`env`)

# Standardize date & time (UTC)
df = parse_utils.rename_col_hard_include(df,
                                ["lat"], "lat",
                                ["lon"], "lon",
                                ["date"], "datetime"
                                );
df['datetime'] = pd.to_datetime(df['datetime'], utc=True) # Standardize datetime (UTC format)
df = parse_utils.add_time_zoneandlocal(df, datetime_col='datetime', lon_col='lon', lat_col='lat') # datetime = Index key to merge all dfs at the end
time_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "timezone", "date", "time"]) 
    # datetime are in ISO 8601 date and time format, the 00+00:00 is in UTC format (for how much hours is offset)
    # timezone is in IANA database format

# print(df.columns.tolist())
# print(time_df.dropna().head(3)) # shows first non-null lon/lat rows

# PM sensor = Sensirion SPS30
pm_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "pm", "µg/m³"], ["ppm", "particle"]) 
pm_df = parse_utils.rename_col_hard_include(pm_df, 
                                ["pm", "2", "5"], "pm2_5_ugm3_atm", # micrograms per cubic meter (how many micrograms [1/1,000,000 of a gram] of Particle Matter are in a cubic meter of air)
                                ["pm", "10"], "pm10_ugm3_atm",
                                ["pm", "1"], "pm1_0_ugm3_atm"
                                )

pm_ext_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "pm", "particle"], ["µg/m³", "ppm"]); # Only for PRO 2
if pm_ext_df.shape[1] <= 1:  # only datetime column or fewer
    print("No PM extension (raw particle count) data available.")
else:
    pm_ext_df = parse_utils.rename_col_hard_include(pm_ext_df,
                                    ["0", "5"], "pm0_5_um_count", # raw count in micrometer (how many number of particles with a diameter beyond 0.5 micrometer per unit volume)
                                    ["2", "5"], "pm2_5_um_count", # beyond 2.5 um
                                    ["10"], "pm10_um_count", # beyond 10 um
                                    ["1"], "pm1_0_um_count", # beyond 1.0 um 
                                    ["size"], "pmsize_nm_avg" # average particle (convert later)
                                    );
    if "pmsize_nm_avg" in pm_ext_df.columns:
        pm_ext_df["pmsize_um_avg"] = pm_ext_df["pmsize_nm_avg"]/1000 # nanometer (nm) --> micrometer (µm)
    pm_df = pd.merge(pm_df, pm_ext_df, on="datetime", how="left") 

# print(pm_df.dropna().head(3)) 

# Barometer sensor = Bosch BME280 
weather_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "temp", "hum", "press"]) # for heat index and dew point
weather_df = parse_utils.rename_col_hard_include(weather_df,
                                    ["temp"], "temp_c", # celsius
                                    ["hum"], "hum_pct", # relative humidity (how much water vapor is in the air compared to how much it can hold at that temp, 0% = dry, 100% = saturated)
                                    ["press"], "press_ahPa" # hectopascals (absolute pressure at current elevation, not at sea level)
                                    );

# print(weather_df.dropna().head(3)) 

# TOV & Nox sensor = Sensirion SGP41
# CO2 sensor = Sensirion SCD41
# AQS aggregate = PM + TVOC + CO2 + NOx
qs_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "aqs", "tvoc", "nox", "co2"]) # for Air Quality Score (AQS)
qs_df = parse_utils.rename_col_hard_include(qs_df,
                                ["aqs"], "aqs_total", # out of 100 (aggregate of the following variables [0 = polluted, 100 = clean])
                                ["tvoc", "ppm"], "tvoc_ppm", # parts per million (absolute concetration of Total Volatile Organic Compounds gases in 1 vol of air)
                                ["tvoc", "index"], "tvoc_index", # out of 500 (normalized score where 100 = baseline/ref levels in past 24 hrs [>100 = becoming polluted ,<100 = becoming cleaner])
                                ["nox", "index"], "nox_index", # out of 500 (normalized score of Nitric Oxide [NO] & Nitrogen Dioxide [NO2] where 100 = baseline/ref levels in past 24 hrs)
                                ["co2", "ppm"], "co2_ppm" # parts per million (absolute concentration of Carbon Dioxide [CO2] in 1 vol of air)
                                );

# print(qs_df.dropna().head(3)) 

# GPS module = phone (PRO has no internal GPS)
# Altitude = from BME280 pressure alt (GP alt)
gps_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "gps", "lat", "lon", "alt", "motion", "phone", "position"], ["batt", "charg", "localtime", "timezone"]) 
gps_df['gps_past_bool'] = df['Phone GPS'].map({'yes': 'no', 'no': 'yes'})
    # past GPS? no/yes (is the GPS data taken from the phone's local storage?) 
        # yes, PRO grabbed data from the phone's local storage as historical GPS data
        # no, PRO grabbed data from the phone's GPS as real-time GPS data
gps_df = parse_utils.rename_col_hard_include(gps_df,
                                ["lat"], "lat_deg", # degrees (horizontal line at 0° = equator)
                                ["lon"], "lon_deg", # degrees (vertical line at 0° = prime meridian)
                                ["alt"], "alt_m", # current elevation 
                                ["position"], "position_error_m", # meters (how accurate is the phone GPS?)
                                ["motion"], "motion_now_bool", # yes/no (is the device moving?)
                                ["phone"], "gps_now_bool" # yes/no (is the GPS data taken from the phone right now?)
                                );
gps_df = gps_df.replace({'yes': 1, 'no': 0}); # Convert yes/no to 1/0

gps_ext_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "gnss", "sat"]) # Only for PRO 2
if gps_ext_df.shape[1] <= 1:  # only datetime column or fewer
    print("No GPS extension (satellite signal count) data available.")
else:
    gps_ext_df = parse_utils.rename_col_hard_include(gps_ext_df, 
                                    ["sat", "view"], "sat_view_count", # how many satellites are in view?
                                    ["sat", "fix"], "sat_fix_count", # how many satellites are being used to calculate your location fix? (3d fix [lon,lat,alt] min = 4, 2d fix [lon,lat] min = 3)
                                    ["gnss", "snr", "0-19"], "sat_lowsignal_count", # how many satellites are in view with low signal strength?
                                    ["gnss", "snr", "20-49"], "sat_medsignal_count", # medium signal strength?
                                    ["gnss", "snr", "50-99"], "sat_highsignal_count", # high signal strength?
                                    ["gnss", "snr", "avg"], "sat_signal_avg" # how strong satellite signal is relative to surrounding electronic noise?
                                    );
    gps_df = pd.merge(gps_df, gps_ext_df, on="datetime", how="left") 

# print(gps_df.dropna().head(3))

# Notes:
    ## Why PRO can't actually measure GPS accuracy (position error)? 
        # It relies on whatever phone GNSS chip is
        # Accuracy is based on satellite geometry, no. of satellites, and signal strength
        # PRO 2 doesn't record satellite geometry (DOP [HDOP/PDOP/VDOP])
        # PRO 2 does record signal strength (SNR Signal-to-Noise-Ratio across 4 bands)

# ----------------------------------------------------------
### Filter for POWER Variables (`pwr`)

# Battery = Phone
pwr_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "batt", "charg"]) 
pwr_df = parse_utils.rename_col_hard_include(pwr_df,
                                ["batt"], "battery_phone_pct", # how much battery power is left in the phone?
                                ["charg"], "charg_phone_state", # is the phone charging now (2), cooling down post-charge (1), or not charging (0)?
                                )
mapping = {"no": 0, "cd": 1, "yes": 2,} # ordinal values
pwr_df["charg_phone_state"] = (pwr_df["charg_phone_state"].map(mapping))
pwr_df["charg_phone_bool"] = (pwr_df["charg_phone_state"] == 2).astype(int) # simpler variable just incase (1 = charging, 0 = not)

# print(pwr_df.dropna().head(3))

# ----------------------------------------------------------
### Filter for TEXT  Variables (`txt`)
txt_df = parse_utils.get_col_soft_inandexclude(df, ["datetime", "notes"]) # User notes
txt_df = parse_utils.rename_col_hard_include(txt_df,
                                ["notes"], "user_notes" # User notes
                                );

# print(txt_df.dropna().head(3)) 

# ============================================================

### Merge and save as csv in 3.save

dfs = [time_df, pm_df, weather_df, qs_df, gps_df, pwr_df, txt_df]
dfs = reduce(lambda left, right: pd.merge(left, right, on='datetime'), dfs)   

os.chdir(base_dir/ "3.save/atmoData") # Moves into parserTools/3.save/atmoData
print("Save directory:", os.getcwd())

path = Path(csv_path) # saves same file name as og
filename= f"{path.stem}_parsed{path.suffix}"
dfs.to_csv(filename, index=False)

# print(df.head()) # check if it works
# print(df.columns.tolist())
# print(dfs.columns.tolist())



