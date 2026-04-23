import re

import json
import pandas as pd   

from datetime import datetime
from tzfpy import get_tz # to add timezone info based on lat/lon coordinates


def get_col_soft_inandexclude(df, include_keywords, exclude_keywords=None):
    """Finds columns with include_keywords and exclude_keywords (case-insensitive)."""
    
    include_pattern = '|'.join(include_keywords) # OR condition, filters for columns with ANY include_keyword 
    cols = df.columns[df.columns.str.contains(include_pattern, case=False, na=False)]
    
    if exclude_keywords:
        exclude_pattern = '|'.join(exclude_keywords) # and then, filters the included columns for columns with ANY exluded_keyword
        cols = cols[~cols.str.contains(exclude_pattern, case=False, na=False)]
    
    return df[cols]   

## Example: get_col_soft_inandexclude(df, ["pm" "temp"], ["ppm"])

def rename_col_hard_include(df, *args):
    """Finds columns iwth include_keywords (case-insensitive). 
    Sequentially runs through the list of (["keywords", "of, "old_name"], "new_name") pairs. 
    Renames each pair one at a time."""

    df = df.copy()
    mapping = {}
    used_cols = set()

    for i in range(0, len(args) - 1, 2):
        keywords = args[i]
        new_name = args[i + 1]

        matched = [
            col for col in df.columns
            if all(k.lower() in col.lower() for k in keywords) # k --> keyword, k.lower --> makes keyword lowercase, col.lower --> makes col lowercase ==> ensures case-insensitive
        ]       # all(...) --> ensures ALL keywords must be included 

        for col in matched:
            if col not in used_cols:
                mapping[col] = new_name
                used_cols.add(col)
                break

    return df.rename(columns=mapping)  

## Example: rename_col_hard_include(pm_df, 
                                # ["pm", "2", "5"], "pm2.5",
                                # ["pm", "10"], "pm10",
                                # ["pm", "1"], "pm1.0"
                                # );


def add_time_zoneandlocal(df, datetime_col='datetime', lon_col='lon', lat_col='lat'):
    """Adds four new columns "timezone", "localtime", "date", and "time" based on the datetime and lon/lat data. 
    Ensures that every row has a timezone and localtime value by forward filling timezone and localtime data from the last available valid lon/lat rows.
    For the first rows with null lon/lat, it backwards fills from the first available valid lon/lat row.
    Returns a new DataFrame with added timezone, localtime, date, and time columns."""

    df = df.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col], utc=True)
    df['timezone'] = None

    valid_coords = df[lat_col].notna() & df[lon_col].notna() # 1. finds first row with available lat/lon data
    df.loc[valid_coords, 'timezone'] = df[valid_coords].apply(
        lambda row: get_tz(row[lon_col], row[lat_col]), axis=1
    )

    df['timezone'] = df['timezone'].bfill() # 2. backward fills for all the rows before the first valid lat/lon row
    df['timezone'] = df['timezone'].ffill() # 3. forward fills for all the rows after the first valid lat/lon row

    df.loc[valid_coords, 'timezone'] = df[valid_coords].apply( # 4. check if the next row with valid lat/lon data is of a different timezone
        lambda row: get_tz(row[lon_col], row[lat_col]), axis=1
    )
    df['timezone'] = df['timezone'].ffill() # 5. forward fills again if the last valid lat/lon row was updated to a different timezone

    def to_localtime(row): # 6. converts the UTC datetime to local time based on the newly added timezone col
        if pd.notna(row['timezone']):
            local_dt = row[datetime_col].tz_convert(row['timezone'])
            return (
                # local_dt,                      # localtime: full timestamp with timezone (e.g., 2025-10-28 08:29:00+04:00)
                local_dt.date().isoformat(),   # date: YYYY-MM-DD
                local_dt.time().isoformat()    # time: HH:MM:SS
            )
        return pd.NaT, pd.NaT, pd.NaT

    df[['date', 'time']] = df.apply(to_localtime, axis=1, result_type='expand') # to get localtime: df[['localtime', 'date', 'time']]

    return df

## Example: add_time_zoneandlocal(gis_df, datetime_col='datetime', lon_col='lon', lat_col='lat') 

# ----------------------------------------------------------

def safe_json_loads(s):
    """Safely parse JSON string; returns empty dict on failure."""
    try:
        return json.loads(s) if pd.notna(s) else {}
    except (json.JSONDecodeError, TypeError):
        return {}

def flatten_dict(d, parent_key='', sep='.'):
    """Recursively flatten nested dictionaries."""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def process_jsoncol_csv(df, json_col='payload', flat_cols=None, output_path=None): # if flat_cols is none, defaults to timestamp
    """Process DataFrame with JSON column: parse, merge, flatten, and optionally save.
    Parameters:
        df (pd.DataFrame): Input DataFrame
        json_col (str): Column containing JSON strings
        flat_cols (list): Columns to keep flat (e.g., ['timestamp'])
        output_path (str, optional): Path to save JSON output. If None, skip saving.
    Returns:
        pd.DataFrame: Flattened DataFrame
    """
    if flat_cols is None:
        flat_cols = ['timestamp']
    
    df[json_col] = df[json_col].apply(safe_json_loads)
    
    data = []
    for _, row in df.iterrows():
        combined = {col: row[col] for col in flat_cols}
        combined.update(flatten_dict(row[json_col]))
        data.append(combined)
    
    result_df = pd.DataFrame(data)
    
    if output_path: # if output_path is provided, save as csv
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=4)
    
    return result_df   

# Example: df_flat = process_jsonifiedcsv(df, json_col='payload', flat_cols=['timestamp'])

