from flask import Flask, request, jsonify, render_template
from tradingview_screener import Query, col, And, Or
import pandas as pd
from datetime import datetime, timezone
import threading
from time import sleep
import os
import rookiepy
from scan import run_intraday_scan, save_scan_results, load_scan_results

app = Flask(__name__)

# --- Global state for scanner settings ---
scanner_settings = {
    "market": "india",
    "exchange": "nse",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 500000,
    "min_value_traded": 10000000
}

cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

class AppState:
    def __init__(self):
        # self.all_fired_events = []
        self.latest_scan_results = {"fired": pd.DataFrame()}

    # def add_fired_events(self, new_fired_events):
    #     self.all_fired_events.extend(new_fired_events)

    def get_all_fired_events(self):
        df = load_scan_results()
        # The upsert logic in scan.py should prevent most duplicates, but this is a safeguard.
        df_cleaned = df.drop_duplicates(subset=['name', 'fired_timestamp'], keep='first')

        # Sort by the original fired timestamp to show the most recent events first.
        if 'fired_timestamp' in df_cleaned.columns and not df_cleaned.empty:
            df_cleaned['fired_timestamp'] = pd.to_datetime(df_cleaned['fired_timestamp'])
            return df_cleaned.sort_values(by='fired_timestamp', ascending=False)

        return df_cleaned


    def set_latest_scan_results(self, results):
        self.latest_scan_results = results

    def get_latest_scan_results(self):
        return self.latest_scan_results

app_state = AppState()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Returns the latest cached scan data."""
    with data_lock:
        results = app_state.get_latest_scan_results()
        response_data = {
            "fired": results["fired"].to_dict(orient='records')
        }
    return jsonify(response_data)

data_lock = threading.Lock()




 # The pandas Timestamp type is used when a column has a datetime dtype
from pandas import Timestamp
from datetime import datetime
from math import floor
import pandas as pd

# 1. Define the time_ago function (as before)
def time_ago(date_string):
    """
    Converts a date string into a fixed "time ago" bucket.
    """
    if not date_string:
        return 'N/A'
 
    # print(date_string)
    # print(type(date_string) )
    try:
        # if isinstance(date_string, (datetime, Timestamp)):
        #     date_time = date_string
        # else :
        #     date_time = datetime.fromisoformat(date_string)
        now = datetime.now()
        # print("========================================================")
        # print(now)
        # print(date_string)
        time_difference = now - date_string
        minutes_difference = floor(time_difference.total_seconds() / 60)
        minutes_difference = abs(minutes_difference)
        # print("========================================================")
        # print(minutes_difference)

        # Logic to assign to fixed time buckets
        if minutes_difference <= 5: 
            return '5m ago'
        if minutes_difference <= 15: 
            return '15m ago'
        if minutes_difference <= 30: 
            return '30m ago'
        if minutes_difference <= 60: 
            return '1 hr ago' 
        if minutes_difference <= 120: 
            return '2 hr ago'
        if minutes_difference <= 180: 
            return '3 hr ago'
        if minutes_difference <= 240: 
            return '4 hr ago'
        if minutes_difference <= 300: 
            return '5 hr ago'
        if minutes_difference <= 360: 
            return '6 hr ago'

        hours_difference = floor(minutes_difference / 60)
        return f'{hours_difference} hr ago'
        
    except Exception:
        #print stacktrace
        import traceback
        traceback.print_exc()
        
        return 'Invalid Time'


 
import pandas as pd
import json
@app.route('/get_all_fired_events', methods=['GET'])
def get_all_fired_events():
    """Returns all fired squeeze events for the current day."""
    with data_lock:
        df = app_state.get_all_fired_events()
        df['time_ago_group'] = df['fired_timestamp'].apply(time_ago) 
        
        # 1. Sort values by the group and name (optional, but ensures consistent removal)
        df_sorted = df.sort_values(['time_ago_group', 'name'])

        # 2. Use drop_duplicates()
        #    We keep the first occurrence where the combination of 'time_ago_group' AND 'name' is the same.
        #    The result keeps all original columns but removes the second 'Alice' entry.
        filtered_df = df_sorted.drop_duplicates(subset=['time_ago_group', 'name'], keep='first')



        # Use the built-in pandas method to handle all data types and formatting correctly:
        json_output = filtered_df.to_json(orient='records', indent=4, date_format='iso') 
        return json_output



@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Updates the global scanner settings."""
    global scanner_settings
    new_settings = request.get_json()
    with data_lock:
        for key, value in new_settings.items():
            if key in scanner_settings:
                try:
                    if isinstance(scanner_settings[key], int):
                        scanner_settings[key] = int(value)
                    elif isinstance(scanner_settings[key], float):
                        scanner_settings[key] = float(value)
                    else:
                        scanner_settings[key] = value
                except (ValueError, TypeError):
                    pass
    return jsonify({"status": "success", "settings": scanner_settings})

if __name__ == "__main__":
    def background_scanner():
        """Function to run scans in the background."""
        while True:
            print("Running background scanner...")
            with data_lock:
                current_settings = scanner_settings.copy()

            

            # Run intraday scan
            intraday_results = run_intraday_scan(current_settings, cookies)
            # print(intraday_results)

            with data_lock:
                app_state.set_latest_scan_results(intraday_results)
                # app_state.add_fired_events(intraday_results["fired"].to_dict(orient='records'))

            sleep(300) # Scan every 5 minutes

    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False , use_reloader=False)