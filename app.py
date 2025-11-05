from flask import Flask, request, jsonify, render_template
from tradingview_screener import Query, col, And, Or
import pandas as pd
from datetime import datetime, timezone, time
import threading
from time import sleep
import pytz
import os
import rookiepy
import json
from scan import run_intraday_scan, save_scan_results, load_scan_results

app = Flask(__name__)

# --- Load configuration from config.json ---
with open('config.json', 'r') as f:
    config = json.load(f)

# --- Global state for scanner settings ---
scanner_settings = {
    # "market": "india",
    # "exchange": ["NSE"],
    "market": "america",
    "exchange": ["NASDAQ", "NYSE", "AMEX"],
    "min_price": 2,
    "max_price": 100000,
    "min_volume": 500000,
    "min_value_traded": 1000000,
    "RVOL_threshold": config['scanner_settings']['RVOL_threshold'],
    "beta_1_year": config['scanner_settings']['beta_1_year'],
    "db_name": config['database']['america']
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

    def get_all_fired_events(self, db_name):
        df = load_scan_results(db_name)
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

@app.route('/v2')
def index2():
    return render_template('index2.html')


@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Returns the latest cached scan data."""
    with data_lock:
        results = app_state.get_latest_scan_results()
        if results is None or results["fired"].empty:
            return jsonify({"fired": []})
        response_data = {
            "fired": results["fired"].to_dict(orient='records')
        }
    return jsonify(response_data)

data_lock = threading.Lock()




@app.route('/get_all_fired_events', methods=['GET'])
def get_all_fired_events():
    """Returns all fired squeeze events for the current day."""
    with data_lock:
        df = app_state.get_all_fired_events(scanner_settings['db_name'])
        
        if df.empty:
            return jsonify([])

        # The frontend will handle the time grouping, so we just return the sorted data.
        df_sorted = df.sort_values(by='fired_timestamp', ascending=False)

        # Use the built-in pandas method to handle all data types and formatting correctly:
        json_output = df_sorted.to_json(orient='records', indent=4, date_format='iso')
        return json_output


#import split
@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Updates the global scanner settings."""
    global scanner_settings, config
    new_settings = request.get_json()
    with data_lock:
        for key, value in new_settings.items():
            if key in scanner_settings:
                try:
                    if key == 'market':
                        scanner_settings['market'] = value
                        if value == 'india':
                            scanner_settings['db_name'] = config['database']['india']
                            scanner_settings['exchange'] = ["NSE"]
                        elif value == 'america':
                            scanner_settings['db_name'] = config['database']['america']
                            scanner_settings['exchange'] = ["NASDAQ", "NYSE", "AMEX"]
                    elif key in ['RVOL_threshold', 'beta_1_year']:
                        scanner_settings[key] = float(value)
                    elif isinstance(scanner_settings[key], int):
                        scanner_settings[key] = int(value)
                    else:
                        scanner_settings[key] = value
                except (ValueError, TypeError):
                    pass
    return jsonify({"status": "success", "settings": scanner_settings})

@app.route('/manual_scan', methods=['POST','GET'])
def manual_scan():
    """Triggers a manual scan and updates the app state."""
    print("Manual scan triggered.")
    with data_lock:
        current_settings = scanner_settings.copy()

    intraday_results = run_intraday_scan(current_settings, cookies, current_settings['db_name'])

    with data_lock:
        app_state.set_latest_scan_results(intraday_results)

    return jsonify({"status": "success", "message": "Manual scan completed."})

# def is_market_open():
#     """Checks if the Indian stock market is currently within trading hours (9:15 AM to 3:30 PM IST)."""
#     # IST = pytz.timezone('Asia/Kolkata')
#     # now_ist = datetime.now(IST)
#     # market_start = time(9, 15)
#     # market_end = time(15, 30)

#     # # Check if the current time is a weekday and within the trading window
#     # return (now_ist.weekday() < 5) and (market_start <= now_ist.time() <= market_end)
#     return True

import datetime as dt
from datetime import timedelta
import pytz

def get_market_status():
    """
    Checks the Indian stock market status and calculates time to sleep.
    Returns a dictionary with status (bool) and timeToSleep (int in seconds).
    """
    # Indian Stock Market trading hours
    market_open = dt.time(9, 15)
    market_close = dt.time(15, 30)

    # 2025 Indian stock market holidays (referencing NSE data)
    # shows some holidays but a full list is recommended for production.
    holidays = [
        dt.date(2025, 1, 26),  # Republic Day
        dt.date(2025, 2, 26),  # Mahashivratri
        dt.date(2025, 3, 14),  # Holi
        dt.date(2025, 3, 31),  # Mahavir Jayanti
        dt.date(2025, 4, 1),  # Annual Bank Closing
        dt.date(2025, 4, 18),  # Good Friday
        dt.date(2025, 5, 1),  # Maharashtra Day
        dt.date(2025, 8, 15),  # Independence Day
        dt.date(2025, 10, 2),  # Mahatma Gandhi Jayanti
        dt.date(2025, 10, 24),  # Diwali
        dt.date(2025, 11, 5),                       # 05/11/2025	Wednesday	Prakash Gurpurb Sri Guru Nanak Dev
                                #14	25/12/2025	Thursday	Christmas
        dt.date(2025, 12, 25) # Christmas
    ]

    # Get current time in IST
    ist = pytz.timezone('Asia/Kolkata')
    now = dt.datetime.now(ist)
    print(now.date())

    # Check for holidays and weekends
    if now.date() in holidays or now.weekday() >= 5:  # 5=Saturday, 6=Sunday
        print(" HOLIDAY or WEEKEND ")
        return calculate_time_to_open(now, market_open, holidays)

    # Check if market is open
    if market_open <= now.time() <= market_close:
        return {'status': True, 'timeToSleep': 60}
    else:
        return calculate_time_to_open(now, market_open, holidays)

def calculate_time_to_open(now, market_open, holidays):
    """
    Helper function to calculate time to next market open.
    """
    next_market_day = now.date()
    # Find the next trading day
    while True:
        if now.time() > market_open:
            next_market_day += timedelta(days=1)
        
        if next_market_day.weekday() < 5 and next_market_day not in holidays:
            break
        
        next_market_day += timedelta(days=1)
    
    # Calculate time difference
    ist  = pytz.timezone('Asia/Kolkata')
    open_datetime = ist.localize(dt.datetime.combine(next_market_day, market_open))
    time_difference = open_datetime - now
    
    return {'status': False, 'timeToSleep': int(time_difference.total_seconds())}



if __name__ == "__main__":
    def background_scanner():
        """Function to run scans in the background."""
        while True:
            IST = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(IST)
            # get_market_status()
            statusDict = get_market_status()
            # statusDict['timeToSleep']
            status = statusDict['status']
            timeToSleep = statusDict['timeToSleep']

            
            status = True
            timeToSleep = 60


            # {status,timeToSleep }= 
            print(status, timeToSleep)
            if status:
                
                print(f"Market is open. Running background scanner...{now_ist}")
                with data_lock:
                    current_settings = scanner_settings.copy()

                # Run intraday scan
                intraday_results = run_intraday_scan(current_settings, cookies, current_settings['db_name'])

                with data_lock:
                    app_state.set_latest_scan_results(intraday_results)
                sleep(30)
            else:
                print(f"Market is closed. Scanner is sleeping {timeToSleep} seconds. ")
                sleep(timeToSleep)

              # Check every 5 minutes

    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False , use_reloader=False)