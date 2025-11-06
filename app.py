# Import necessary libraries
from flask import Flask, request, jsonify, render_template
from tradingview_screener import Query, col, And, Or
import pandas as pd
from datetime import datetime, timezone, time
import threading
from time import sleep
import pytz
import os
import rookiepy
import configparser
import argparse
from scan import run_intraday_scan, save_scan_results, load_scan_results

# Initialize Flask app
app = Flask(__name__)

# --- Load configuration from config.properties ---
# Read the configuration file
config = configparser.ConfigParser()
config.read('config.properties')

# --- Argument parser for market selection ---
# Set up an argument parser to select the market at runtime
parser = argparse.ArgumentParser(description='Run the stock screener application.')
parser.add_argument('--market', type=str, default='america', choices=['india', 'america'],
                    help='The market to run the scanner for (india or america).')
args = parser.parse_args()

# --- Initialize scanner settings based on the selected market ---
# Load the settings for the selected market from the config file
market_config = config[args.market]
scanner_settings = {
    "market": args.market,
    "exchange": ["NASDAQ", "NYSE", "AMEX"] if args.market == 'america' else ["NSE"],
    "min_price": 2,
    "max_price": 100000,
    "min_volume": 500000,
    "min_value_traded": 1000000,
    "RVOL_threshold": market_config.getfloat('RVOL_threshold'),
    "beta_1_year": market_config.getfloat('beta_1_year'),
    "db_name": market_config.get('db_name')
}

# --- Load TradingView cookies ---
# Try to load cookies to enable scanning
cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

# --- AppState Class ---
# Manages the application's state, including scan results
class AppState:
    def __init__(self):
        self.latest_scan_results = {"fired": pd.DataFrame()}

    def get_all_fired_events(self, db_name):
        df = load_scan_results(db_name)
        df_cleaned = df.drop_duplicates(subset=['name', 'fired_timestamp'], keep='first')
        if 'fired_timestamp' in df_cleaned.columns and not df_cleaned.empty:
            df_cleaned['fired_timestamp'] = pd.to_datetime(df_cleaned['fired_timestamp'])
            return df_cleaned.sort_values(by='fired_timestamp', ascending=False)
        return df_cleaned

    def set_latest_scan_results(self, results):
        self.latest_scan_results = results

    def get_latest_scan_results(self):
        return self.latest_scan_results

app_state = AppState()

# --- Flask Routes ---
@app.route('/')
def index():
    """Render the main dashboard page."""
    return render_template('index.html')

@app.route('/v2')
def index2():
    """Render the treemap view page."""
    return render_template('index2.html')

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Return the latest cached scan data."""
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
    """Return all fired squeeze events for the current day."""
    with data_lock:
        df = app_state.get_all_fired_events(scanner_settings['db_name'])
        if df.empty:
            return jsonify([])
        df_sorted = df.sort_values(by='fired_timestamp', ascending=False)
        json_output = df_sorted.to_json(orient='records', indent=4, date_format='iso')
        return json_output

@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Update the global scanner settings."""
    global scanner_settings
    new_settings = request.get_json()
    with data_lock:
        for key, value in new_settings.items():
            if key in scanner_settings:
                try:
                    if key == 'market':
                        scanner_settings['market'] = value
                        market_config = config[value]
                        scanner_settings['db_name'] = market_config.get('db_name')
                        scanner_settings['exchange'] = ["NASDAQ", "NYSE", "AMEX"] if value == 'america' else ["NSE"]
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
    """Trigger a manual scan and update the app state."""
    print("Manual scan triggered.")
    with data_lock:
        current_settings = scanner_settings.copy()
    intraday_results = run_intraday_scan(current_settings, cookies, current_settings['db_name'])
    with data_lock:
        app_state.set_latest_scan_results(intraday_results)
    return jsonify({"status": "success", "message": "Manual scan completed."})

import datetime as dt
from datetime import timedelta

def get_market_status_india():
    """Check the Indian stock market status and calculate time to sleep."""
    market_open = dt.time(9, 15)
    market_close = dt.time(15, 30)
    holidays = [
        dt.date(2025, 1, 26), dt.date(2025, 2, 26), dt.date(2025, 3, 14),
        dt.date(2025, 3, 31), dt.date(2025, 4, 1), dt.date(2025, 4, 18),
        dt.date(2025, 5, 1), dt.date(2025, 8, 15), dt.date(2025, 10, 2),
        dt.date(2025, 10, 24), dt.date(2025, 11, 5), dt.date(2025, 12, 25)
    ]
    ist = pytz.timezone('Asia/Kolkata')
    now = dt.datetime.now(ist)
    if now.date() in holidays or now.weekday() >= 5:
        return calculate_time_to_open(now, market_open, holidays, ist)
    if market_open <= now.time() <= market_close:
        return {'status': True, 'timeToSleep': 60}
    else:
        return calculate_time_to_open(now, market_open, holidays, ist)

def get_market_status_america():
    """Check the American stock market status and calculate time to sleep."""
    market_open = dt.time(9, 30)
    market_close = dt.time(16, 0)
    holidays = [
        dt.date(2025, 1, 1), dt.date(2025, 1, 20), dt.date(2025, 2, 17),
        dt.date(2025, 4, 18), dt.date(2025, 5, 26), dt.date(2025, 7, 4),
        dt.date(2025, 9, 1), dt.date(2025, 11, 27), dt.date(2025, 12, 25)
    ]
    est = pytz.timezone('US/Eastern')
    now = dt.datetime.now(est)
    if now.date() in holidays or now.weekday() >= 5:
        return calculate_time_to_open(now, market_open, holidays, est)
    if market_open <= now.time() <= market_close:
        return {'status': True, 'timeToSleep': 60}
    else:
        return calculate_time_to_open(now, market_open, holidays, est)

def calculate_time_to_open(now, market_open, holidays, tz):
    """Calculate the time to the next market open."""
    next_market_day = now.date()
    while True:
        if now.time() > market_open:
            next_market_day += timedelta(days=1)
        if next_market_day.weekday() < 5 and next_market_day not in holidays:
            break
        next_market_day += timedelta(days=1)
    open_datetime = tz.localize(dt.datetime.combine(next_market_day, market_open))
    time_difference = open_datetime - now
    return {'status': False, 'timeToSleep': int(time_difference.total_seconds())}

def background_scanner():
    """Run scans in the background."""
    while True:
        if scanner_settings['market'] == 'india':
            statusDict = get_market_status_india()
        else:
            statusDict = get_market_status_america()

        status = statusDict['status']
        timeToSleep = statusDict['timeToSleep']

        if status:
            print(f"Market is open. Running background scanner...{datetime.now(pytz.timezone('Asia/Kolkata'))}")
            with data_lock:
                current_settings = scanner_settings.copy()
            intraday_results = run_intraday_scan(current_settings, cookies, current_settings['db_name'])
            with data_lock:
                app_state.set_latest_scan_results(intraday_results)
            sleep(30)
        else:
            print(f"Market is closed. Scanner is sleeping {timeToSleep} seconds.")
            sleep(timeToSleep)

if __name__ == "__main__":
    # Start the background scanner thread
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    # Run the Flask app
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False, use_reloader=False)
