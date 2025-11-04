from flask import Flask, request, jsonify, render_template
from tradingview_screener import Query, col, And, Or
import pandas as pd
from datetime import datetime, timezone, time
import threading
from time import sleep
import pytz
import os
import rookiepy
from scan import run_intraday_scan, save_scan_results, load_scan_results

app = Flask(__name__)

# --- Global state for scanner settings ---
scanner_settings = {
    # "market": "india",
    # "exchange": "NSE",
    "market" : "america",
    "exchange": ["NASDAQ","NYSE","AMEX"],
    "min_price": 2,
    "max_price": 100000,
    "min_volume": 500000,
    "min_value_traded": 1000000
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
        df = app_state.get_all_fired_events()
        
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
                    if key == 'exchange':
                        listExchanges = str(value).split(',')
                        scanner_settings[key] = listExchanges
                    elif key == 'market':
                        listmarket = str(value).spli(',')
                        scanner_settings[key] = listmarket
                    else:
                        scanner_settings[key] = value
                except (ValueError, TypeError):
                    pass
    return jsonify({"status": "success", "settings": scanner_settings})

@app.route('/manual_scan', methods=['POST'])
def manual_scan():
    """Triggers a manual scan and updates the app state."""
    print("Manual scan triggered.")
    with data_lock:
        current_settings = scanner_settings.copy()

    intraday_results = run_intraday_scan(current_settings, cookies)

    with data_lock:
        app_state.set_latest_scan_results(intraday_results)

    return jsonify({"status": "success", "message": "Manual scan completed."})

def is_market_open():
    """Checks if the Indian stock market is currently within trading hours (9:15 AM to 3:30 PM IST)."""
    # IST = pytz.timezone('Asia/Kolkata')
    # now_ist = datetime.now(IST)
    # market_start = time(9, 15)
    # market_end = time(15, 30)

    # # Check if the current time is a weekday and within the trading window
    # return (now_ist.weekday() < 5) and (market_start <= now_ist.time() <= market_end)
    return True


if __name__ == "__main__":
    def background_scanner():
        """Function to run scans in the background."""
        while True:
            IST = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(IST)
            if is_market_open():
                
                print(f"Market is open. Running background scanner...{now_ist}")
                with data_lock:
                    current_settings = scanner_settings.copy()

                # Run intraday scan
                intraday_results = run_intraday_scan(current_settings, cookies)

                with data_lock:
                    app_state.set_latest_scan_results(intraday_results)
                sleep(30)
            else:
                print(f"Market is closed. Scanner is sleeping 5min. {now_ist}")
                sleep(300)

              # Check every 5 minutes

    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False , use_reloader=False)