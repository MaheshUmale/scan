import os
import urllib.parse
import json
from time import sleep
import threading
from datetime import datetime, timedelta, time, timezone
import numpy as np
import pandas as pd
from flask import Flask, render_template, jsonify, request
import simplejson
import asyncio 
import logging
import ssl
from collections import deque
import random
import requests
import traceback
import socketio
import websockets
from pymongo import MongoClient, ReplaceOne # Necessary for background_scanner
from asyncio import QueueEmpty
import pytz 
from typing import List, Dict, Optional
# Placeholder for the Squeeze Screener library:
from tradingview_screener import Query, col, And, Or 

# Import configuration and constants from the client module
# import LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR

# Mock functions for LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR
def create_bidirectional_mapping():
    """Mock function for instrument mapping"""
    return {}, {}, {}

def is_market_open():
    """Mock function to check if market is open"""
    return True 

# --- CONFIGURATION (MongoDB & Flask) ---
# Reuse the MongoDB config from the anomaly detector for consistency
# MONGO_URI = LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.MONGO_URI
# DATABASE_NAME = LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.DATABASE_NAME
# ALERTS_COLLECTION_NAME = LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.ALERTS_COLLECTION_NAME
# SQUEEZE_CONTEXT_COLLECTION_NAME = LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.SQUEEZE_CONTEXT_COLLECTION_NAME 


# MongoDB Settings
MONGO_URI = "mongodb://localhost:27017/"  
DATABASE_NAME = "upstox_data"
COLLECTION_NAME = "upstox_ticks"
ALERTS_COLLECTION_NAME = "anomaly_alerts"
SQUEEZE_CONTEXT_COLLECTION_NAME = "squeeze_context" # Scanner results for WSS subscription


# NEW: Collection for storing the history of all squeeze events 
SQUEEZE_HISTORY_COLLECTION_NAME = "squeeze_history_mongo" 
FIRED_EVENTS_COLLECTION_NAME = "fired_events_mongo"



# --- UTILITIES ---

def connect_db():
    """Synchronous MongoDB connection for Flask/Scanner threads."""
    global db
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # The ismaster call verifies that the connection is active
        client.admin.command('ping')
        client.admin.command('ismaster')
        db = client[DATABASE_NAME]
        print("✅ MongoDB connection established successfully in app.py")
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB in app.py: {e}")
        db = None

def datetime_to_iso(o):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(o, (datetime, time)):
        # Ensure it's timezone-aware before converting to ISO format
        if o.tzinfo is None or o.tzinfo.utcoffset(o) is None:
             return pytz.utc.localize(o).isoformat()
        return o.isoformat()
    # Handle numpy types if present in DataFrame output
    if isinstance(o, (np.integer, np.int64)):
        return int(o)
    if isinstance(o, (np.floating, np.float64)):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

connect_db()
# --- MongoDB Client Initialization (Global/Setup) ---
# try:
#     mongo_client = MongoClient(MONGO_URI)
#     db = mongo_client[DATABASE_NAME]
#     # Collection for the real-time, long-term squeeze state (used by WSS client)
#     squeeze_context_collection = db[SQUEEZE_CONTEXT_COLLECTION_NAME]
#     # Collection for a historical record of all *in-squeeze* scans
#     squeeze_history_collection = db[SQUEEZE_HISTORY_COLLECTION_NAME] 
#     # Collection for a historical record of *fired* events
#     fired_events_collection = db[FIRED_EVENTS_COLLECTION_NAME] 
    
#     print(f"MongoDB connected successfully. Target collections: {SQUEEZE_CONTEXT_COLLECTION_NAME}, {SQUEEZE_HISTORY_COLLECTION_NAME}, {FIRED_EVENTS_COLLECTION_NAME}")
    
#     mongo_client.admin.command('ping')
#     print("✅ Synchronous MongoDB connection successful for Flask API.")

# except Exception as e:
#     print(f"ERROR connecting to MongoDB in app.py: {e}")
#     mongo_client = None
#     db = None
#     squeeze_context_collection = None
#     squeeze_history_collection = None
#     fired_events_collection = None
fired_events_collection = db[FIRED_EVENTS_COLLECTION_NAME] 
squeeze_context_collection = db[SQUEEZE_CONTEXT_COLLECTION_NAME]
squeeze_history_collection = db[SQUEEZE_HISTORY_COLLECTION_NAME] 
# --- PERSISTENCE FUNCTIONS (MongoDB Replacements) ---

def load_previous_squeeze_list_from_mongo():
    """
    Loads the last complete squeeze scan from the MongoDB history collection.
    
    Returns:
        list: A list of (ticker, timeframe, volatility) tuples from the last scan.
    """
    if squeeze_history_collection is None:
        print("MongoDB connection failed. Cannot load previous squeeze context.")
        return []
    
    try:
        # 1. Find the most recent scan_timestamp
        latest_scan = squeeze_history_collection.find_one(
            {}, 
            sort=[('scan_timestamp', -1)], 
            projection={'scan_timestamp': 1}
        )
        if latest_scan is None:
            return []
        
        last_timestamp = latest_scan['scan_timestamp']
        
        # 2. Retrieve all records from that specific scan_timestamp
        cursor = squeeze_history_collection.find(
            {'scan_timestamp': last_timestamp},
            projection={'ticker': 1, 'timeframe': 1, 'volatility': 1, '_id': 0}
        )
        
        # 3. Format the data as a list of tuples
        return [(doc['ticker'], doc['timeframe'], doc['volatility']) for doc in cursor]
        
    except Exception as e:
        print(f"ERROR loading previous squeeze list from MongoDB: {e}")
        return []

def save_current_squeeze_list_to_mongo(squeeze_records: List[Dict]):
    """
    Saves the current complete squeeze scan to the MongoDB history collection.
    
    Args:
        squeeze_records (List[Dict]): A list of dictionaries, one per (ticker, timeframe) squeeze.
    """
    if not squeeze_records or squeeze_history_collection is None:
        if squeeze_history_collection is None:
            print("MongoDB connection failed. Cannot save squeeze history.")
        return
        
    try:
        now = datetime.now(timezone.utc)
        
        # Add the current timestamp to all records
        data_to_insert = []
        for r in squeeze_records:
            doc = r.copy()
            doc['scan_timestamp'] = now
            # Ensure proper types for MongoDB (e.g., convert numpy types)
            for key in ['volatility', 'rvol', 'SqueezeCount', 'HeatmapScore']:
                if key in doc and isinstance(doc[key], (np.generic, float)):
                    doc[key] = float(doc[key])
            data_to_insert.append(doc)

        # Bulk insert the current scan data
        squeeze_history_collection.insert_many(data_to_insert, ordered=False)
        print(f"Saved {len(data_to_insert)} records to {SQUEEZE_HISTORY_COLLECTION_NAME}.")
        
    except Exception as e:
        print(f"ERROR saving current squeeze list to MongoDB: {e}")

def save_fired_events_to_mongo(fired_events_df: pd.DataFrame):
    """
    Saves newly detected fired squeeze events to the MongoDB fired events collection.
    
    Args:
        fired_events_df (pd.DataFrame): DataFrame of newly fired events (already cleaned/reduced).
    """
    if fired_events_df.empty or fired_events_collection is None:
        if fired_events_collection is None:
            print("MongoDB connection failed. Cannot save fired events.")
        return
        
    try:
        # Convert DataFrame to list of dictionaries
        data_to_insert = fired_events_df.to_dict('records')
        
        # Convert pandas/numpy types to native Python types for MongoDB
        for doc in data_to_insert:
            if 'fired_timestamp' in doc and pd.notna(doc['fired_timestamp']):
                # Ensure it's a datetime object, converting from pandas Timestamp if necessary
                if isinstance(doc['fired_timestamp'], pd.Timestamp):
                    doc['fired_timestamp'] = doc['fired_timestamp'].to_pydatetime()
            
            # Convert boolean and float types, replace NaN with None
            for key, value in doc.items():
                if isinstance(value, (np.bool_, bool)):
                    doc[key] = bool(value)
                elif isinstance(value, (np.generic, float)) and np.isnan(value):
                    doc[key] = None # CRITICAL FIX: Replace NaN with None for MongoDB
                elif isinstance(value, (np.generic, float)):
                    doc[key] = float(value)
                elif pd.isna(value):
                    doc[key] = None # MongoDB prefers None over NaN
        
        # Filter out documents where 'ticker' is missing (shouldn't happen, but safety measure)
        data_to_insert = [doc for doc in data_to_insert if doc.get('ticker')]

        if data_to_insert:
            fired_events_collection.insert_many(data_to_insert, ordered=False)
            print(f"Saved {len(data_to_insert)} fired events to {FIRED_EVENTS_COLLECTION_NAME}.")
        
    except Exception as e:
        print(f"ERROR saving fired events to MongoDB: {e}")

def load_recent_fired_events_from_mongo(minutes_ago=15):
    """
    Loads fired events from the MongoDB collection within the last 'minutes_ago' period.
    
    Returns:
        pd.DataFrame: DataFrame of recent fired events.
    """
    if fired_events_collection is None:
        return pd.DataFrame()
        
    try:
        time_limit = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
        
        # Query for documents fired after the time limit
        cursor = fired_events_collection.find(
            {'fired_timestamp': {'$gte': time_limit}}
        ).sort('fired_timestamp', -1)
        
        df = pd.DataFrame(list(cursor))
        
        # Drop the MongoDB internal ID column
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
        
        # CRITICAL FIX: Replace NaN/NaT values with None
        df = df.replace({np.nan: None, pd.NaT: None})
        
        # --- START OF CHANGE: Convert UTC to IST for processing ---
        IST = pytz.timezone('Asia/Kolkata')
        
        # Convert the naive UTC timestamp to an IST timezone-aware datetime object
        df['fired_timestamp'] = pd.to_datetime(df['fired_timestamp']).apply(
            # Localize to UTC, convert to IST
            lambda x: x.tz_localize(pytz.utc).tz_convert(IST)
            if pd.notna(x) else None
        )
        # --- END OF CHANGE ---
        return df
        
    except Exception as e:
        print(f"ERROR loading recent fired events from MongoDB: {e}")
        return pd.DataFrame()

def load_all_day_fired_events_from_mongo():
    """
    Loads all fired events from the database for the current day,
    ensuring the DataFrame is ready for JSON serialization (NaNs replaced with None).
    """
    if fired_events_collection is None:
        return pd.DataFrame()
        
    try:
        # Get start of the current day in UTC
        now_utc = datetime.now(timezone.utc)
        today_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Query for documents fired today
        cursor = fired_events_collection.find(
            {'fired_timestamp': {'$gte': today_start_utc}}
        ).sort('fired_timestamp', -1)
        
        df = pd.DataFrame(list(cursor))
        
        if not df.empty:
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            
            # CRITICAL FIX: Replace NaN/NaT values with None (which becomes null in JSON)
            df = df.replace({np.nan: None, pd.NaT: None})
            
            # --- START OF CHANGE: CONVERT UTC TO IST FOR DISPLAY ---
            # 1. Define the target timezone (Indian Standard Time)
            IST = pytz.timezone('Asia/Kolkata')
            
            # 2. Convert timestamp column (currently UTC naive from MongoDB) to IST string format
            df['fired_timestamp'] = pd.to_datetime(df['fired_timestamp']).apply(
                # Localize the naive datetime object to UTC, convert to IST, and format.
                lambda x: x.tz_localize(pytz.utc).tz_convert(IST).isoformat()#.strftime('%Y-%m-%d %H:%M:%S %Z')
                if pd.notna(x) else None
            )
            # --- END OF CHANGE ---
            
        return df
        
    except Exception as e:
        print(f"ERROR loading all day fired events from MongoDB: {e}")
        return pd.DataFrame()
        
def cleanup_old_fired_events():
    """Removes fired events older than 7 days from the collection."""
    if fired_events_collection is None:
        return
    try:
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        result = fired_events_collection.delete_many(
            {'fired_timestamp': {'$lt': seven_days_ago}}
        )
        print(f"Cleaned up {result.deleted_count} old fired events from MongoDB.")
    except Exception as e:
        print(f"ERROR cleaning up old fired events in MongoDB: {e}")


# --- EXISTING FUNCTION REUSE/ADAPTATION ---

def save_squeeze_context_to_mongodb(dfs_dict: dict):
    # This function is retained from the original app.py. 
    # It updates the 'squeeze_context' collection for the WSS client.
    """
    Saves the multi-timeframe squeeze context from the scanner DataFrame 
    into the dedicated MongoDB collection.
    
    This replaces the document for each instrument_key with the latest status.
    """
    if squeeze_context_collection is None:
        print("MongoDB connection failed. Cannot save squeeze context.")
        return

    # 1. CONSOLIDATE DATA INTO A SINGLE DATAFRAME FOR MAPPING
    df_in_squeeze = dfs_dict.get("in_squeeze", pd.DataFrame()).copy()
    df_fired = dfs_dict.get("fired", pd.DataFrame()).copy()
    
    # 1a. Mark context explicitly
    if not df_in_squeeze.empty:
        df_in_squeeze['is_in_squeeze'] = True
    if not df_fired.empty and not df_in_squeeze.empty:
        df_fired['is_in_squeeze'] = False
        # Remove symbols in df_fired that are still in df_in_squeeze
        df_fired = df_fired[~df_fired['ticker'].isin(df_in_squeeze['ticker'])]
    
    final_df = pd.concat([df_in_squeeze, df_fired], ignore_index=True)

    if final_df.empty:
        print(" !! Squeeze scan returned no data to save to context DB.")
        return
    
    # result_dict  = final_df.to_dict(orient='dict')
    # 2. USE EXISTING API TO FORMAT DATA

    context_data_list = generate_heatmap_data((final_df) )

    # 3. MAP SYMBOL ('name') TO CORE INSTRUMENT ID AND PREPARE BULK WRITE
    operations = []
    
    instrument_to_name, name_to_instrument , name_to_tradingSymbol = create_bidirectional_mapping() 
    # for doc in context_data_list:
    for doc in context_data_list:
        symbol_NSE = str(doc.get('name') )
        if ":" in symbol_NSE :
            symbol = symbol_NSE.split(":")[1] # e.g., 'AXISBANK'
        else :
            symbol = symbol_NSE
            
        # Get the CORE ID (e.g., NSE_EQ|INE614G01033)
        core_instrument_id = name_to_instrument.get(symbol,symbol_NSE) 

        if core_instrument_id.startswith(symbol):
            print(f"Skipping {symbol}: Instrument key mapping failed.")
            continue
            
        # KEY CHANGE 1: Set the identifier field to "ticker" (WSS key)
        doc['ticker'] = core_instrument_id 
        
        # **CRITICAL ADDITION:** Set the human-readable name for API/UI use
        doc['tradingname'] = symbol # e.g., 'AXISBANK'
        
        # Ensure the 'is_in_squeeze' boolean status is available
        status_row = final_df[final_df['ticker'] == symbol].iloc[0] if symbol in final_df['ticker'].values else None
        if status_row is not None:
             doc['is_in_squeeze'] = bool(status_row.get('is_in_squeeze', False))
             
        # Cleanup: Remove the symbol field if you don't need it for the WSS client
        #doc.pop('name', None) 

        # KEY CHANGE 2: Filter the upsert operation using the "ticker" field
        op = ReplaceOne(
            {'ticker': core_instrument_id}, # Filter on the "ticker" field
            doc, 
            upsert=True
        )
        operations.append(op)

    # 4. EXECUTE BULK WRITE
    if operations:
        try:
            print( f" WRITING TO {SQUEEZE_CONTEXT_COLLECTION_NAME} COLLECTION ")
            result = squeeze_context_collection.bulk_write(operations, ordered=False)
            print(f"MongoDB context update complete. Saved/Updated {result.upserted_count + result.modified_count} instruments to squeeze_context.")
        except Exception as e:
            print(f"ERROR during MongoDB bulk write: {e}")
            
class CustomJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            # Convert datetime objects to ISO 8601 strings
            return obj.isoformat()
        # Handle numpy/pandas types like NaN by replacing with None for valid JSON
        if isinstance(obj, (np.generic, float)) and np.isnan(obj):
            return None
        # Fall back to the default simplejson encoder for other types
        return super().default(obj)
            
# --- Flask App Initialization ---
app = Flask(__name__)
# CRITICAL FIX: Use the custom encoder to handle NaNs/datetimes
app.json_encoder = CustomJSONEncoder 
# Initial DB connection
connect_db()

# ... [omitted global state and imports for brevity] ...

# --- Global state for auto-scanning ---
auto_scan_enabled = True
latest_scan_dfs = {
    "in_squeeze": pd.DataFrame(),
    "formed": pd.DataFrame(),
    "fired": pd.DataFrame()
}
data_lock = threading.Lock()

# --- Global state for scanner settings ---
scanner_settings = {
    "market": "india",
    "exchange": "NSE",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 500000,
    "min_value_traded": 10000000
}


# import rookiepy
cookies = None
try:
    # cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    # print("Successfully loaded TradingView cookies.")
    print("TradingView cookies not loaded (rookiepy not available). Scanning will continue without cookies.")
except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")


# --- Timeframe Configuration ---
timeframes = [ '|3', '|5', '|15', '|30', '|60', '|120', '|240','',  '|1W', '|1M']
tf_order_map = {'|1M': 10, '|1W': 9 , '': 8, '|240':7 , '|120': 6, '|60': 5, '|30': 4, '|15': 3, '|5': 2, '|3': 1}
tf_display_map = {'|3': '3m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H','': 'Daily', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}
VOLUME_THRESHOLDS = {
    '|3': 15000,
    '|5': 25000,
    '|15': 50000,
    '|30': 100000,
    '|60': 200000,
    '|120': 400000,
    '|240': 800000,
    '': 5000000, # Daily timeframe
    '|1W': 25000000, # Weekly timeframe
    '|1M': 100000000 # Monthly timeframe
}

# Construct select columns for all timeframes
select_cols = [
    'name', 'logoid', 'close', 'close|60m', 'MACD.hist', 'RSI', 'RSI|60m', 
    'SMA20', 'SMA20|60m', 'volume', 'volume|60m', 'average_volume_10d_calc', 
    'average_volume_10d_calc|60m', 'Value.Traded', 'Value.Traded|60m', 'beta_1_year'
]
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',
        f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
    ])

# --- Helper Functions ---

def generate_heatmap_data(df ):
    """
    Generates a simple, flat list of dictionaries from the dataframe for the D3 heatmap.
    """
    # --- Check and Convert Step ---
    if isinstance(df, dict):
        # Assuming the dictionary is structured like a DataFrame, 
        # e.g., 'orient='index' is often used for simple dict representation
        print("Input is a DICT. Converting to DataFrame.")
        df = pd.DataFrame.from_dict(df, orient='index', dtype=object)
    
    elif not isinstance(df, pd.DataFrame):
        # Handle cases where input is neither dict nor DataFrame
        raise TypeError(f"Expected dict or pandas.DataFrame, got {type(df)}")
    

    for c in ['ticker', 'HeatmapScore', 'SqueezeCount', 'rvol', 'URL', 'logo', 'momentum', 'highest_tf', 'squeeze_strength']:
        if c not in df.columns:
            # Note: This assigns 'N/A' or 0 to the entire new column
            df[c] = 'N/A' if c in ['momentum', 'highest_tf', 'squeeze_strength'] else 0

    heatmap_data = []
    # print(df.head())
    for index, row in df.iterrows():
        stock_data = {
            "name": row['ticker'],
            # Ensure proper handling of potential NaN values on the way out
            "value": row['HeatmapScore'] if pd.notna(row['HeatmapScore']) else None, 
            "count": row.get('SqueezeCount', 0) if pd.notna(row.get('SqueezeCount', 0)) else 0,
            "rvol": row['rvol'] if pd.notna(row['rvol']) else None,
            "url": row['URL'],
            "logo": row['logo'],
            "momentum": row['momentum'] if pd.notna(row['momentum']) else None,
            "highest_tf": row['highest_tf'] if pd.notna(row['highest_tf']) else None,
            "squeeze_strength": row['squeeze_strength'] if pd.notna(row['squeeze_strength']) else None
        }
        
        # Handle optional columns with NaN checks
        for col_name in ['fired_timeframe', 'previous_volatility', 'current_volatility', 'volatility_increased']:
            if col_name in df.columns:
                stock_data[col_name] = row[col_name] if pd.notna(row[col_name]) else None

        if 'fired_timestamp' in df.columns and pd.notna(row['fired_timestamp']):
            stock_data['fired_timestamp'] = row['fired_timestamp'].isoformat() if isinstance(row['fired_timestamp'], datetime) else str(row['fired_timestamp'])

        heatmap_data.append(stock_data)
    return heatmap_data



def get_highest_squeeze_tf(row):
    for tf_suffix in sorted(tf_order_map, key=tf_order_map.get, reverse=True):
        if row.get(f'InSqueeze{tf_suffix}', False): return tf_display_map[tf_suffix]
    return 'Unknown'

def get_dynamic_rvol(row, timeframe_name, tf_suffix_map):
    tf_suffix = tf_suffix_map.get(timeframe_name)
    if tf_suffix is None: return 0
    vol_col, avg_vol_col = f'volume{tf_suffix}', f'average_volume_10d_calc{tf_suffix}'
    volume, avg_volume = row.get(vol_col), row.get(avg_vol_col)
    
    # Ensure scalar values
    if hasattr(volume, 'iloc'):
        volume = volume.iloc[0] if len(volume) > 0 else None
    if hasattr(avg_volume, 'iloc'):
        avg_volume = avg_volume.iloc[0] if len(avg_volume) > 0 else None
    
    if pd.isna(volume) or pd.isna(avg_volume) or avg_volume == 0: return 0
    return volume / avg_volume

def get_dynamic_rvol_for_fired(row):
    """
    Calculates RVOL for a FIRED event using un-suffixed volume columns.
    Assumes columns 'volume' and 'average_volume_10d_calc' exist in the simplified Fired DF.
    """
    volume = row.get('volume')
    avg_volume = row.get('average_volume_10d_calc')
    if pd.isna(volume) or pd.isna(avg_volume) or avg_volume == 0: return 0
    return volume / avg_volume

def get_squeeze_strength(row):
    highest_tf_name = row['highest_tf']
    tf_suffix = tf_suffix_map.get(highest_tf_name)
    if tf_suffix is None: return "N/A"
    bb_upper, bb_lower = row.get(f'BB.upper{tf_suffix}'), row.get(f'BB.lower{tf_suffix}')
    kc_upper, kc_lower = row.get(f'KltChnl.upper{tf_suffix}'), row.get(f'KltChnl.lower{tf_suffix}')
    if any(pd.isna(val) for val in [bb_upper, bb_lower, kc_upper, kc_lower]): return "N/A"
    bb_width, kc_width = bb_upper - bb_lower, kc_upper - kc_lower
    if bb_width == 0: return "N/A"
    sqz_strength = kc_width / bb_width
    if sqz_strength >= 2: return "VERY STRONG"
    elif sqz_strength >= 1.5: return "STRONG"
    elif sqz_strength > 1: return "Regular"
    else: return "N/A"

def process_fired_events(events, tf_order_map, tf_suffix_map):
    if not events: return pd.DataFrame()
    df = pd.DataFrame(events)
    def get_tf_sort_key(display_name):
        suffix = tf_suffix_map.get(display_name, '')
        return tf_order_map.get(suffix, -1)
    df['tf_order'] = df['fired_timeframe'].apply(get_tf_sort_key)
    processed_events = []
    for ticker, group in df.groupby('ticker'):
        # Get the event from the highest timeframe that fired
        highest_tf_event = group.loc[group['tf_order'].idxmax()]
        consolidated_event = highest_tf_event.to_dict()
        # Count how many timeframes fired
        consolidated_event['SqueezeCount'] = group['fired_timeframe'].nunique()
        consolidated_event['highest_tf'] = highest_tf_event['fired_timeframe']
        processed_events.append(consolidated_event)
    return pd.DataFrame(processed_events)

def get_fired_breakout_direction_for_fired(row):
    """
    Determines breakout direction for a FIRED event using un-suffixed indicator columns
    from the simplified Fired DF.
    """
    # Columns are now un-suffixed for the simplification logic
    close, bb_upper, kc_upper = row.get('close'), row.get('BB.upper'), row.get('KltChnl.upper')
    bb_lower, kc_lower = row.get('BB.lower'), row.get('KltChnl.lower')
    
    if any(pd.isna(val) for val in [close, bb_upper, kc_upper, bb_lower, kc_lower]): return 'Neutral'
    
    # Bullish: Close > BB Upper AND BB Upper > KC Upper (Volatility expansion)
    if close > bb_upper and bb_upper > kc_upper: return 'Bullish'
    # Bearish: Close < BB Lower AND BB Lower < KC Lower
    elif close < bb_lower and bb_lower < kc_lower: return 'Bearish'
    else: return 'Neutral'

def ensure_scalar_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks if any column contains non-scalar (list, series, array) values 
    and converts them to a string representation if found, preventing ValueError 
    during array creation or serialization.
    """
    if df.empty:
        return df
        
    for col in df.columns:
        # Check if any element in the column is a list, tuple, array, or Series
        is_sequence = df[col].apply(lambda x: isinstance(x, (list, tuple, np.ndarray, pd.Series)))
        if is_sequence.any():
            print(f"⚠️ Warning: Column '{col}' contains sequences. Converting to string.")
            # Convert non-scalar values to a string representation
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, tuple, np.ndarray, pd.Series)) else x)
            
    return df

# --- Main Scanning Logic (Updated for MongoDB and Field Reduction) ---
def run_multi_strategy_scan(settings):
    """
    Run both high-potential (squeeze/breakout) and high-probability (trending/pullback) scans.
    """
    try:
        print("Running multi-strategy scan...")
        
        # Run high-potential squeeze/breakout scan
        breakout_results = run_scan(settings)
        
        # Run high-probability trending/pullback scan
        trending_results = run_trending_pullback_scan(settings)
        
        # Combine results
        combined_results = {
            "high_potential": breakout_results,
            "high_probability": trending_results,
            "combined": combine_strategy_results(breakout_results, trending_results)
        }
        
        return combined_results
        
    except Exception as e:
        print(f"Error in run_multi_strategy_scan: {e}")
        return {"high_potential": {}, "high_probability": {}, "combined": {}}

def run_trending_pullback_scan(settings):
    """
    Run the trending/pullback scan for high-probability setups.
    """
    try:
        print("Running trending/pullback scan...")
        
        # Removed cookie dependency for testing
        # if cookies is None:
        #     print("Skipping scan because cookies are not loaded.")
        #     return pd.DataFrame()
        
        # Query for trending stocks with pullbacks
        trending_conditions = [
            col('is_primary') == True, 
            col('typespecs').has('common'), 
            col('type') == 'stock',
            col('exchange') == settings['exchange'],
            col('close').between(settings['min_price'], settings['max_price']), 
            col('active_symbol') == True,
            col('average_volume_10d_calc') > settings['min_volume'], 
            col('Value.Traded') > settings['min_value_traded'],
            col('beta_1_year') > 0.8,
            # Defer volume multiplier to post-processing
            Or(
                And(col('RSI|60m').between(40, 60), col('close|60m') > col('SMA20|60m')),
                And(col('RSI') >= 45, col('close') > col('SMA20'))
            )
        ]
        
        query_trending = Query().select(*select_cols).where2(And(*trending_conditions)).set_markets(settings['market'])
        _, df_trending = query_trending.get_scanner_data(cookies=cookies)
        
        if df_trending is None or df_trending.empty:
            print("No trending/pullback stocks found.")
            return pd.DataFrame()

        # Clean specific columns that might contain lists
        cols_to_clean = [
            'volume|60m', 'average_volume_10d_calc|60m', 'RSI|60m', 'close|60m', 'SMA20|60m',
            'volume', 'average_volume_10d_calc', 'RSI', 'close', 'SMA20', 'Value.Traded'
        ]
        for col in cols_to_clean:
            if col in df_trending.columns:
                df_trending[col] = df_trending[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
                df_trending[col] = pd.to_numeric(df_trending[col], errors='coerce')

        # Post-filter for volume conditions
        df_trending = df_trending.dropna(subset=cols_to_clean) # Drop rows where conversion failed
        df_trending = df_trending[
            (
                (df_trending['volume|60m'] > df_trending['average_volume_10d_calc|60m'] * 1.2) & 
                (df_trending['RSI|60m'].between(40, 60)) & 
                (df_trending['close|60m'] > df_trending['SMA20|60m'])
            ) |
            (
                (df_trending['volume'] > df_trending['average_volume_10d_calc'] * 1.5) & 
                (df_trending['RSI'] >= 45) & 
                (df_trending['close'] > df_trending['SMA20'])
            )
        ]
        
        # Process trending results
        df_trending = process_trending_pullback_results(df_trending)
        df_trending['Potency_Score'] = df_trending.apply(scan.calculate_trending_potency_score, axis=1)
        
        # Add URLs and logos
        df_trending['URL'] = 'https://www.tradingview.com/chart/?symbol=' + df_trending['ticker'].str.replace(':', '')
        df_trending['logo'] = df_trending['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notnull(x) and x.strip() else '')
        
        # Add strategy type
        df_trending['Strategy_Type'] = 'High Probability (Trending/Pullback)'
        
        # Filter and sort
        df_trending = df_trending[df_trending['Potency_Score'] >= 50]  # Minimum potency threshold
        df_trending = df_trending.sort_values('Potency_Score', ascending=False)
        
        return df_trending
        
    except Exception as e:
        print(f"Error in run_trending_pullback_scan: {e}")
        return pd.DataFrame()

def process_trending_pullback_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process trending/pullback scan results to add derived columns.
    """
    # Determine primary timeframe and calculate derived metrics
    def get_trending_metrics(row):
        # Prefer daily timeframe for stronger trends
        rsi_1d = row.get('RSI', 50)
        rsi_60m = row.get('RSI|60m', 50)
        close_1d = row.get('close', 0)
        close_60m = row.get('close|60m', 0)
        sma20_1d = row.get('SMA20', 0)
        sma20_60m = row.get('SMA20|60m', 0)
        volume_1d = row.get('volume', 0)
        volume_60m = row.get('volume|60m', 0)
        avg_vol_1d = row.get('average_volume_10d_calc', 1)
        avg_vol_60m = row.get('average_volume_10d_calc|60m', 1)
        
        # Calculate trend strength based on RSI and price position relative to SMA
        trend_strength_1d = min(100, max(0, (rsi_1d - 30) * 2.5)) if close_1d > sma20_1d else min(100, max(0, (70 - rsi_1d) * 2.5))
        trend_strength_60m = min(100, max(0, (rsi_60m - 30) * 2.5)) if close_60m > sma20_60m else min(100, max(0, (70 - rsi_60m) * 2.5))
        
        # Calculate pullback depth based on RSI
        pullback_depth_1d = abs(50 - rsi_1d)
        pullback_depth_60m = abs(50 - rsi_60m)
        
        if trend_strength_1d >= 60 and 10 <= pullback_depth_1d <= 30:
            return {
                'timeframe': '1d',
                'trend_strength': trend_strength_1d,
                'pullback_depth': pullback_depth_1d,
                'momentum': 'Bullish' if close_1d > sma20_1d and volume_1d > avg_vol_1d * 1.5 else 'Bearish',
                'rsi': rsi_1d,
                'macd_signal': 'Bullish' if close_1d > sma20_1d else 'Bearish'
            }
        elif trend_strength_60m >= 55 and 5 <= pullback_depth_60m <= 25:
            return {
                'timeframe': '60m',
                'trend_strength': trend_strength_60m,
                'pullback_depth': pullback_depth_60m,
                'momentum': 'Bullish' if close_60m > sma20_60m and volume_60m > avg_vol_60m * 1.2 else 'Bearish',
                'rsi': rsi_60m,
                'macd_signal': 'Bullish' if close_60m > sma20_60m else 'Bearish'
            }
        else:
            return {
                'timeframe': '60m',
                'trend_strength': trend_strength_60m,
                'pullback_depth': pullback_depth_60m,
                'momentum': 'Neutral',
                'rsi': rsi_60m,
                'macd_signal': 'Neutral'
            }
    
    # Apply metrics calculation
    metrics_df = df.apply(get_trending_metrics, axis=1, result_type='expand')
    
    # Add calculated metrics to original dataframe
    for col in ['timeframe', 'trend_strength', 'pullback_depth', 'momentum', 'rsi', 'macd_signal']:
        df[col] = metrics_df[col]
    
    return df

def combine_strategy_results(breakout_results: dict, trending_results: pd.DataFrame) -> dict:
    """
    Combine high-potential and high-probability scan results.
    """
    try:
        # Convert trending results to same format as breakout results
        trending_records = trending_results.to_dict('records') if not trending_results.empty else []
        
        # Combine all results
        all_in_squeeze = breakout_results.get('in_squeeze', []) + trending_records
        
        # Sort by potency score (handle missing Potency_Score)
        if all_in_squeeze:
            all_in_squeeze.sort(key=lambda x: x.get('Potency_Score', x.get('HeatmapScore', 0)), reverse=True)
        
        return {
            "in_squeeze": all_in_squeeze,
            "formed": breakout_results.get('formed', []),
            "fired": breakout_results.get('fired', [])
        }
        
    except Exception as e:
        print(f"Error combining strategy results: {e}")
        return breakout_results

def run_scan(settings):
    """
    Runs a full squeeze scan, processes the data, saves it to MongoDB,
    and returns the processed dataframes.
    """
    # Removed cookie dependency for testing
        # if cookies is None:
        #     print("Skipping scan because cookies are not loaded.")
        #     return {
        #         "in_squeeze": pd.DataFrame(),
        #         "formed": pd.DataFrame(),
        #         "fired": pd.DataFrame()
        #     }
    try:
        # 1. Load previous squeeze state from MongoDB history
        prev_squeeze_pairs = load_previous_squeeze_list_from_mongo()
        
        # 2. Find all stocks currently in a squeeze or with a volume spike
        squeeze_conditions = [And(col(f'BB.upper{tf}') < col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') > col(f'KltChnl.lower{tf}')) for tf in timeframes]
        # volSpike = [And(col(f'volume{tf}') > 75000, col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 100)) for tf in timeframes]
        volSpikeWithBreak = [And(col(f'volume{tf}')  > VOLUME_THRESHOLDS[tf], 
                        col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 2),
                        Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), 
                           col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'))
                        ) for tf in timeframes  ] # Using a subset of TFs for efficiency/stability


        filters = [
            col('is_primary') == True, col('typespecs').has('common'), col('type') == 'stock',
            col('exchange') == settings['exchange'],
            col('close').between(settings['min_price'], settings['max_price']), col('active_symbol') == True,
            col('average_volume_10d_calc|5') > settings['min_volume'], col('Value.Traded|5') > settings['min_value_traded'],
            col('beta_1_year') > 1.2,
            Or( *volSpikeWithBreak)
        ]
        query_in_squeeze = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])

        _, df_in_squeeze = query_in_squeeze.get_scanner_data(cookies=cookies)

        print(f"Found {len(df_in_squeeze) if df_in_squeeze is not None else 0} stocks currently in a squeeze.")
        #print(df_in_squeeze)
        current_squeeze_pairs = []
        df_in_squeeze_processed = pd.DataFrame()
        if df_in_squeeze is not None and not df_in_squeeze.empty:
            for _, row in df_in_squeeze.iterrows():
                for tf_suffix, tf_name in tf_display_map.items():
                    #print(f'   {tf_name}    ::: BB.upper{tf_suffix}  :::::: KltChnl.upper{tf_suffix} FOR ----------------->'+row['ticker'])
                    bb_upper_val = row.get(f'BB.upper{tf_suffix}')
                    kc_upper_val = row.get(f'KltChnl.upper{tf_suffix}')
                    bb_lower_val = row.get(f'BB.lower{tf_suffix}')
                    kc_lower_val = row.get(f'KltChnl.lower{tf_suffix}')

                    # Ensure we have scalar values, not Series
                    if hasattr(bb_upper_val, 'iloc'):
                        bb_upper_val = bb_upper_val.iloc[0] if len(bb_upper_val) > 0 else None
                    if hasattr(kc_upper_val, 'iloc'):
                        kc_upper_val = kc_upper_val.iloc[0] if len(kc_upper_val) > 0 else None
                    if hasattr(bb_lower_val, 'iloc'):
                        bb_lower_val = bb_lower_val.iloc[0] if len(bb_lower_val) > 0 else None
                    if hasattr(kc_lower_val, 'iloc'):
                        kc_lower_val = kc_lower_val.iloc[0] if len(kc_lower_val) > 0 else None

                    if all(pd.notna(v) for v in [bb_upper_val, kc_upper_val, bb_lower_val, kc_lower_val]):
                        if bb_upper_val < kc_upper_val and bb_lower_val > kc_lower_val:
                            atr = row.get(f'ATR{tf_suffix}')
                            sma20 = row.get(f'SMA20{tf_suffix}')
                            bb_upper = row.get(f'BB.upper{tf_suffix}')
                            
                            # Ensure scalar values for volatility calculation
                            if hasattr(atr, 'iloc'):
                                atr = atr.iloc[0] if len(atr) > 0 else None
                            if hasattr(sma20, 'iloc'):
                                sma20 = sma20.iloc[0] if len(sma20) > 0 else None
                            if hasattr(bb_upper, 'iloc'):
                                bb_upper = bb_upper.iloc[0] if len(bb_upper) > 0 else None
                            
                            # Volatility is BB Width (BB.upper - SMA20) relative to ATR
                            volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                            current_squeeze_pairs.append((row['ticker'], tf_name, volatility))

            # --- Data Enrichment (Retained) ---
            df_in_squeeze['encodedTicker'] = df_in_squeeze['ticker'].apply(urllib.parse.quote)
            df_in_squeeze['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_in_squeeze['encodedTicker']
            df_in_squeeze['logo'] = df_in_squeeze['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
            for tf in timeframes:
                df_in_squeeze[f'InSqueeze{tf}'] = (df_in_squeeze[f'BB.upper{tf}'] < df_in_squeeze[f'KltChnl.upper{tf}']) & (df_in_squeeze[f'BB.lower{tf}'] > df_in_squeeze[f'KltChnl.lower{tf}'])
            df_in_squeeze['SqueezeCount'] = df_in_squeeze[[f'InSqueeze{tf}' for tf in timeframes]].sum(axis=1)
            df_in_squeeze['highest_tf'] = df_in_squeeze.apply(get_highest_squeeze_tf, axis=1)
            df_in_squeeze['squeeze_strength'] = df_in_squeeze.apply(get_squeeze_strength, axis=1)
            df_in_squeeze = df_in_squeeze[df_in_squeeze['squeeze_strength'].isin(['STRONG', 'VERY STRONG'])]
            df_in_squeeze['rvol'] = df_in_squeeze.apply(lambda row: get_dynamic_rvol(row, row['highest_tf'], tf_suffix_map), axis=1)
            df_in_squeeze['momentum'] = df_in_squeeze['MACD.hist'].apply(lambda x: 'Bullish' if x > 0 else 'Bearish' if x < 0 else 'Neutral')
            volatility_map = {(ticker, tf): vol for ticker, tf, vol in current_squeeze_pairs}
            df_in_squeeze['volatility'] = df_in_squeeze.apply(lambda row: volatility_map.get((row['ticker'], row['highest_tf']), 0), axis=1)
            df_in_squeeze['HeatmapScore'] = df_in_squeeze['rvol'] * df_in_squeeze['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_in_squeeze['volatility']

            # Create records for saving to squeeze_history_mongo
            ticker_data_map = {row['ticker']: row.to_dict() for _, row in df_in_squeeze.iterrows()}
            current_squeeze_records = [{'ticker': t, 'timeframe': tf, 'volatility': v, **ticker_data_map.get(t, {})} for t, tf, v in current_squeeze_pairs]
            df_in_squeeze_processed = df_in_squeeze
        else:
            current_squeeze_records = []

        # 3. Process event-based squeezes (Formed and Fired logic retained)
        prev_squeeze_set = {(ticker, tf) for ticker, tf, vol in prev_squeeze_pairs}
        current_squeeze_set = {(r['ticker'], r['timeframe']) for r in current_squeeze_records}

        # --- Refactored for efficient confluence lookup ---
        prev_squeeze_state = {}
        for ticker, tf, _ in prev_squeeze_pairs:
            if ticker not in prev_squeeze_state:
                prev_squeeze_state[ticker] = set()
            prev_squeeze_state[ticker].add(tf)

        # Newly Formed
        formed_pairs = current_squeeze_set - prev_squeeze_set
        df_formed_processed = pd.DataFrame()
        if formed_pairs:
            formed_tickers = list(set(ticker for ticker, tf in formed_pairs))
            df_formed_processed = df_in_squeeze_processed[df_in_squeeze_processed['ticker'].isin(formed_tickers)].copy()

        # Newly Fired
        fired_pairs = prev_squeeze_set - current_squeeze_set
        print(f" Previous Squeeze Count: {len(prev_squeeze_set)}")
        print(f" Current Squeeze Count: {len(current_squeeze_set)}")
        print(f" Newly Fired Count: {len(fired_pairs)}")
        
        if fired_pairs:
            fired_tickers = list(set(ticker for ticker, tf in fired_pairs))
            previous_volatility_map = {(ticker, tf): vol for ticker, tf, vol in prev_squeeze_pairs}
            
            # Re-query only the fired tickers to get their latest data
           
            query_fired = Query().select(*select_cols).set_tickers(*fired_tickers)
            # .where2(And(*filters)).set_markets(settings['market'])
            _, df_fired = query_fired.get_scanner_data(cookies=cookies)

            if df_fired is not None and not df_fired.empty:
                newly_fired_events = []
                df_fired_map = {row['ticker']: row for _, row in df_fired.iterrows()}
                
                for ticker, fired_tf_name in fired_pairs:
                    if ticker in df_fired_map:
                        row_data = df_fired_map[ticker]
                        tf_suffix = tf_suffix_map.get(fired_tf_name)
                        
                        if tf_suffix:
                            # Check for relative volume condition
                            relative_volume = get_dynamic_rvol(row_data, fired_tf_name, tf_suffix_map)
                            if relative_volume <= 1.5:
                                continue

                            previous_volatility = previous_volatility_map.get((ticker, fired_tf_name), 0.0) or 0
                            
                            # Fetch current volatility from the specific fired timeframe
                            atr, sma20, bb_upper = row_data.get(f'ATR{tf_suffix}'), row_data.get(f'SMA20{tf_suffix}'), row_data.get(f'BB.upper{tf_suffix}')
                            current_volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                            
                            if current_volatility > previous_volatility:
                                # --- Confluence Check ---
                                has_confluence = False
                                fired_tf_rank = tf_order_map.get(tf_suffix_map.get(fired_tf_name, ''), -1)
                                if ticker in prev_squeeze_state:
                                    for prev_tf in prev_squeeze_state[ticker]:
                                        prev_tf_rank = tf_order_map.get(tf_suffix_map.get(prev_tf, ''), -1)
                                        if prev_tf_rank > fired_tf_rank:
                                            has_confluence = True
                                            break

                                # --- FIELD REDUCTION: Create simplified fired event dictionary ---
                                fired_event = {
                                    'ticker': ticker,
                                    'name': row_data.get('name'), 
                                    'close': row_data.get('close'),
                                    'logoid': row_data.get('logoid'),
                                    'MACD.hist': row_data.get('MACD.hist'),
                                    # Specific indicator data for the FIRED timeframe (un-suffixed for helper functions)
                                    'BB.upper': row_data.get(f'BB.upper{tf_suffix}'),
                                    'BB.lower': row_data.get(f'BB.lower{tf_suffix}'),
                                    'KltChnl.upper': row_data.get(f'KltChnl.upper{tf_suffix}'),
                                    'KltChnl.lower': row_data.get(f'KltChnl.lower{tf_suffix}'),
                                    'volume': row_data.get(f'volume{tf_suffix}'),
                                    'average_volume_10d_calc': row_data.get(f'average_volume_10d_calc{tf_suffix}'),
                                    # Core calculated/status fields
                                    'fired_timeframe': fired_tf_name,
                                    'previous_volatility': previous_volatility,
                                    'current_volatility': current_volatility,
                                    'volatility_increased': True,
                                    'fired_timestamp': datetime.now(timezone.utc), 
                                    'confluence': has_confluence
                                }
                                newly_fired_events.append(fired_event)
                                print(f"Fired: {ticker} on {fired_tf_name} | Prev Vol: {previous_volatility:.4f}, Curr Vol: {current_volatility:.4f}, Confluence: {has_confluence}")    
                
                if newly_fired_events:
                    # Consolidated and enriched dataframe for saving/display
                    df_newly_fired = process_fired_events(newly_fired_events, tf_order_map, tf_suffix_map)
                    
                    df_newly_fired['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_newly_fired['ticker'].apply(urllib.parse.quote)
                    df_newly_fired['logo'] = df_newly_fired['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
                    
                    # Use the new helper functions that look for un-suffixed columns
                    df_newly_fired['rvol'] = df_newly_fired.apply(get_dynamic_rvol_for_fired, axis=1)
                    df_newly_fired['momentum'] = df_newly_fired.apply(get_fired_breakout_direction_for_fired, axis=1)
                    
                    df_newly_fired['squeeze_strength'] = np.where(df_newly_fired['confluence'], 'FIRED (Confluence)', 'FIRED')
                    df_newly_fired['HeatmapScore'] = df_newly_fired['rvol'] * df_newly_fired['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_newly_fired['current_volatility']
                    
                    # --- MONGO DB SAVE ---
                    save_fired_events_to_mongo(df_newly_fired)
                    print(f"Saved {len(df_newly_fired)} newly fired events to MongoDB.")

        # 4. Consolidate and prepare final data
        cleanup_old_fired_events() # Run periodic cleanup
        df_recent_fired = load_recent_fired_events_from_mongo(minutes_ago=15)
        if not df_recent_fired.empty:
            fired_events_list = df_recent_fired.to_dict('records')
            # Reprocess to consolidate if multiple timeframes fired recently
            df_recent_fired_processed = process_fired_events(fired_events_list, tf_order_map, tf_suffix_map)
            
            # Re-apply enrichment fields lost during consolidation
            df_recent_fired_processed['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_recent_fired_processed['ticker'].apply(urllib.parse.quote)
            df_recent_fired_processed['logo'] = df_recent_fired_processed['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
            df_recent_fired_processed['rvol'] = df_recent_fired_processed.apply(get_dynamic_rvol_for_fired, axis=1)
            df_recent_fired_processed['momentum'] = df_recent_fired_processed.apply(get_fired_breakout_direction_for_fired, axis=1)
            df_recent_fired_processed['squeeze_strength'] = np.where(df_recent_fired_processed['confluence'], 'FIRED (Confluence)', 'FIRED')
            df_recent_fired_processed['HeatmapScore'] = df_recent_fired_processed['rvol'] * df_recent_fired_processed['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_recent_fired_processed['current_volatility']

        else:
            df_recent_fired_processed = pd.DataFrame()


        # 5. Save current state to MongoDB history
        print(" SAVE SQZ to MONGO DB ---------------------------------"+str(len(current_squeeze_records)))
        save_current_squeeze_list_to_mongo(current_squeeze_records)

        # 6. Return processed dataframes


        # 6. Return processed dataframes
        # Apply cleaning to ensure no sequences are accidentally left in columns
        df_in_squeeze_processed = ensure_scalar_columns(df_in_squeeze_processed)
        df_formed_processed = ensure_scalar_columns(df_formed_processed)
        df_recent_fired_processed = ensure_scalar_columns(df_recent_fired_processed)
        
        return {
            "in_squeeze": df_in_squeeze_processed,
            "formed": df_formed_processed,
            "fired": df_recent_fired_processed
        }
    


    except Exception as e:
        print(f"An error occurred during scan: {e}")
        traceback.print_exc()
        return {
            "in_squeeze": pd.DataFrame(),
            "formed": pd.DataFrame(),
            "fired": pd.DataFrame()
        }


def background_scanner():
    """Function to run scans in the background."""
    while True:
        if is_market_open():
            print("Current time is between 9:00 AM and 3:30 PM. Performing action...")
            if auto_scan_enabled:
                print("Auto-scanning...")
                with data_lock:
                    current_settings = scanner_settings.copy()
                
                print("run_multi_strategy_scan")
                scan_result_dfs = run_multi_strategy_scan(current_settings)
                # This call updates the real-time context collection
                save_squeeze_context_to_mongodb(scan_result_dfs) 
                with data_lock:
                    global latest_scan_dfs
                    latest_scan_dfs = scan_result_dfs
                    # Assuming df_heatmap is the consolidated DataFrame

                    latest_scan_dfs["in_squeeze"] = scan_result_dfs.get("combined", {}).get("in_squeeze", [])
                    
                # --- START NEW LOGIC: PERSIST SQUEEZE CONTEXT TO MONGODB ---
            if not scan_result_dfs:
                print(f"Persisting {len(scan_result_dfs)} squeeze context documents to MongoDB...")
                
                # Get the collection
                context_collection = db[SQUEEZE_CONTEXT_COLLECTION_NAME]
                
                # Fields required for alerts enrichment and context
                required_cols = ['ticker', 'name', 'logo', 'URL', 'timeframe', 'type', 'rvol', 'timestamp']
                
                # Prepare a list of ReplaceOne operations
                operations = []
                
                # Convert DataFrame rows to MongoDB documents
                for index, row in scan_result_dfs.iterrows():
                    # Create a document containing only the required enrichment and context fields
                    doc = {col: row[col] for col in required_cols if col in row and pd.notna(row[col])}
                    
                    if doc and 'ticker' in doc:
                        # Use ReplaceOne to upsert the document, indexed by 'ticker'
                        operation = ReplaceOne(
                            {'ticker': doc['ticker']}, # Filter: find the existing document by ticker
                            doc,                       # Replacement: the new document data
                            upsert=True                # Upsert: insert if it doesn't exist
                        )
                        operations.append(operation)

                if operations:
                    # Perform bulk write
                    context_collection.bulk_write(operations)
                    print(f"Successfully performed bulk upsert for {len(operations)} context documents.")
                
            # --- END NEW LOGIC ---

            sleep(60)
        else :
            print(" OUTSIDE MARKET HOUR ,,Sleeping 5 mins")
            sleep(300)

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('SqueezeHeatmap.html')

@app.route('/fired')
def fired_page():
    return render_template('Fired.html')

@app.route('/anomaly_dashboard')
def anomaly_dashboard():
    return render_template('anomaly_dashboard.html')

# --- API ENDPOINT TO RETRIEVE ANOMALY ALERTS (Retained) ---

def datetime_to_iso(obj):
    """Helper function to serialize datetime objects to ISO format."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    # Handle ObjectId serialization from MongoDB
    if hasattr(obj, 'binary'):
        return str(obj)
    # Handle numpy/pandas types like NaN by replacing with None for valid JSON
    if isinstance(obj, (np.generic, float)) and np.isnan(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# @app.route('/api/alerts', methods=['GET'])
# def get_alerts():
#     if db is None:
#         return jsonify({"error": "Database not connected"}), 500
    
#     try:
#         alerts_collection = db[LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.ALERTS_COLLECTION_NAME]
#         print(" finding alerts")
#         alerts_cursor = alerts_collection.find({}) \
#             .sort('timestamp', -1) \
#             .limit(100)
#         print('Got cursor')
#         alerts_data = list(alerts_cursor)
#         symbol_URL_LOGO_nameDF = LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.symbol_URL_LOGO_nameDF
#         for alert in alerts_data:
#             symbolName = alert.get('tradingname')
#             matching_row = symbol_URL_LOGO_nameDF.loc[symbol_URL_LOGO_nameDF['name'] == symbolName, ['logo', 'URL']]
#             if not matching_row.empty:
#                 logo = matching_row['logo'].iloc[0]
#                 url = matching_row['URL'].iloc[0]
#                 if not pd.isna(logo) and  isinstance(logo, str) :
#                     alert['logo'] = logo
#                 if not pd.isna(url) and  isinstance(url, str) :
#                     alert['URL'] = url
                    
#         return jsonify(json.loads(json.dumps(alerts_data, default=datetime_to_iso))), 200

#     except Exception as e:
#         print(f"❌ Error fetching alerts from MongoDB: {e}")
#         return jsonify({"error": f"Failed to fetch alerts: {e}"}), 500


# app.py (Modified get_alerts function)


# app.py (Modified get_alerts function to fully populate long_term_context)

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """
    Fetches light-weight alerts and enriches them by joining with the squeeze_context
    collection, ensuring the full 'long_term_context' is populated.
    """
    if db is None:
        return jsonify({"error": "Database not connected"}), 500
    
    try:
        alerts_collection = db[ ALERTS_COLLECTION_NAME]
        
        # 1. Fetch recent alerts (Lightweight documents)
        alerts_cursor = alerts_collection.find({}) \
            .sort('timestamp', -1) \
            .limit(100)
        alerts_data = list(alerts_cursor)

        if not alerts_data:
            return jsonify([]), 200

        # 2. Extract unique instrument keys (e.g., 'NSE_EQ|INE...' or 'AXISBANK')
        _tickers = {alert.get('ticker') for alert in alerts_data if alert.get('ticker')}
        # Create a new set with the appended string
        unique_tickers = {'NSE_EQ|'+item  for item in _tickers}

        # print(new_set)


        # print(unique_tickers)
        # 3. Perform a single, efficient lookup (MongoDB JOIN) on the Squeeze Context collection
        squeeze_context_collection = db[SQUEEZE_CONTEXT_COLLECTION_NAME] 
        
        # CRITICAL FIX: Fetch ALL fields from the context, not just name/logo/URL, 
        # so the full long_term_context object can be reconstructed.
        projection_fields = {'_id': 0} # Fetch all fields except _id
        
        # Query: Find all context documents whose 'ticker' matches one of the unique alert tickers
        context_cursor = squeeze_context_collection.find(
            {'ticker': {'$in': list(unique_tickers)}}, 
            projection_fields
        )
        

        # for doc in context_cursor:
        #     print(doc)
        """
        {'name': 'NSE:CENTRALBK', 
        'value': 2.177532922261044, 
        'count': 2, 'rvol': 1.9500240492813015, 
        'url': 'https://in.tradingview.com/chart/N8zfIJVK/?symbol=NSE%3ACENTRALBK', 
        'logo': 'https://s3-symbol-logo.tradingview.com/central-bank-of-india.svg', 
        'momentum': 'Bullish', 'highest_tf': 'Weekly', 'squeeze_strength': 'STRONG', 
        'fired_timeframe': None, 'previous_volatility': None, 'current_volatility': None, 
        'volatility_increased': None, 
        'ticker': 'NSE_EQ|INE483A01010', 
        'tradingname': 'CENTRALBK'}
        """
        # Create a lookup map: {'TICKER': {full context document}}
        context_map = {doc['ticker']: doc for doc in context_cursor}

        # 4. Enrich the alerts_data with context from the map
        for alert in alerts_data:
            instrument_key = alert.get('ticker') 
            
            # Fetch the entire context document from the map
            context = context_map.get('NSE_EQ|'+instrument_key, {}) 
            
            # print("context")
            # print(context)
            # --- START MERGE/ENRICHMENT ---
            
            # 4a. RECONSTRUCT: Merge the fetched context into the alert's 'long_term_context' field
            # We start with the existing (lightweight) context and overwrite with the full data.
            current_long_term_context = alert.get('long_term_context', {})
            # This merges all fields (rvol, highest_tf, logo, etc.) from the context doc
            current_long_term_context.update(context) 
            alert['long_term_context'] = current_long_term_context
            
            # 4b. DISPLAY ENRICHMENT: Ensure root level fields for UI are correct (name, logo, URL)
            
            # Name: Use 'name' from context, fallback to 'tradingname' (short symbol)
            alert['name'] = context.get('name') if context.get('name') and isinstance(context.get('name'), str) else alert.get('tradingname')
            
            # Add logo (if valid)
            logo = context.get('logo')
            if logo and isinstance(logo, str) and logo.strip():
                alert['logo'] = logo
                    
            # Add URL (if valid)
            url = context.get('URL')
            if url and isinstance(url, str) and url.strip(): 
                alert['URL'] = url
            
            # --- END MERGE/ENRICHMENT ---
            
        return jsonify(json.loads(json.dumps(alerts_data, default=datetime_to_iso))), 200

    except Exception as e:
        print(f"❌ Error fetching alerts and enriching context: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to fetch alerts: {e}"}), 500

@app.route('/formed')
def formed_page():
    return render_template('Formed.html')

@app.route('/compact')
def compact_page():
    return render_template('CompactHeatmap.html')

@app.route('/scan', methods=['POST'])
def scan_endpoint():
    """Triggers a new scan and returns the filtered results."""
    try:
        # Get request parameters
        strategy_filter = request.json.get('strategy', 'all') if request.json else request.args.get('strategy', 'all')
        rvol_threshold = request.json.get('rvol', 0) if request.json else float(request.args.get('rvol', 0))
        
        with data_lock:
            current_settings = scanner_settings.copy()
        
        # Run multi-strategy scan
        scan_results = run_multi_strategy_scan(current_settings)
        
        # Select appropriate results based on strategy filter
        if strategy_filter == 'high_potential':
            results = scan_results.get("high_potential", {})
        elif strategy_filter == 'high_probability':
            results = scan_results.get("high_probability", {})
        else:  # 'all' or default
            results = scan_results.get("combined", {})
        
        # Apply RVOL filtering
        filtered_results = {
            "in_squeeze": [stock for stock in results.get("in_squeeze", []) if stock.get('rvol', 0) >= rvol_threshold],
            "formed": [stock for stock in results.get("formed", []) if stock.get('rvol', 0) >= rvol_threshold],
            "fired": [stock for stock in results.get("fired", []) if stock.get('rvol', 0) >= rvol_threshold]
        }
        
        # Generate heatmap data
        response_data = {
            "in_squeeze": generate_heatmap_data(filtered_results["in_squeeze"]),
            "formed": generate_heatmap_data(filtered_results["formed"]),
            "fired": generate_heatmap_data(filtered_results["fired"]),
            "strategy_summary": {
                "high_potential_count": len(scan_results.get("high_potential", {}).get("in_squeeze", [])),
                "high_probability_count": len(scan_results.get("high_probability", {}).get("in_squeeze", [])),
                "total_count": len(results.get("in_squeeze", []))
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error in /scan route: {e}")
        return jsonify({"in_squeeze": [], "formed": [], "fired": [], "strategy_summary": {}})

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Returns the latest cached scan data, with optional RVOL filtering."""
    
    try:
        strategy_filter = request.args.get('strategy', 'all')
        rvol_threshold = float(request.args.get('rvol', 0))
        
        scan_results = latest_scan_dfs
        
        # Filter results based on strategy
        if strategy_filter == 'high_potential':
            results = scan_results.get("high_potential", {})
        elif strategy_filter == 'high_probability':
            results = scan_results.get("high_probability", {})
        else:
            results = scan_results.get("combined", scan_results)
        
        # Apply RVOL filtering
        filtered_results = {
            "in_squeeze": [stock for stock in results.get("in_squeeze", []) if stock.get('rvol', 0) >= rvol_threshold],
            "formed": [stock for stock in results.get("formed", []) if stock.get('rvol', 0) >= rvol_threshold],
            "fired": [stock for stock in results.get("fired", []) if stock.get('rvol', 0) >= rvol_threshold]
        }
        
        response_data = {
            "in_squeeze": generate_heatmap_data(filtered_results["in_squeeze"]),
            "formed": generate_heatmap_data(filtered_results["formed"]),
            "fired": generate_heatmap_data(filtered_results["fired"]),
            "strategy_summary": {
                "high_potential_count": len(scan_results.get("high_potential", {}).get("in_squeeze", [])),
                "high_probability_count": len(scan_results.get("high_probability", {}).get("in_squeeze", [])),
                "total_count": len(results.get("in_squeeze", []))
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error in get_latest_data: {e}")
        # Fallback to original behavior
        scan_results = latest_scan_dfs
        response_data = {
            "in_squeeze": generate_heatmap_data(scan_results.get("in_squeeze", [])),
            "formed": generate_heatmap_data(scan_results.get("formed", [])),
            "fired": generate_heatmap_data(scan_results.get("fired", [])),
            "strategy_summary": {}
        }
        return jsonify(response_data)


@app.route('/toggle_scan', methods=['POST'])
def toggle_scan():
    global auto_scan_enabled
    data = request.get_json()
    auto_scan_enabled = data.get('enabled', auto_scan_enabled)
    return jsonify({"status": "success", "auto_scan_enabled": auto_scan_enabled})

@app.route('/get_all_fired_events', methods=['GET'])
def get_all_fired_events():
    """Returns all fired squeeze events for the current day from MongoDB."""
    # This now uses the corrected function that replaces NaNs with None.
    fired_events_df = load_all_day_fired_events_from_mongo()
    
    # We use jsonify on the dict to leverage the CustomJSONEncoder set on the app, 
    # but the load function already made the data clean.
    return jsonify(fired_events_df.to_dict('records'))

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
    # 1. Start the synchronous background scanner thread
    # This thread runs your synchronous scanning logic and saves to MongoDB
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    # 2. Start the synchronous Flask web server in a separate thread
    def run_flask():
        print("Starting Flask Web Dashboard on http://127.0.0.1:5001/...")
        app.run(debug=False, port=5001, use_reloader=False)

    # flask_thread = threading.Thread(target=run_flask, daemon=True)
    # flask_thread.start()
    run_flask()
    # # 3. Start the main asynchronous trading client in the main thread
    # print(f"Using Access Token: {LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.ACCESS_TOKEN[:10]}...{LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.ACCESS_TOKEN[-10:]}")
    # print(f"Starting the Unified Live WSS Client and Anomaly Detector (Async)...")
    # try:
    #     # This blocks the main thread and runs the WSS client/MongoDB tasks
    #     asyncio.run(LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.main())
    # except KeyboardInterrupt:
    #     print("Program interrupted and shut down.")
    # except RuntimeError as e:
    #     if "cannot run" in str(e):
    #          asyncio.get_event_loop().run_until_complete(LIVE_WSS_CLIENT_n_ANOMALY_DETECTOR.main())
    #     else:
    #          raise e