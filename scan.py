
# Import necessary libraries
from tradingview_screener import Query, col, And, Or
import pandas as pd
from pymongo import MongoClient
import os

# --- Constants ---
# Define volume thresholds for different timeframes
VOLUME_THRESHOLDS = {
    '|1': 25000, '|5': 100000, '|15': 300000, '|30': 500000,
    '|60': 1000000, '|120': 2000000, '|240': 4000000, '': 5000000,
    '|1W': 25000000, '|1M': 100000000
}

# Define timeframes and their mappings for ordering, display, and suffixing
timeframes = ['|1', '|5', '|15', '|30', '|60', '|120', '|240', '', '|1W', '|1M']
tf_order_map = {'|1': 1, '|5': 2, '|15': 3, '|30': 4, '|60': 5, '|120': 6, '|240': 7, '': 8, '|1W': 9, '|1M': 10}
tf_display_map = {'|1': '1m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H', '': 'Daily', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}

# Define weights and scores for potency calculation
POTENCY_WEIGHTS = {'RVOL': 2.0, 'TF_Order': 1.5, 'Breakout_Type_Score': 1.0}
BREAKOUT_TYPE_SCORES = {'Both': 3, 'Squeeze': 2, 'Donchian': 1, 'None': 0}

# Define UI columns
UI_COLUMNS = ['name', 'ticker', 'logoid', 'relative_volume_10d_calc', 'Breakout_TFs', 'fired_timestamp', 'momentum', 'breakout_type', 'highest_tf', 'Potency_Score']

# Construct select columns for all timeframes
select_cols = ['name', 'logoid', 'close', 'relative_volume_10d_calc', 'beta_1_year']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}', f'DonchCh20.Upper{tf}', f'DonchCh20.Lower{tf}',
        f'KltChnl.lower[1]{tf}', f'KltChnl.upper[1]{tf}', f'BB.lower[1]{tf}', f'BB.upper[1]{tf}', f'DonchCh20.Upper[1]{tf}', f'DonchCh20.Lower[1]{tf}',
        f'ATRP{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'close{tf}', f'Value.Traded{tf}', f'MACD.hist{tf}', f'MACD.hist[1]{tf}'
    ])

def run_intraday_scan(settings, cookies, db_name):
    """Run the intraday scan for breakouts."""
    if cookies is None:
        return {"fired": pd.DataFrame()}

    # Define base filters for the scan
    base_filters = [
        col('beta_1_year') > settings['beta_1_year'],
        col('is_primary') == True,
        col('typespecs').has(['', 'common', 'foreign-issuer']),
        col('type').isin(['dr', 'stock']),
        col('close').between(settings['min_price'], settings['max_price']),
        col('active_symbol') == True,
        col('Value.Traded|5') > settings['min_value_traded'],
    ]
    # Define trend, breakout, and volume spike filters
    trendFilter = [Or(And(col(f'EMA20{tf}') > col(f'EMA200{tf}'), col(f'close{tf}') > col(f'EMA20{tf}')), And(col(f'EMA20{tf}') < col(f'EMA200{tf}'), col(f'close{tf}') < col(f'EMA20{tf}'))) for tf in timeframes]
    donchian_break = [Or(col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'), col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}')) for tf in timeframes]
    squeeze_breakout = [Or(And(col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'), col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}')), And(col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'), col(f'BB.lower{tf}') <= col(f'KltChnl.lower[1]{tf}'))) for tf in timeframes]
    vol_spike = [Or(col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), settings['RVOL_threshold'] - 1), col(f'relative_volume_10d_calc{tf}') > settings['RVOL_threshold'], col('relative_volume_intraday|5') > settings['RVOL_threshold']) for tf in timeframes]

    # Combine filters and create query
    filters = [And(*base_filters), And(*vol_spike), Or(*squeeze_breakout, *donchian_break, *trendFilter)]
    query = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market']).limit(1000).set_property('symbols', {'query': {}})

    try:
        _, df = query.get_scanner_data(cookies=cookies)
        if df is not None:
            df = df.fillna(value=pd.NA).replace({pd.NA: None}).replace([float('inf'), float('-inf')], None).where(pd.notnull(df), None)
            if not df.empty:
                df = identify_combined_breakout_timeframes(df, timeframes)
                df = calculate_potency_score(df, tf_order_map, tf_suffix_map)
            else:
                return {"fired": pd.DataFrame()}
        else:
            return {"fired": pd.DataFrame()}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"fired": pd.DataFrame()}

    # Process and save new breakouts
    df_New = df.drop_duplicates(subset=['name']).copy()
    df_New = df_New[df_New['breakout_type'] != 'None'].copy()
    if df_New.empty:
        return {"fired": pd.DataFrame()}
    df_New['fired_timestamp'] = pd.Timestamp.now()
    df_New['momentum'] = df_New['momentum_score'].apply(lambda x: 'Bullish' if x > 0 else ('Bearish' if x < 0 else 'Neutral'))
    df_New = df_New.replace([float('inf'), float('-inf')], None).where(pd.notnull(df_New), None)
    df_filtered = df_New[[col for col in UI_COLUMNS if col in df_New.columns]].copy()
    save_scan_results(df_filtered, db_name)
    return {"fired": df_filtered}

def identify_combined_breakout_timeframes(df: pd.DataFrame, timeframes: list) -> pd.DataFrame:
    """Identify and combine breakout timeframes."""
    if df is None or df.empty:
        return pd.DataFrame()
    squeeze_indicators, donchian_indicators = {}, {}
    for tf in timeframes:
        upper_breakout = (df[f'BB.upper[1]{tf}'] < df[f'KltChnl.upper[1]{tf}']) & (df[f'BB.upper{tf}'] >= df[f'KltChnl.upper{tf}'])
        lower_breakout = (df[f'BB.lower[1]{tf}'] > df[f'KltChnl.lower[1]{tf}']) & (df[f'BB.lower{tf}'] <= df[f'KltChnl.lower{tf}'])
        squeeze_indicators[tf] = upper_breakout | lower_breakout
        donchian_indicators[tf] = (df[f'DonchCh20.Upper{tf}'] > df[f'DonchCh20.Upper[1]{tf}']) | (df[f'DonchCh20.Lower{tf}'] < df[f'DonchCh20.Lower[1]{tf}'])
    squeeze_df, donchian_df = pd.DataFrame(squeeze_indicators, index=df.index), pd.DataFrame(donchian_indicators, index=df.index)
    combined_df = squeeze_df | donchian_df
    def get_breakout_type(row):
        is_squeeze, is_donchian = any(squeeze_df.loc[row.name]), any(donchian_df.loc[row.name])
        if is_squeeze and is_donchian: return 'Both'
        elif is_squeeze: return 'Squeeze'
        elif is_donchian: return 'Donchian'
        return 'None'
    df['breakout_type'] = df.apply(get_breakout_type, axis=1)
    df['Breakout_TFs'] = combined_df.apply(lambda row: ','.join([tf_display_map.get(tf, tf) for tf, is_breakout in row.items() if is_breakout]), axis=1)
    df['highest_tf'] = df['Breakout_TFs'].apply(lambda tfs: get_highest_timeframe(tfs, tf_order_map, tf_suffix_map))
    return df

def get_highest_timeframe(breakout_tfs_str, order_map, suffix_map):
    """Determine the highest timeframe from a comma-separated string."""
    if not isinstance(breakout_tfs_str, str) or not breakout_tfs_str:
        return 'Unknown'
    tfs = breakout_tfs_str.split(',')
    return max(tfs, key=lambda tf: order_map.get(suffix_map.get(tf, ''), 0))

def calculate_potency_score(df: pd.DataFrame, tf_order_map: dict, tf_suffix_map: dict) -> pd.DataFrame:
    """Calculate the potency score for each stock."""
    if df.empty:
        return df
    df['volume_score'], df['momentum_score'], df['ATR_score'] = 0.0, 0.0, 0.0
    for index, row in df.iterrows():
        for tf_display in row['Breakout_TFs'].split(','):
            tf = tf_suffix_map.get(tf_display)
            if tf and f'volume{tf}' in df.columns and f'average_volume_10d_calc{tf}' in df.columns:
                safe_avg_vol = row[f'average_volume_10d_calc{tf}'] if row[f'average_volume_10d_calc{tf}'] > 0 else 1
                df.loc[index, 'volume_score'] += (row[f'volume{tf}'] / safe_avg_vol) * tf_order_map.get(tf, 0)
            if tf and f'MACD.hist{tf}' in df.columns and f'MACD.hist[1]{tf}' in df.columns:
                value_to_add = (pd.to_numeric(row[f'MACD.hist{tf}'], errors='coerce') - pd.to_numeric(row[f'MACD.hist[1]{tf}'], errors='coerce')) / tf_order_map.get(tf, 1)
                df.loc[index, 'momentum_score'] += value_to_add
            if tf and f'ATRP{tf}' in df.columns:
                df.loc[index, 'ATR_score'] += row[f'ATRP{tf}']
    df['RVOL_Score'] = df['volume_score'] / len(timeframes)
    df['Breakout_Type_Score'] = df['breakout_type'].map(BREAKOUT_TYPE_SCORES).fillna(0)
    df['TF_Order'] = df['highest_tf'].apply(lambda tf_display_name: tf_order_map.get(tf_suffix_map.get(tf_display_name), 0))
    df['Potency_Score'] = ((df['relative_volume_10d_calc'] * POTENCY_WEIGHTS['RVOL']) + (df['TF_Order'] * POTENCY_WEIGHTS['TF_Order']) + (df['Breakout_Type_Score'] * POTENCY_WEIGHTS['Breakout_Type_Score']) + (df['RVOL_Score'] * 0.5) + abs((df['momentum_score'] * 0.5)) + (df['ATR_score']))
    df = df.sort_values(by='Potency_Score', ascending=False)
    df['Potency_Score'] = df['Potency_Score'].round(2)
    return df.drop(columns=['Breakout_Type_Score', 'TF_Order'], errors='ignore')

# --- MongoDB Functions ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
COLLECTION_NAME = "scan_results"

def get_mongo_collection(db_name):
    """Get the MongoDB collection."""
    client = MongoClient(MONGO_URI)
    return client[db_name][COLLECTION_NAME]

def save_scan_results(df: pd.DataFrame, db_name: str):
    """Save scan results to MongoDB, removing null values first."""
    if df.empty: return
    df.dropna(inplace=True)
    if df.empty: return
    collection = get_mongo_collection(db_name)
    for record in df.to_dict(orient='records'):
        query = {'name': record['name'], 'Breakout_TFs': record.get('Breakout_TFs')}
        update_doc = {**record, 'last_updated': pd.Timestamp.now()}
        original_timestamp = update_doc.pop('fired_timestamp', None)
        collection.update_one(query, {'$set': update_doc, '$setOnInsert': {'fired_timestamp': original_timestamp}}, upsert=True)

def load_scan_results(db_name: str) -> pd.DataFrame:
    """Load scan results from MongoDB."""
    collection = get_mongo_collection(db_name)
    records = list(collection.find({}))
    if not records: return pd.DataFrame()
    df = pd.DataFrame(records)
    if '_id' in df.columns: df = df.drop(columns=['_id'])
    if 'Potency_Score' in df.columns: df = df.sort_values(by='Potency_Score', ascending=False)
    return df
