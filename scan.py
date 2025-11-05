
from tradingview_screener import Query, col, And, Or
import pandas as pd

VOLUME_THRESHOLDS = {
    '|1': 25000,
    '|5': 100000,
    '|15': 300000,
    '|30': 500000,
    '|60': 1000000,
    '|120': 2000000,
    '|240': 4000000,
    '': 5000000,
    '|1W': 25000000,
    '|1M': 100000000
}

timeframes = ['|1',  '|5', '|15','|30' ,'|60','|120','|240','', '|1W','|1M']
tf_order_map = { '|1': 1 ,  '|5': 2, '|15': 3,  '|30': 4, '|60': 5, '|120': 6,   '|240':7, '': 8, '|1W': 9 , '|1M': 10 }
tf_display_map = { '|1': '1m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H','': 'Daily', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}

# Potency Scoring Constants for Potency_Score calculation
POTENCY_WEIGHTS = {
    'RVOL': 2.0,
    'TF_Order': 1.5,
    'Breakout_Type_Score': 1.0,
}

BREAKOUT_TYPE_SCORES = {
    'Both': 3,
    'Squeeze': 2,
    'Donchian': 1,
    'None': 0
}


# Define the columns that are essential for the UI.
UI_COLUMNS = [
    'name','ticker', 'logoid', 'relative_volume_10d_calc', 'Breakout_TFs',
    'fired_timestamp', 'momentum', 'breakout_type', 'highest_tf', 'Potency_Score'
]


# Construct select columns for all timeframes
select_cols = ['name', 'logoid', 'close','relative_volume_10d_calc', 'beta_1_year']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',f'DonchCh20.Upper{tf}', f'DonchCh20.Lower{tf}',
        f'KltChnl.lower[1]{tf}', f'KltChnl.upper[1]{tf}', f'BB.lower[1]{tf}', f'BB.upper[1]{tf}',f'DonchCh20.Upper[1]{tf}', f'DonchCh20.Lower[1]{tf}',
        f'ATRP{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'close{tf}', f'Value.Traded{tf}',f'MACD.hist{tf}',f'MACD.hist[1]{tf}'
    ])



def run_intraday_scan(settings, cookies, db_name):


    if cookies is None:
        return {"fired": pd.DataFrame()}



    base_filters = [
        col('beta_1_year') > settings['beta_1_year'],
        col('is_primary') == True,
        col('typespecs').has(['', 'common', 'foreign-issuer']),
        col('type').isin(['dr', 'stock']),
        # col('exchange').isin(settings['exchange']),
        col('close').between(settings['min_price'], settings['max_price']),
        col('active_symbol') == True,
        col('Value.Traded|5') > settings['min_value_traded'],        
        # col('relative_volume_intraday|5') >settings['RVOL_threshold']
    ]

    trendFilter   = [Or(
        And(
        # col(f'EMA5{tf}')  > col(f'EMA20{tf}'),
        col(f'EMA20{tf}') > col(f'EMA200{tf}'), 
        col(f'close{tf}') > col(f'EMA20{tf}'),
        # col(f'EMA5{tf}') > col(f'SMA5{tf}'),

        # col(f'EMA5{tf}') > col(f'EMA200{tf}'),        
        ),
        And(
        # col(f'EMA5{tf}')  < col(f'EMA20{tf}'),
        col(f'EMA20{tf}') < col(f'EMA200{tf}'),        
        col(f'close{tf}') < col(f'EMA20{tf}'),
        # col(f'EMA5{tf}') < col(f'SMA5{tf}'),
        # col(f'EMA5{tf}') < col(f'EMA200{tf}'),        
        ),

        )for tf in timeframes]
        
        
    # for tf in timeframes:
    donchian_break = [Or(
        col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'),
        col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'),

    ) for tf in timeframes]

    squeeze_breakout = [Or(
        And(
            col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'),
            col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'),
           
        ),
        And(
            col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
            col(f'BB.lower{tf}') <= col(f'KltChnl.lower[1]{tf}'),
            
        ),


        )for tf in timeframes]



    vol_spike = [Or(
        # col(f'volume{tf}')> VOLUME_THRESHOLDS.get(tf,50000),

         
            col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), settings['RVOL_threshold']-1),
            col(f'relative_volume_10d_calc{tf}') > settings['RVOL_threshold'],
            col('relative_volume_intraday|5') >settings['RVOL_threshold']
            

    )for tf in timeframes]

 
 




    filters = [And(*base_filters )]
    filters =filters + [And(*vol_spike )]
    # filters =filters + [And(*trendFilter)]
    # filters =filters + [And(*donchian_break )]
    # filters =filters + [And(*squeeze_breakout )]
    filters =filters + [Or(*squeeze_breakout,*donchian_break, *trendFilter)]
    # +
    query = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market']).limit(1000).set_property('symbols', {'query': {}})
    # print(query)
    print(settings['market'])
    # query.set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr']}})

    try:
        # print(f"Running intraday scan for timeframe: {tf or '1D'}")
        _, df = query.get_scanner_data(cookies=cookies)
        if df is not None:
            df = df.fillna(value=pd.NA).replace({pd.NA: None})
            # ROBUST CONVERSION: Convert pandas NaN, numpy NaN, and Inf to Python None
            df = df.replace([float('inf'), float('-inf')], None).where(pd.notnull(df), None)
        print(f"Scan completed  size: {len(df)}")


        if df is not None and not df.empty:
            df = identify_combined_breakout_timeframes(df, timeframes)
            df = calculate_potency_score(df, tf_order_map)
            # all_results = pd.concat( [all_results ,df])

        else :
            #all_results =  pd.DataFrame()
            print(" NO NEW DATA  --------------- returning empty df")
            # print(all_results)

            #df['timeframe'] = tf
            # all_results.append(df)
            return {"fired": pd.DataFrame()}
    except Exception as e:
        print(f"Error in intraday scan : {e}")
        #stack strace exceprion prine exception
        import traceback
        traceback.print_exc()



    # if  all_results is  None or  all_results.empty :
    #     all_results = pd.DataFrame()
    #     return {"fired": pd.DataFrame()}

    df_New =df.drop_duplicates(subset=['name']).copy()
    # df_all = df_all.rename(columns={'timeframe': 'highest_tf'})
    # PRINT SIZE OF df_all
    # NEW FILTER: Only keep records that are actual breakouts ('Both', 'Squeeze', 'Donchian')
    df_New = df_New[df_New['breakout_type'] != 'None'].copy()
    if df_New.empty:
        print(f"===========NO NEW BREAKOUT =================================")

        return {"fired": pd.DataFrame()}
    # else :
    #     print(f"==================> NEW BREAKOUT HAPPENING ----------------------------->    df_New size: {len(df_New)}")


    df_New['fired_timestamp'] = pd.Timestamp.now()
    df_New['count'] = 1
    df_New['previous_volatility'] = 0
    df_New['current_volatility'] = 0
    df_New['momentum'] = df_New['momentum_score'].apply(lambda x: 'Bullish' if x > 0 else ('Bearish' if x < 0 else 'Neutral'))

    # print(df_all.columns)
    # Second ROBUST CONVERSION: Ensure final Potency_Score (which might be NaN) is converted before UI columns are selected
    df_New = df_New.replace([float('inf'), float('-inf')], None).where(pd.notnull(df_New), None)

    # Filter the DataFrame to include only the columns needed by the UI
    df_filtered = df_New[[col for col in UI_COLUMNS if col in df_New.columns]].copy()
    save_scan_results(df_filtered, db_name)
    return {"fired": df_filtered}

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

def identify_combined_breakout_timeframes(df: pd.DataFrame, timeframes: list) -> pd.DataFrame:
    """
    Adds 'Breakout_TFs' and 'breakout_type' columns to the DataFrame.
    - 'Breakout_TFs': Lists timeframes where a Donchian or Squeeze Breakout occurs.
    - 'breakout_type': Identifies the type of breakout (Squeeze, Donchian, or Both).
    """
    if df is None or df.empty:
        print("No data to process.")
        return pd.DataFrame()

    squeeze_indicators = {}
    donchian_indicators = {}

    for tf in timeframes:
        # --- Squeeze Breakout Condition ---
        upper_breakout = (
            (df[f'BB.upper[1]{tf}'] < df[f'KltChnl.upper[1]{tf}']) &
            (df[f'BB.upper{tf}'] >= df[f'KltChnl.upper{tf}'])
        )
        lower_breakout = (
            (df[f'BB.lower[1]{tf}'] > df[f'KltChnl.lower[1]{tf}']) &
            (df[f'BB.lower{tf}'] <= df[f'KltChnl.lower{tf}'])
        )
        squeeze_indicators[tf] = upper_breakout | lower_breakout

        # --- Donchian Break Condition ---
        donchian_indicators[tf] = (
            (df[f'DonchCh20.Upper{tf}'] > df[f'DonchCh20.Upper[1]{tf}']) |
            (df[f'DonchCh20.Lower{tf}'] < df[f'DonchCh20.Lower[1]{tf}'])
        )

    squeeze_df = pd.DataFrame(squeeze_indicators, index=df.index)
    donchian_df = pd.DataFrame(donchian_indicators, index=df.index)

    # Combined condition for any breakout
    combined_df = squeeze_df | donchian_df

    # --- Determine Breakout Type ---
    def get_breakout_type(row):
        is_squeeze = any(squeeze_df.loc[row.name])
        is_donchian = any(donchian_df.loc[row.name])
        if is_squeeze and is_donchian:
            return 'Both'
        elif is_squeeze:
            return 'Squeeze'
        elif is_donchian:
            return 'Donchian'
        return 'None'

    df['breakout_type'] = df.apply(get_breakout_type, axis=1)

    # --- Create Breakout_TFs string ---
    df['Breakout_TFs'] = combined_df.apply(
        lambda row: ','.join([tf_display_map.get(tf, tf) for tf, is_breakout in row.items() if is_breakout]),
        axis=1
    )

    df['highest_tf'] = df['Breakout_TFs'].apply(
        lambda tfs: get_highest_timeframe(tfs, tf_order_map, tf_suffix_map)
    )

    return df


def get_highest_timeframe(breakout_tfs_str, order_map, suffix_map):
    """
    Determines the highest timeframe from a comma-separated string of breakout timeframes.
    """
    if not isinstance(breakout_tfs_str, str) or not breakout_tfs_str:
        return 'Unknown'

    tfs = breakout_tfs_str.split(',')

    # Get the suffix with the highest order value, default to a low value if not found
    highest_tf_suffix = max(tfs, key=lambda tf: order_map.get(suffix_map.get(tf, ''), 0))

    return highest_tf_suffix


def calculate_potency_score(df: pd.DataFrame, tf_order_map: dict) -> pd.DataFrame:
    """
    Calculates a Potency Score for each stock based on RVOL,
    Highest Timeframe, and Breakout Type.
    """
    if df.empty:
        return df


    ##for every row sum all columns ratio df[f'volume{tf}']/ df[f'average_volume_10d_calc{tf}'] in volumeScore
    df['volume_score'] = 0.0
    for tf in timeframes:
        # Ensure columns exist before performing operations
        if f'volume{tf}' in df.columns and f'average_volume_10d_calc{tf}' in df.columns:
            # Replace 0 in average_volume_10d_calc to avoid division by zero
            safe_avg_vol = df[f'average_volume_10d_calc{tf}'].replace(0, 1)
            df['volume_score'] += (df[f'volume{tf}'] / safe_avg_vol)*tf_order_map.get(tf, 0)
        else:
            print(f"Warning: Missing volume data for timeframe {tf}. Skipping volume score calculation for this TF.")

    df['RVOL_Score'] = df['volume_score'] / len(timeframes) # Average RVOL across all TFs


    # Check which parts are NaN
    # print("Is momentum_score NaN?", df['momentum_score'].isna().any())
    # print(f"Is MACD.hist{tf} NaN?", df[f'MACD.hist{tf}'].isna().any())
    df['momentum_score'] =0.0

    for tf in timeframes:
        # Ensure columns exist before performing operations
        if f'MACD.hist{tf}'   in df.columns and f'MACD.hist[1]{tf}'   in df.columns:
            # Replace 0 in average_volume_10d_calc to avoid division by zero
            
            # df['momentum_score'] += df[f'MACD.hist{tf}']  *tf_order_map.get(tf, 0)
            value_to_add =(df[f'MACD.hist{tf}'].fillna(0)  -df[f'MACD.hist[1]{tf}'].fillna(0) ) /tf_order_map.get(tf, 1)
            df['momentum_score'] = df['momentum_score'].add(value_to_add, fill_value=0)
        else:
            print(f"Warning: Missing volume data for timeframe {tf}. Skipping volume score calculation for this TF.")
    print(df[['momentum_score']])

    #     # Get the highest value
    # highest_score = df['momentum_score'].max()

    # # Get the lowest value
    # lowest_score = df['momentum_score'].min()

    # # Print the results
    # print(f"The highest momentum score is: {highest_score}")
    # print(f"The lowest momentum score is: {lowest_score}")


    # # 2. Calculate the normalized momentum score
    # # It's safest to create a new temporary column for the normalized values first
    # df['momentum_score_normalized'] = (df['momentum_score'] - lowest_score) / (highest_score - lowest_score)

    # # Optional: Verify the new normalized range (should be ~0.0 to ~1.0)
    # print(f"Normalized Min: {df['momentum_score_normalized'].min()}")
    # print(f"Normalized Max: {df['momentum_score_normalized'].max()}")

    # print(df['momentum_score_normalized'])


    # # Create an instance of the scaler
    # scaler = MinMaxScaler()

    # # The scaler expects a 2D array, so we reshape the single column
    # # We have to drop NaNs first if you haven't handled them already
    # momentum_data = df['momentum_score'].dropna().values.reshape(-1, 1)

    # # Fit and transform the data (this modifies the original values in place or creates new ones)
    # # This example creates a new column with the scaled data:
    # df['momentum_score_normalized_skl'] = np.nan # Initialize the column
    # df.loc[df['momentum_score'].notna(), 'momentum_score_normalized_skl'] = scaler.fit_transform(momentum_data)


    # print(f"Normalized Min: {df['momentum_score_normalized_skl'].min()}")
    # print(f"Normalized Max: {df['momentum_score_normalized_skl'].max()}")
 
    # print(df[['momentum_score_normalized_skl']])



    # print(" VOLUME SCORE ")
    # print(df[['volume_score']])
    # Normalize volume_score if needed, or use it directly
    # For now, let's use it directly as a sum


    df['ATR_score'] = 0.0
    for tf in timeframes:
        # Ensure columns exist before performing operations
        if f'ATRP{tf}' in df.columns  :
            
            df['ATR_score'] += (df[f'ATRP{tf}'] )
        else:
            print(f"Warning: Missing volume data for timeframe {tf}. Skipping volume score calculation for this TF.")

    # df['RVOL_Score'] = df['volume_score'] / len(timeframes) # Average RVOL across all TFs



    # 1. Breakout Type Score
    # Map the breakout type to a score, defaulting to 0
    df['Breakout_Type_Score'] = df['breakout_type'].map(BREAKOUT_TYPE_SCORES).fillna(0)

    # 2. Highest Timeframe Order Score
    # Map the display name (e.g., '15m') back to the suffix (e.g., '|15')
    # and then get the order from tf_order_map. tf_suffix_map is available globally.
    def get_tf_order(tf_display_name):
        tf_suffix = tf_suffix_map.get(tf_display_name)
        # tf_order_map is passed as an argument
        return tf_order_map.get(tf_suffix, 0)

    df['TF_Order'] = df['highest_tf'].apply(get_tf_order)

    # 3. Potency Score Calculation
    # Potency_Score = (2.0 * RVOL) + (1.5 * TF_Order) + (1.0 * Breakout_Type_Score)
    df['Potency_Score'] = (
        (df['relative_volume_10d_calc'] * POTENCY_WEIGHTS['RVOL']) +
        (df['TF_Order'] * POTENCY_WEIGHTS['TF_Order']) +
        (df['Breakout_Type_Score'] * POTENCY_WEIGHTS['Breakout_Type_Score']) +
        (df['RVOL_Score'] * 0.5)+
        abs((df['momentum_score'] * 0.5))+
        (df['ATR_score']  )
    )

    # Sort by Potency Score, descending
    df = df.sort_values(by='Potency_Score', ascending=False)

    # Keep only 2 decimal places for display purposes
    df['Potency_Score'] = df['Potency_Score'].round(2)

    # Drop intermediate columns
    df = df.drop(columns=['Breakout_Type_Score', 'TF_Order'], errors='ignore')

    return df

#Add API to SAVE complete scan results into MONGO DB (ScannerDB) and RETRIEVE when APP re-starts
from pymongo import MongoClient
import os

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = "ScannerDB"
COLLECTION_NAME = "scan_results"

def get_mongo_collection(db_name):
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    return db[COLLECTION_NAME]

def save_scan_results(df: pd.DataFrame, db_name: str):
    """Saves the DataFrame to MongoDB using an upsert mechanism to avoid duplicates."""
    if df.empty:
        print("No data to save to MongoDB.")
        return

    # Select only the columns needed for the UI to reduce database size
    # Ensure only existing columns are selected
    df_ui = df[[col for col in UI_COLUMNS if col in df.columns]].copy()

    collection = get_mongo_collection(db_name)
    records = df_ui.to_dict(orient='records')

    upsert_count = 0
    modified_count = 0

    for record in records:
        # Use 'name' and 'Breakout_TFs' to uniquely identify a breakout event
        query = {
            'name': record['name'],
            'Breakout_TFs': record.get('Breakout_TFs')
        }

        # Prepare the update document
        update_doc = record.copy()
        update_doc['last_updated'] = pd.Timestamp.now()

        # Remove the original fired_timestamp from the main update payload
        original_timestamp = update_doc.pop('fired_timestamp', None)

        update = {
            '$set': update_doc,
            '$setOnInsert': {
                'fired_timestamp': original_timestamp
            }
        }

        result = collection.update_one(query, update, upsert=True)
        if result.upserted_id:
            upsert_count += 1
        elif result.modified_count > 0:
            modified_count += 1

    print(f"Upserted {upsert_count} and modified {modified_count} records in MongoDB.")

def load_scan_results(db_name: str) -> pd.DataFrame:
    """Loads scan results from MongoDB."""
    collection = get_mongo_collection(db_name)

    # Retrieve all documents from the collection
    records = list(collection.find({}))

    if not records:
        print("No previous scan results found in MongoDB.")
        return pd.DataFrame()

    # Convert list of dictionaries back to DataFrame
    df = pd.DataFrame(records)

    # Drop the MongoDB '_id' column if it exists
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    # print(f"===============================================================================Loaded {len(df)} records from MongoDB.")

    # Sort by the new Potency_Score before returning
    if 'Potency_Score' in df.columns:
         df = df.sort_values(by='Potency_Score', ascending=False)

    return df