
from tradingview_screener import Query, col, And, Or
import pandas as pd

VOLUME_THRESHOLDS = {
    '|3': 15000,
    '|5': 25000,
    '|15': 50000,
    '|30': 100000,
    '|60': 200000,
    '|120': 400000,
    '|240': 800000,
    '': 5000000,
    '|1W': 25000000,
    '|1M': 100000000
}

timeframes = ['|5', '|15','|30' ,'|60','|120','|240','']#, '|1W','|1M']
tf_order_map = {'|1M': 10, '|1W': 9 , '': 8, '|240':7 , '|120': 6, '|60': 5, '|30': 4, '|15': 3, '|5': 2, '|3': 1}
tf_display_map = {'|3': '3m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H','': 'Daily', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}


# Construct select columns for all timeframes
select_cols = ['name', 'logoid', 'close', 'MACD.hist','relative_volume_10d_calc', 'beta_1_year']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',f'DonchCh20.Upper{tf}', f'DonchCh20.Lower{tf}',
        f'KltChnl.lower[1]{tf}', f'KltChnl.upper[1]{tf}', f'BB.lower[1]{tf}', f'BB.upper[1]{tf}',f'DonchCh20.Upper[1]{tf}', f'DonchCh20.Lower[1]{tf}',
        
        f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
    ])

def run_intraday_scan(settings, cookies):
   
    
    if cookies is None:
        return {"fired": pd.DataFrame()}

    
    
    base_filters = [
        col('beta_1_year') > 1.2,
        col('is_primary') == True,
        col('typespecs').has('common'),
        col('type') == 'stock',
        col('exchange') == 'NSE',
        col('active_symbol') == True,
    ]

    # for tf in timeframes:
    donchian_break = [Or(
        col(f'DonchCh20.Upper{tf}') > col(f'DonchCh20.Upper[1]{tf}'),
        col(f'DonchCh20.Lower{tf}') < col(f'DonchCh20.Lower[1]{tf}'), 
        col(f'close{tf}') > col(f'DonchCh20.Upper[1]{tf}'),
        col(f'close{tf}') < col(f'DonchCh20.Lower[1]{tf}')
    ) for tf in timeframes]

    squeeze_breakout = [Or(
        And(
            col(f'BB.upper[1]{tf}') < col(f'KltChnl.upper[1]{tf}'),
           Or( col(f'BB.upper{tf}') >= col(f'KltChnl.upper{tf}'),
              col(f'close{tf}') > col(f'BB.upper{tf}'),
              col(f'close{tf}') > col(f'KltChnl.upper{tf}')
           )
        ),
        And(
            col(f'BB.lower[1]{tf}') > col(f'KltChnl.lower[1]{tf}'),
            Or(col(f'BB.lower{tf}') <= col(f'KltChnl.lower[1]{tf}'),
            
            col(f'close{tf}') < col(f'BB.lower{tf}'),
            col(f'close{tf}') < col(f'KltChnl.lower{tf}')
            ),
        ),
         
        
    )for tf in timeframes]

    vol_spike = [And(
        col(f'volume{tf}') > VOLUME_THRESHOLDS.get(tf,50000),
        Or (col(f'volume{tf}').above_pct(col(f'average_volume_10d_calc{tf}'), 1.5),
            col(f'relative_volume_10d_calc{tf}') > 1.5,
            col('relative_volume_intraday|5') >1.5
            ),
        
    )for tf in timeframes]

   
    filters = base_filters + [*vol_spike, Or(*donchian_break, *squeeze_breakout)]
    query = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])
    
    try:
        # print(f"Running intraday scan for timeframe: {tf or '1D'}")
        _, df = query.get_scanner_data(cookies=cookies)
        if df is not None:
            df = df.fillna(value=pd.NA).replace({pd.NA: None})
        print(f"Scan completed ") 
        ##print size of df
        print(f"QUERY RESULT ----> size: {len(df)}")
        print(df.head())
        
        # print(df.columns)
   

        # print(df)


        # all_results =load_scan_results()
        # if  all_results is  None or  all_results.empty :
        #     all_results = pd.DataFrame()

        if df is not None and not df.empty:
            df = identify_combined_breakout_timeframes(df, timeframes)
            # all_results = pd.concat( [all_results ,df])
            
        else :
            #all_results =  pd.DataFrame() 
            print(" NO NEW DATA  --------------- ")
            # print(all_results)
            
            #df['timeframe'] = tf
            # all_results.append(df)
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
    if df_New.empty:
        print(f"===========NO NEW BREAKOUT =================================")
    
        return {"fired": pd.DataFrame()}
    # else :
    #     print(f"==================> NEW BREAKOUT HAPPENING ----------------------------->    df_New size: {len(df_New)}")
    
        
    df_New['fired_timestamp'] = pd.Timestamp.now()
    df_New['count'] = 1
    df_New['previous_volatility'] = 0
    df_New['current_volatility'] = 0
    df_New['momentum'] = df_New['MACD.hist'].apply(lambda x: 'Bullish' if x > 0 else ('Bearish' if x < 0 else 'Neutral'))

    # print(df_all.columns)
    save_scan_results(df_New)
    return {"fired": df_New}

import pandas as pd

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


#Add API to SAVE complete scan results into MONGO DB (ScannerDB) and RETRIEVE when APP re-starts
from pymongo import MongoClient
import os

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = "ScannerDB"
COLLECTION_NAME = "scan_results"

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

def save_scan_results(df: pd.DataFrame):
    """Saves the DataFrame to MongoDB using an upsert mechanism to avoid duplicates."""
    if df.empty:
        print("No data to save to MongoDB.")
        return

    # Select only the columns needed for the UI to reduce database size
    ui_columns = [
        'name', 'logoid', 'relative_volume_10d_calc', 'Breakout_TFs',
        'fired_timestamp', 'momentum', 'breakout_type', 'highest_tf'
    ]
    # Ensure only existing columns are selected
    df_ui = df[[col for col in ui_columns if col in df.columns]].copy()

    collection = get_mongo_collection()
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

def load_scan_results() -> pd.DataFrame:
    """Loads scan results from MongoDB."""
    collection = get_mongo_collection()
    
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
    return df
    
 
