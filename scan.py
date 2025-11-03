
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
    Adds a new string column 'Breakout_TFs' to the DataFrame, listing the timeframes 
    where Donchian Break OR Squeeze Breakout conditions are true.
    """
    
    # 1. Dictionary to store the Boolean Series for the combined condition (Donchian OR Squeeze)
    combined_breakout_indicators = {}
   
      
    if df is not None and not df.empty:
        
        for tf in timeframes:
            # --- Donchian Break Condition ---
            # Donchian Break: Upper band expands OR Lower band expands
            donchian_break = (
                (df[f'DonchCh20.Upper{tf}'] > df[f'DonchCh20.Upper[1]{tf}']) |  # Upper moves up
                (df[f'DonchCh20.Lower{tf}'] < df[f'DonchCh20.Lower[1]{tf}'])   # OR Lower moves down
            )
            
            # --- Squeeze Breakout Condition ---
            # Upper Breakout
            upper_breakout = (
                (df[f'BB.upper[1]{tf}'] < df[f'KltChnl.upper[1]{tf}']) &
                (df[f'BB.upper{tf}'] >= df[f'KltChnl.upper{tf}'])
            )
            # Lower Breakout
            lower_breakout = (
                (df[f'BB.lower[1]{tf}'] > df[f'KltChnl.lower[1]{tf}']) &
                (df[f'BB.lower{tf}'] <= df[f'KltChnl.lower{tf}'])
            )
            squeeze_breakout = upper_breakout | lower_breakout
            
            # 🚨 NEW: Combined Condition (Donchian Break OR Squeeze Breakout) 🚨
            # If EITHER condition is true, the timeframe is noted.
            final_combined_condition = donchian_break | squeeze_breakout
            
            # Store the resulting Boolean Series in the dictionary
            combined_breakout_indicators[tf] = final_combined_condition

        # 2. Convert the dictionary of Boolean Series into a single DataFrame
        # Each column is a timeframe, and the values are True/False for the combined condition.
        indicators_df = pd.DataFrame(combined_breakout_indicators, index=df.index)

        # 3. Create the final string column using row-wise application
        # Creates a comma-separated string of all timeframes where the condition is True.
        df['Breakout_TFs'] = indicators_df.apply(
            lambda row: ','.join([tf for tf in timeframes if row[tf]]), 
            axis=1  # Apply function across columns for each row
        )
        
        return df
    else:
        print("No data to process.")
        print("Returning empty DataFrame.   ------------------------------------------------------>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        return pd.DataFrame()


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

    collection = get_mongo_collection()
    records = df.to_dict(orient='records')
    
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
    
 
