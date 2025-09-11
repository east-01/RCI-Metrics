import pandas as pd
import re
import numpy as np
import math

from src.utils.timeutils import to_unix_ts, from_unix_ts

def _preprocess_df(df: pd.DataFrame, preserve_columns, step):
    df = _filter_cols_zero(df)
    df = _merge_columns_on_uid(df, preserve_columns)
    df = _infer_times(df, step)
    return df

def _filter_cols_zero(df: pd.DataFrame):
    """
    Removes columns with a sum of 0, makes processing more efficient later.

    Args:
        df (pd.DataFrame): The DataFrame to remove columns.
    
    Returns:
        pd.DataFrame: The adjusted DataFrame with removed empty columns.
    """
    
    df.drop(columns=df.columns[df.sum() == 0], inplace=True)
    return df    

def _merge_columns_on_uid(df: pd.DataFrame, preserve_columns: bool = False):
    """
    Takes the max value from each uid in the DataFrame and places it in a single row, this
        squashes the DataFrame horizontally for more efficient processing later.
    
    Args:
        df (pd.DataFrame): The DataFrame to merge horizontally.
        preserve_columns (bool): Keep the column names as they were or replace with only UIDs.
    
    Returns:
        pd.DataFrame: The adjusted DataFrame with squashed columns.
    """
    # Holds string uid key with original column name values
    orig_names = {}

    def select_uid(string):
        if(string == "Time"):
            return string

        match = re.search(r'uid="([^"]+)"', string)
        if match:
            uid = match.group(1)

            # Store first instance of the uid's original name, pandas will append .X to duplicate 
            #   column names
            if(preserve_columns and uid not in orig_names):
                orig_names[uid] = string

            return uid
        else:
            raise Exception(f"Failed to read uid in column name \"{string}\"")

    time_col = df['Time'] # Separate Time column
    df = df.drop(columns='Time')

    uids = [select_uid(col) for col in df.columns]
    df.columns = uids

    seen_uids = set() # The set of uids that have been visited in the loop
    duplicate_uids = set() # The set of uids that have more than one column
    for uid in uids:
        if(uid in seen_uids):
            duplicate_uids.add(uid)
        
        seen_uids.add(uid)
        
    static_df = df[list(seen_uids-duplicate_uids)]

    to_merge_df = df[list(duplicate_uids)]
    to_merge_df = to_merge_df.T.groupby(to_merge_df.columns).max().T # Merge columns with the same UID

    df = pd.concat([static_df, to_merge_df], axis=1)

    order = np.argsort(df.columns)
    df = df.iloc[:, order]

    if(preserve_columns):
        df.columns = [orig_names[uid] for uid in df.columns]

    df.insert(0, 'Time', time_col) # Reattach Time column

    return df

def _infer_times(df: pd.DataFrame, step):
    """
    Infer timestamps in between rows of a Grafana DataFrame based off of a step value.
    If the difference in timestamps between two rows is greater than the step value, new timestamps
        will be generated with the correct step values to fill the gap.

    Args:
        df (pd.DataFrame): The DataFrame that times will need to be inferred for.
        step (int): The time in seconds between expected steps.

    Returns:
        pd.DataFrame: The adjusted DataFrame with inferred time rows.    
    """

    columns_excluding_time = list(df.columns)
    columns_excluding_time.remove("Time")

    i = 0
    while i < len(df["Time"]) - 1:
        
        time = to_unix_ts(df["Time"][i])
        next_time = to_unix_ts(df["Time"][i+1])
        time_offset = next_time-time

        if(time_offset <= step):
            i += 1
            continue

        rows_to_add = math.floor(time_offset/step)

        times_arr = [from_unix_ts(time + j*step) for j in range(1, rows_to_add)]
        times_dict = { "Time": times_arr } 

        rows_arr = [float('NaN')]*len(times_arr)
        rows_dict = { key: rows_arr for key in columns_excluding_time }

        final_df = times_dict | rows_dict

        rows_df = pd.DataFrame(final_df)        
                
        df = pd.concat([df.iloc[:i+1], rows_df, df.iloc[i+1:]]).reset_index(drop=True)

        i += rows_to_add

    return df