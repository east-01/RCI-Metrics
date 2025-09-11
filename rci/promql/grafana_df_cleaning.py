from plugins.rci.promql.grafana_df_analyzer import _extract_column_data

def has_time_column(df):
    return df.columns[0]=="Time"

def clear_time_column(df):
    if(not has_time_column(df)):
        raise ValueError("Can't clear time column from DataFrame, it doesn't have one!")
    return df[df.columns[1:]]

def clear_duplicate_uids(df):
    """
    Given a DataFrame with UIDs in the headers, return a new DataFrame with repeated UIDs removed.

    Args:
    df (Pandas DataFrame): The DataFrame to deduplicate
    """
    uids = set()

    if(has_time_column(df)):
        df = clear_time_column(df)

    def is_not_duplicate(col_name):
        nonlocal uids
        col_data = _extract_column_data(col_name)
        uid = col_data['uid']
        if(uid in uids):
            return False
        else:
            uids.add(uid)
            return True

    df_included_columns = [col_name for col_name in df.columns if is_not_duplicate(col_name)]
    return df[df_included_columns]

def clear_blacklisted_uids(df, blacklist):
    """
    Given a DataFrame with UIDs in the headers, return a new DataFrame with blacklisted UIDs
      removed.

    Args:
    df (Pandas DataFrame): The DataFrame to deduplicate
    blacklist (list): The list of uids to not include in the output DataFrame.
    """
    if(has_time_column(df)):
        df = clear_time_column(df)

    df_included_columns = [col_name for col_name in df.columns if _extract_column_data(col_name)['uid'] not in blacklist]
    return df[df_included_columns]