import re
import math
import time
import datetime
import numpy as np
import os

from plugins.rci_plugins.promql.query_designer import *
from plugins.rci_plugins.promql.query_executor import *
from plugins.rci_plugins.promql.grafana_df_analyzer import *
from plugins.rci_plugins.promql.query_preprocess import preprocess_df
from src.utils.timeutils import to_unix_ts, get_range_printable
from src.data.filters import *
from src.parameter_utils import ConfigurationException

def verify_query_config(query_config):
    """
    Verify that the query configuration has the proper format.

    Query config expects:
        step: int (seconds)
        yieldstypes: list of strings (from settings["type_options"])
    """

    intro = f"Problem with query config \"{query_config["cfg_name"]}\":"

    if("step" not in query_config):
        raise ConfigurationException(f"{intro} The query config doesn't have a step section. Indicate the step size (in seconds).")
    if("yieldstypes" not in query_config):
        raise ConfigurationException(f"{intro} The query config doesn't have a yieldstypes section. yieldstypes section expects a list of possible types from all possible types: {settings["type_options"]}")
    if(not isinstance(query_config["yieldstypes"], list)):
        raise ConfigurationException(f"{intro} The yieldstypes section is not in the form of a list. PromQLIngestController yieldstypes section expects a list of possible types from all possible types: {settings["type_options"]}")
    if(len(query_config["yieldstypes"]) == 0):
        raise ConfigurationException(f"{intro} The yieldstypes section is empty. At least one yielded type must be provided.")
    settings_set = set(settings["type_options"])
    config_set = set(query_config["yieldstypes"])
    if(not config_set.issubset(settings_set)):
        raise ConfigurationException(f"{intro} The yieldstypes section had unexpected types: {", ".join(config_set-settings_set)}. Options must be from: {", ".join(settings_set)}")

class DataFramePullException(Exception):
    """ Exception raised when there is an issue pulling a DataFrame from PromQL. """
    pass

def run(query_config, type, period, cached_status_dfs):
    """
    Run this query config over the provided period list. Gets the status, cpu values, and gpu
        values DataFrames and then applies processing steps on them.
    """

    start_ts, end_ts = period

    requires_status = "status" in query_config["queries"]
    # Get status DataFrame if it is specified in the query config.
    if(requires_status):
        cache_id = (query_config["cfg_name"], start_ts, end_ts)
        # tqdm.write(str(cache_id))
        if(cache_id not in cached_status_dfs):
            status_url = build_query_url(query_config, "status", None, period) # Gets the URL for performing the query
            status_response = get_query_response(status_url) # Gets JSON response from web
            status_df_raw = transform_query_response(status_response) # Transform the JSON to a DataFrame
            cached_status_dfs[cache_id] = preprocess_df(status_df_raw, False, query_config["step"]) # Preprocess DF for application
            
        status_df = cached_status_dfs[cache_id]

        if(len(status_df) == 0):
            raise DataFramePullException(f"Status DataFrame pulled for query config \"{query_config['cfg_name']}\" over period {get_range_printable(start_ts, end_ts)} is empty, cannot proceed with applying status filter to values DataFrame.")
    else:
        status_df = None

    # The same pipeline as above
    values_url = build_query_url(query_config, "values", type, period)
    values_response = get_query_response(values_url)
    values_df = transform_query_response(values_response)

    if(len(values_df) == 0):
        raise DataFramePullException(f"Values DataFrame pulled for query config \"{query_config['cfg_name']}\" type \"{type}\" over period {get_range_printable(start_ts, end_ts)} is empty.")

    # Apply status DataFrame if it exists
    if(requires_status):
        values_df = preprocess_df(values_df, True, query_config["step"])
        values_df = _apply_status_df(status_df, values_df)

    return values_df

def _apply_status_df(status_df, values_df):
    """
    Apply the status DataFrame to the values DataFrame, only accepting values from the values_df
        when the status_df has a 1 in that cell position.
    Cells are identified by column=uid and row=timestamp, so the status_df and the values_df must
        have matching time columns.

    Args:
        status_df (pd.DataFrame): The DataFrame holding running/pending statuses.
        values_df (pd.DataFrame): The DataFrame holding the usage values.

    Returns:
        pd.DataFrame: The values DataFrame with the running/pending statuses applied.
    """

    def select_uid(string):
        if(string == "Time"):
            return string

        match = re.search(r'uid="([^"]+)"', string)
        if match:
            uid = match.group(1)
            return uid
        else:
            raise Exception(f"Failed to read uid in column name \"{string}\"")

    # Strip start and end timestamps from the values DataFrame
    start_ts = to_unix_ts(values_df["Time"][0])
    end_ts = to_unix_ts(list(values_df["Time"])[-1])

    # Get the list of timestamps from the status DataFrame
    times_list = [to_unix_ts(time) for time in status_df["Time"]]

    # Determine the indexes (row numbers) of the start and end timestamps in the status DataFrame
    try:
        start_index = times_list.index(start_ts)
    except ValueError:
        start_index = 0

    try:
        end_index = times_list.index(end_ts)
    except ValueError:
        end_index = len(times_list)-1

    drop_columns = []

    for column in values_df.columns:
        if(column == "Time"):
            continue

        uid = select_uid(column)

        if(uid not in status_df.columns):
            drop_columns.append(column)
            continue

        status_column = status_df[uid].iloc[range(start_index, end_index+1)]
        status_column.index = values_df.index

        values_df[column] = values_df[column].where(status_column == 1)

    values_df.drop(columns=drop_columns, inplace=True)

    return values_df