import re
import math
import time
import datetime
import numpy as np
import os

from plugins.rci.promql.query_designer import *
from plugins.rci.promql.query_executor import *
from plugins.rci.promql.grafana_df_analyzer import *
from plugins.rci.promql.query_preprocess import _preprocess_df
from plugins.rci.rci_identifiers import GrafanaIdentifier
from plugins.rci.analyses.jobs_analyses import filter_source_type
from src.data.data_repository import DataRepository
from src.utils.timeutils import to_unix_ts, get_range_printable
from src.data.filters import *

def run(query_config, period_list):
    # The data repository that holds GrafanaIdentifiers, this is different from the
    #   standard DataRepository because there are multiple queries per period and type, to be
    #   used in the processing step where we only get pending/running pods.
    data_repo: DataRepository = DataRepository()

    cfg_name = query_config["cfg_name"]
    step = query_config["step"]
    period_cnt = len(period_list)
    query_count = period_cnt+len(settings["type_options"])*period_cnt
    print(f"PromQL: Loading data using ingest config \"{cfg_name}\" resulting in {query_count} query/queries.")

    for period in period_list:

        start_time = time.time()

        print(f"  {get_range_printable(period[0], period[1], step)}")
        print(f"\r    Getting status...", end="", flush=True)

        # Pipeline for getting a Grafana type DataFrame from PromQL
        status_url = build_query_url(query_config, "status", None, period) # Gets the URL for performing the query
        status_response = perform_query(status_url) # Gets JSON response from web
        status_df_nonnumeric = transform_query_response(status_response) # Transform the JSON to a DataFrame
        status_df_raw = convert_to_numeric(status_df_nonnumeric) # Transform DF to numeric

        if(len(status_df_raw) == 0):
            print(f" Status empty.")
            continue

        status_df = _preprocess_df(status_df_raw, False, step) # Preprocess DF for application

        for type in settings["type_options"]:

            print(f"\r{" "*30}\r", end="", flush=True)
            print(f"\r    Getting {type.upper()}...", end="", flush=True)

            # The same pipeline as above
            values_url = build_query_url(query_config, "values", type, period)
            values_response = perform_query(values_url)
            values_df_nonnumeric = transform_query_response(values_response)
            values_df_raw = convert_to_numeric(values_df_nonnumeric)

            if(len(values_df_raw) == 0):
                print(f" {type.upper()} values empty.")
                continue

            values_df = _preprocess_df(values_df_raw, True, step)

            print(f"\r{" "*30}\r", end="", flush=True)
            print(f"\r    Applying status to {type.upper()}...", end="", flush=True)

            final_values_df = _apply_status_df(status_df, values_df)

            identifier = GrafanaIdentifier(period[0], period[1], type, cfg_name)
            data_repo.add(identifier, final_values_df)

        print(f"\r{" "*30}\r", end="", flush=True)
        print(f"\r    Took {(time.time()-start_time):.2f}s")

    print("  Stitching...", end="", flush=True)

    start_time = time.time()
    data_repo = _stitch(data_repo)

    print(f"\r{" "*30}\r", end="", flush=True)
    print(f"\r  Stitching took {(time.time()-start_time):.2f} seconds.")

    return data_repo

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

    start_ts = to_unix_ts(values_df["Time"][0])
    end_ts = to_unix_ts(list(values_df["Time"])[-1])

    times_list = [to_unix_ts(time) for time in status_df["Time"]]

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

def _stitch(data_repo: DataRepository):
    """
    Filter a DataRepository containing multiple GrafanaIdentifiers to SourceIdentifiers
        based off of their running/pending status.

    Args:
        data_repo (DataRepository): The input repository, contains GrafanaIdentifiers to
            be transformed.
    
    Returns:
        DataRepository: The output DataRepository, contains SourceIdentifiers.
    """

    out_data_repo = DataRepository()

    for type in settings["type_strings"]:
        identifiers = data_repo.filter_ids(filter_source_type(type))
        identifiers.sort(key=lambda id: id.start_ts)

        if(len(identifiers) == 0):
            continue

        # The data frame that we're building for the current period; right now the data frames
        #   are built by month. But we should use Timeline later
        df = pd.DataFrame()
        df_ids = [] # Stores a list of identifiers for this specific dataframe
        last_dt = datetime.datetime.fromtimestamp(identifiers[0].start_ts)

        # Store the current data frame
        def store_df():
            nonlocal df, df_ids

            new_identifier = GrafanaIdentifier(df_ids[0].start_ts, df_ids[-1].end_ts, df_ids[0].type, df_ids[0].query_cfg)
            out_data_repo.add(new_identifier, df)

        def reset_df():
            nonlocal df, df_ids

            df = pd.DataFrame()
            df_ids = []

        for identifier in identifiers:

            # If the current month and year do not match the existing dataframe's month and year,
            #   switch to a new df
            curr_dt = datetime.datetime.fromtimestamp(identifier.start_ts)
            if((curr_dt.month, curr_dt.year) != (last_dt.month, last_dt.year)):
                store_df()
                reset_df()

            last_dt = curr_dt

            df_toadd = data_repo.get_data(identifier)
            df = pd.concat([df, df_toadd], ignore_index=True, sort=False)
            df_ids.append(identifier)

        store_df()

    return out_data_repo