"""
The Query Executor actually performs the GET request then transforms the returned data into the
  Grafana DF we're expecting.
"""

import datetime
import json
import pandas as pd
import requests

from src.utils.timeutils import from_unix_ts

def perform_query(queryURL):
    """
    Perform an HTTP GET request with the queryURL, handle the response and return the DataFrame
    """

    cache_mode = 'nocache'
    json_response = None

    if(cache_mode == 'nocache' or cache_mode == 'save'):
        response = requests.get(queryURL)

        if(response.status_code != 200):
            raise Exception(f"Failed to perform PromQL query for url:\n{queryURL}")

        # Check if 'data' is in the response JSON to avoid KeyError
        json_response = response.json()
        if 'data' not in json_response:
            raise Exception(f"Missing 'data' in the response:\n{json_response}")

    if(cache_mode == 'save'):
        with open('./cached_response.json', 'w') as f:
            json.dump(json_response, f)
            print("Saved json.")
    elif(cache_mode == 'use'):
        print("WARN: Used cached response instead of performing a new PromQL query.")
        with open('./cached_response.json', 'r') as f:
            json_response = json.load(f)

    return json_response['data']['result']

def transform_query_response(query_response):
    """
    Given query_response json, produce a “time-joined” table like Grafana's CSV export.
    """
    # 1) Helper to turn metric dict → {key="value",…} string
    def fmt_metric(mdict):
        return "{"+", ".join(f'{k}=\"{v}\"' for k,v in mdict.items())+"}"

    # 2) Build a flat list of (timestamp, metric_str, value)
    records = []
    for series in query_response:
        mstr = fmt_metric(series['metric'])
        # extend with tuples (time, metric, value)
        # values is list of [time, value]
        records.extend((int(ts), mstr, val)
                       for ts, val in series['values'])

    # 3) One-shot construction of the long DataFrame
    long_df = pd.DataFrame.from_records(records,
                                        columns=['Time','Metric','Value'])

    # 4) Pivot so each metric becomes its own column, outer‐joining on Time
    out_df = long_df.pivot_table(index='Time',
                                 columns='Metric',
                                 values='Value',
                                 aggfunc='first') \
                    .reset_index()

    # 5) Convert Unix seconds → datetime once, vectorized
    out_df['Time'] = out_df['Time'].map(from_unix_ts)

    return out_df