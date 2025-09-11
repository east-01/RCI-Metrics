import datetime
import pandas as pd

from plugins.rci.promql.settings import settings
from src.utils.timeutils import to_unix_ts

def convert_to_numeric(df: pd.DataFrame):
    # Convert the data frame to numeric values so we can properly analyze it later.
    df.iloc[:, 1:] = df.iloc[:, 1:].map(pd.to_numeric, errors="coerce")
    return df

def _extract_column_data(col_name):
    """
    Given a column name with the format {label1="value1", label2="value2",...} break it down into a
      usable python dictionary.
    """
    # Strip opening and closing {}
    col_name = col_name[1:-1]
    data = {}

    for pair in col_name.split(', '):
        pairsplit = pair.split('=')
        label = pairsplit[0]
        value = pairsplit[1][1:-1] # Grab the value without the first and last characters to remove quotes.

        data[label] = value

    return data

def get_resource_type(df):
    """
    Analyze each column data in the DataFrame to get the set of unique resource types.
    The resource type is the string following the resource= tag in the column title.
    It is expected that the DataFrame has uniform resource types among all columns.

    Args:
        df (pd.DataFrame): A GRAFANA pandas DataFrame. Expecting formatting from Grafana generated
            .csv, with the Time column included.
    
    Returns:
        string: The resource type of all columns in the DataFrame.

    Raises:
        Exception: Column doesn't include a resource type, more than one resource type is in the
            DataFrame, the settings["type_string"] doesn't contain the target type- the function
            won't be able to reverse the program type from the type string.
    """
    col_datas = [_extract_column_data(col_name) for col_name in df.columns[1:]]

    # Create a set of all the type strings, this will give a list of unique type names
    type_set = set()
    for col_data in col_datas:
        if("resource" not in col_data.keys()):
            raise Exception(f"Column \"{col_data}\" doesn't have a resource type.")
        type_set.add(col_data["resource"])

    # If the length of the set is more than one we have an invalid DF
    if(len(type_set) == 0):
        raise Exception(f"Type analysis error: yielded no types. It is expected that the DataFrame has consistent resource types between all columns.")
    if(len(type_set) > 1):
        raise Exception(f"Type analysis error: yielded more than one type: {list(type_set)}. It is expected that the DataFrame has consistent resource types between all columns.")

    # Reverse the type string (i.e. "nvidia_com_gpu") to it's associated program type (i.e. "gpu")
    type_string = list(type_set)[0]
    type = None
    
    # Find the program type by finding the dictionary key from its value
    type_strings = settings['type_strings']
    for possible_type in type_strings.keys():
        if(type_strings[possible_type] == type_string):
            type = possible_type
            break

    if(type is None):
        raise Exception(f"Type analysis error: couldn't reverse type string \"{type_string}\" into it's associated program type. [{type_strings.keys()}]")

    return type

def get_period(df):
    """
    Ensure the DataFrame has a Time column, returning the start and ending times.
    """
    
    if('Time' not in df.columns):
        raise Exception("Period analysis error: \"Time\" not in the columns of the DataFrame.")

    times = list(df['Time'])

    if(len(times) < 1):
        raise Exception("Period analysis error: Time column of length less than 1")
    
    start = to_unix_ts(times[0])
    start_dt = datetime.datetime.fromtimestamp(start)

    end = to_unix_ts(times[-1])
    end_dt = datetime.datetime.fromtimestamp(end)
    
    if(start_dt.month != end_dt.month or start_dt.year != end_dt.year):
        print("ERROR: Analyzing df's period shows that the start and end times belong to different months. This will most likely yield broken results. Try using a PromQL query instead of input directory.")

    return (start, end)

