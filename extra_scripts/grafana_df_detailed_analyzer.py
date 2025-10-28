import argparse
import pandas as pd
import re

if(__name__ != "__main__"):
    print("This script is only supposed to be executed by itself")
    exit()

def select_uid(string):
    if string == "Time":
        return string

    match = re.search(r'uid="([^"]+)"', string)
    if match:
        uid = match.group(1)
        return uid
    else:
        raise Exception(f"Failed to read uid in column name \"{string}\"")
    
parser = argparse.ArgumentParser(prog='AutoTM', description='Auto Tide Metrics- Collect and visualize tide metrics')

parser.add_argument('df1', type=str, help="The path to the first .csv file to compare")
parser.add_argument('df2', type=str, help="The path to the second .csv file to compare")
parser.add_argument("--output", type=str, help="The optional path to the output file")

args = parser.parse_args()

def strip_uids_sort_n_save(df, address):
    uids = [select_uid(col) for col in df.columns]
    df.columns = uids # Rename columns to uid only names
    
    uids.sort() # Sort uids and apply to df
    df = df[uids]

    df.to_csv(address.replace('.csv','_sorted.csv'), index=False)
    return df

df1 = pd.read_csv(args.df1)
df1 = strip_uids_sort_n_save(df1, args.df1)

df2 = pd.read_csv(args.df2)
df2 = strip_uids_sort_n_save(df2, args.df2)

# Get shared and unique columns
shared_cols = df1.columns.intersection(df2.columns).tolist()
df1_only_cols = df1.columns.difference(df2.columns).tolist()
df2_only_cols = df2.columns.difference(df1.columns).tolist()

# Sort using select_uid
shared_cols.sort()
df1_only_cols.sort()
df2_only_cols.sort()

# Reorder
shared = df1[shared_cols]
df1_reordered = df1[df1_only_cols]
df2_reordered = df2[df2_only_cols]

print(f"Shared: {len(shared_cols)}, df1: {len(df1_only_cols)}, df2: {len(df2_only_cols)}")

# Export to Excel workbook
try:
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        shared.to_excel(writer, sheet_name="Intersection", index=False)
        df1_reordered.to_excel(writer, sheet_name="DataFrame1", index=False)
        df2_reordered.to_excel(writer, sheet_name="DataFrame2", index=False)
except PermissionError as e:
    print("Permission error when trying to save output excel spreadsheet. (Close excel)")
    