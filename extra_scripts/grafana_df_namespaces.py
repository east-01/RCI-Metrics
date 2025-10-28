##### SETTINGS ####
DIRECTORY = "C:/east/work/autotm-bleeding-edge/io/ytdaugust/sources"

###################
##### RUNTIME #####
###################
import re
import os
import pandas as pd

namespaces = set()

try:
    os.listdir(DIRECTORY)
except:
    print(f"Failed to list directory \"{DIRECTORY}\" does it exist?")
    exit()

for filename in os.listdir(DIRECTORY):
    file_path = os.path.join(DIRECTORY, filename)
    if not os.path.isfile(file_path):  # Only process files
        continue

    if(not filename.endswith(".csv")):
        continue

    print(f"Looking at {filename}")

    df = pd.read_csv(file_path)

    for column in df.columns:
        pattern = r'namespace="([^"]+)"'
        match = re.search(pattern, column)

        if(match):
            namespaces.add(match.group(1))

namespaces = list(namespaces)
namespaces.sort()

print("\nNamespaces:\n")
print("\n".join(namespaces))