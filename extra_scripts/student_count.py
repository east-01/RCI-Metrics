import pandas as pd

##### SETTINGS ####
in_filepath = "./src/extra_scripts/student_counts_in.txt"
out_filepath = "./src/extra_scripts/student_counts.xlsx"
email_strips = ["my.", ".edu"]
order = {
    "sdsu": [],
    "csusb": [],
    "humboldt": [],
    "sfsu": [],
    "csus": [],
    "csusm": [],
    "csun": ["uwosh"],
    "fullerton": [],
    "csuchico": [],
    "csustan": [],
    "csufresno": ["mail.fresnostate"],
    "sjsu": [],
    "calstatela": [],
    "csuci": [],
    "cpp": [],
    "csulb": [],
    "csudh": [],
    "calpoly": [],
    "calstate": [],
    "sonoma": [],
    "csub": [],
    "csueastbay": [],
    "csumb": [],
    "csum": []
}

###################
##### RUNTIME #####
###################
print(f"Reading input file from \"{in_filepath}\"")

schools_list = []
seen_users = set()

# Open file for reading
with open(in_filepath, 'r') as file:
    contents = file.read()

# Go through each file line, adding in the school from the second half of the email
for content in contents.split("\n"):
    if(content is None or content == ""):
        continue

    if("@" not in content):
        print(f"Content \"{content}\" didn't have an @")
        continue

    content = content.replace("-", "").strip()
    content_split = content.split("@")
    
    # Check if user has been seen
    user = content_split[0]
    if(user in seen_users):
        continue

    # Get the school and strip all email_strips from it
    school = content_split[1]
    for to_remove in email_strips:
        school = school.replace(to_remove, "")

    schools_list.append(school)

print(f"Counting {len(schools_list)} users...")

# A dictionary of name keys and count values for each 
buckets = {}

for school in schools_list:
    if(school not in buckets):
        buckets[school] = 0

    buckets[school] += 1

# Assign counts to each key in the ordering from settings
# Merges aliases, so output[csun] = buckets[csun]+buckets[uwosh]
output = {}
counted_schools = set() # A set of all school names used in the total

for order_key in order.keys():
    counted_schools.add(order_key)

    if(order_key in buckets):
        total = buckets[order_key]
    else:
        total = 0

    aliases = order[order_key]
    for alias in aliases:
        counted_schools.add(alias)

        if(alias in buckets):
            total += buckets[alias]
    
    output[order_key] = total

output_df = pd.DataFrame(list(output.items()), columns=['school', 'count'])

try:
    with pd.ExcelWriter(out_filepath, engine="xlsxwriter") as writer:
        output_df.to_excel(writer, sheet_name="Student counts", index=False)
    print(f"Saved output to \"{out_filepath}\"")
except PermissionError:
    print("ERROR: Recieved PermissionError when trying to save summary excel spreadsheet. This can happen if the sheet is open in another window (close excel).")

# Count schools missed in ordering
missed = set(buckets.keys())-counted_schools
if(len(missed) > 0):
    print("Missed schools in total:", ", ".join(missed))
    print("These must be added to the order variable at the top of this script as either a new key or an alias for another school.")
else:
    print("All schools are accounted for in total.")