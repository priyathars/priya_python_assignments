import re
import json

with open('Test_File.json') as json_data:
    data = json.load(json_data)


def extract_values(obj, key):  # Get all values of specified key from nested JSON.
    key_list = []

    def extract(obj, key_list, key):  # Recursively search for values of key in JSON
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, key_list, key)
                elif k == key:
                    key_list.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, key_list, key)
        return key_list

    results = extract(obj, key_list, key)
    return results


search_list = extract_values(data, 'value')

pattern1 = r"(FROM|INSERT\sINTO|JOIN|UPDATE)(\s\w+)\.+(\w+)"  # Pattern to find Database names and table names
pattern2 = r"(SELECT|INSERT\sINTO)"  # Pattern to find SELECT and INSERT Queries

count_insert = 0
count_select = 0

for value in search_list:  # Search through SQL Commands
    matches1 = re.findall(pattern1, value, re.I)  # Finding Database names and table names
    matches2 = re.findall(pattern2, value, re.I)  # Finding SELECT and INSERT Queries
    if matches1:
        for i in matches1:
            print(f"Database Name: {i[1]}, Table Name: {i[2]}")

    if matches2:
        for m in matches2:
            if m == 'SELECT':
                count_select += 1
            else:
                count_insert += 1

print("COUNTS of SELECT Queries: ", count_select)
print("COUNTS of INSERT Queries: ", count_insert)
