import json
import os
# from glob import glob
import pandas as pd


# def find_json():
#     hits = glob("**/*.json", recursive=True)
#     json_list = []
#     for item in hits:
#         json_list.append(item.split('\\')[-1])
#     print(f'the json files are: {json_list}')
#     # ['file-1.json', 'file-4.json', 'file-3.json', 'file-2.json']
#     return json_list

# passing flatten_json a loaded json file and it returns a new dictionary of that file content
def flatten_json(json_obj, parent_key='', separator='_'):
    items = {}
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        # check if value is a dictionary
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, separator))
        else:
            items[new_key] = value
    print(f'updated json items are: {items}')
    return items  # final amended dictionary


# the isinstance() function is the most reliable way to check if a variable is n instance of a given class, here Dictionary.


if __name__ == "__main__":
    flattened_data = []
    path = 'data/'
    for root, dirs, j_files in os.walk(path):
        for j in j_files:
            if j.endswith('.json'):
                file_path = os.path.join(root, j)  # join one or more path segments to create a complete path.
                print("Reading file as a result of os.path.join():", file_path)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    flattened = flatten_json(data)  # pass the loaded json file to flattening function
                    flattened_data.append(flattened)  # append each flattened file to a list
    print(f'here is the resulting list of flattened json files with length of {len(flattened_data)}: {flattened_data}')
    df = pd.DataFrame(flattened_data)
    print(f'the columns/headers of the resulting df are:{df.columns}')
    output_csv_file = 'output_csv_file.csv'
    df.to_csv(output_csv_file, index=False)
# If recursive is true, the pattern “**” will match any files and zero or more directories, subdirectories and symbolic links to directories.
# Flattening a JSON file refers to the process of converting a nested or hierarchical JSON structure
# into a simpler, tabular format where each key-value pair is represented as a separate column in a flat structure. insted of having a nested value.
