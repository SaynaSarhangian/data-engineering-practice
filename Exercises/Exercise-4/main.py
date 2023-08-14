import json
import os
# from glob import glob
import pandas as pd


# passing the func a loaded json file and it returns an amended flattened dictionary of that file contents
def flatten_json(json_obj, parent_key='', separator='_'):
    items = {}
    if isinstance(json_obj, dict):
        for k, v in json_obj.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            items.update(flatten_json(v, new_key, separator))
    elif isinstance(json_obj, list):
        for i, v in enumerate(json_obj):
            new_key = f"{parent_key}{separator}{i}" if parent_key else str(i)
            items.update(flatten_json(v, new_key, separator))
    else:
        items[parent_key] = json_obj # Handle simple value (no nested structure)
    # print(f'updated json items are: {items}')
    return items  # final amended dictionary


# the isinstance() function is the most reliable way to check if a variable is an instance of a given class, here Dictionary.


if __name__ == "__main__":
    flattened_data = []
    path = 'data/'
    for root, dirs, j_files in os.walk(path):
        for j in j_files:
            if j.endswith('.json'):
                file_path = os.path.join(root, j)  # join one or more path segments to create a complete path.
                print("locating json file path: ", file_path)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    print(f'loading json file {j} and flattening: ')
                    flattened = flatten_json(data)  # pass the loaded json file to flattening function
                    flattened_data.append(flattened)  # append each flattened file (dict) to a list
                    for key, value in flattened.items():
                        print(f"{key}: {value}")
                    print(flattened_data)


    df = pd.DataFrame(flattened_data)  # convert a list of keyvalue pairs to a df
    output_csv_file = 'output_csv_file.csv'
    df.to_csv(output_csv_file, index=False)#convert the df to a csv file

# Flattening a JSON file refers to the process of converting a nested or hierarchical JSON structure
# into a simpler, tabular format where each key-value pair is represented as a separate column in a flat structure. insted of having a nested value.
# could have used pd.json_normalize() to flatten instead of the func.


# make a list of all json files in current directory
# def find_json():
#     hits = glob("**/*.json", recursive=True)## If recursive is true, the pattern “**” will match any files and zero or more directories, subdirectories and symbolic links to directories.
#     json_list = []
#     for item in hits:
#         json_list.append(item.split('\\')[-1])
#     print(f'the json files are: {json_list}')
#     # ['file-1.json', 'file-4.json', 'file-3.json', 'file-2.json']
#     return json_list
