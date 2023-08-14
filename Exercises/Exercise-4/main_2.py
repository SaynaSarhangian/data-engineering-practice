import json
import os
# from glob import glob
import pandas as pd


# simpler function
def flatten_func(json_input):
    final_dict = {}
    for key, val in json_input.items():
        if isinstance(val, dict):
            print(f'value {val} is a dictionary..flattening in progress...!')
            new_dict = flatten_func(val)
            for subkey, subval in new_dict.items():
                final_dict[f'{key}.{subkey}'] = subval
        elif isinstance(val, list):
            print(f'value {val} is a list..flattening in progress...!')
            for i, j in enumerate(val):
                final_dict[f'{key}.{i}'] = j
        else:
            final_dict[key] = val
    return final_dict


if __name__ == "__main__":
    file_list = []
    path = 'data/'
    for root, dirs, j_files in os.walk(path):
        for j in j_files:
            if j.endswith('.json'):
                file_path = os.path.join(root, j)  # join one or more path segments to create a complete path.
                print("locating json file path: ", file_path)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    flattened_data = flatten_func(data)
                    file_list.append(flattened_data)
    print(file_list)
    df_1 = pd.DataFrame(file_list)  # convert a list of keyvalue pairs to a df
    output_csv_file_2 = 'output_csv_file_2.csv'
    df_1.to_csv(output_csv_file_2, index=False)


