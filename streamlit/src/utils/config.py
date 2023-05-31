import os
import json
from typing import Any

# update this value as we go along
LAST_JOB_ID = 641964054544138
DATA_FOLDER = '../data'
OUTPUT_FOLDER = '../output'


def check_data_output_folder(): 
    if not os.path.exists(DATA_FOLDER):
        os.mkdir(DATA_FOLDER)
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
        
def save_json_data(data: Any, file_name: str): 
    print("Save the data to the output folder")
    with open(os.path.join(OUTPUT_FOLDER, file_name), 'w') as f:
        json.dump(data, f, indent=4)
        
def read_from_json(f_name: str): 
    with open(os.path.join(OUTPUT_FOLDER, f_name), 'r') as f:
        data = json.load(f)
    return data