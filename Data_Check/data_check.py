#!/.venv/bin/python
# /// script
# requires-python = ">=3.13"
# dependencies = []
# ///

import polars as pl
import os 
import glob 
import zipfile
import sys
import icecream as ic
from simple_term_menu import TerminalMenu


#Return all lazy files in a dictionary for easy reference and append height of each range gate to column name 
def read_file() -> dict:

    #need to find a way to check for random values in first row and drop them to cast schema properly(if schema is needed?) 
    # null_items = ['TIMESTAMP', 'm/s', '\u00B0', ""]
    
    # try:
    file_list = glob.glob(os.path.join('**'))
    files_menu = TerminalMenu(file_list, title=('Choose the path of the Sodar files. (can be a .zip file)'))
    selection = files_menu.show()
    file_path = file_list[selection]

    if file_path.endswith('.zip'):
        with zipfile.ZipFile(f'{file_path}', 'r') as zip_file:
            file_path = file_path.strip('.zip')
            file_path_new = zip_file.extractall(f'{file_path}')

    for files in os.walk(file_path):
        for file in files:
            print(file)
            agg = 'Table15'
            if agg in file:
                print(file)
        #     print(files)
        #     case file = (pl
        #         .scan_csv(f'{file_path}/{files}_Table15.csv', has_header=True, null_values=null_items, raise_if_empty=True)
            # .with_columns(pl
            # .col('TIMESTAMP').str
            # .to_datetime('%Y-%m-%d %H:%M:%S',time_unit=None, time_zone=None, strict=False))
        # )

if __name__=='__main__':
    read_file()

