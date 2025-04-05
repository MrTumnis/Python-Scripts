
import polars as pl
import os 
import glob 
import zipfile
import sys
import icecream as ic
from simple_term_menu import TerminalMenu

def read_file() -> dict:

    #need to find a way to check for random values in first row and drop them to cast schema properly(if schema is needed?) 
    # null_items = ['TIMESTAMP', 'm/s', '\u00B0', ""]
    
    try:
        file_list = glob.glob(os.path.join('**'))
        files_menu = TerminalMenu(file_list, title=('Choose the path of the files. (can be a .zip file)'))
        selection = files_menu.show()
        file_path = file_list[selection]
        print(os.path.isdir(file_path))

        
        if os.path.isdir(file_path): 
            dir = os.listdir(file_path)
            dir_menu = TerminalMenu(dir)
            selection = dir_menu.show()
            file_path = dir[selection]
            print(os.path.isdir(file_path))

        elif os.path.isfile(file_path): 
            files = glob.glob(os.path.join('**'))
            files_menu = TerminalMenu(files)
            selection = files_menu.show()
            file_path = files[selection]

    except Exception as e:
        print(e)

    try:
        if file_path.endswith('.zip'):
            with zipfile.ZipFile(f'{file_path}', 'r') as zip_file:
                file_path = file_path.strip('.zip')
                file_path_new = zip_file.extractall(f'{file_path}')
        



    except Exception as e:
        print(e)
        
if __name__=='__main__':
    read_file()

