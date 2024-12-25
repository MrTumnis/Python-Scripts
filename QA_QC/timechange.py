#!/home/thomas/myvenv/bin/python

import sys 
from keys import *
import numpy
import datetime
#from openpyxl.workbook import Workbook
import pandas as pd
#import decimal #***Used for handling decimals used in equations 
from rich import print #***Used for a better looking and colorful output. I.E errors in cli
from icecream import ic
from functools import lru_cache


def file_read(file_path):
    try:
        df = pd.read_csv(file_path)  
        df.columns = df.columns.str.strip()
        return df

    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
       


def col_filter(name_key,filter_key=None):
    df = time_check()
    headers = [] 
    df_columns = [col for col in df.columns if any(key in col for key in (name_key))]
    
    if filter_key is not None:
        filter = [col for col in df_columns if any(key in col for key in (filter_key))]
        for column in df_columns:
            if any(key in column for key in filter):
                headers.append(column)
    
    else:
        return df_columns

    return None 


'''Define Timestamps'''
#check for timestamp aggregation and change it to user specified...eventually
def time_check():
    df = file_read(file_path)
 #   if 'TIMESTAMP' in df.columns:
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
    df.dropna(subset=['TIMESTAMP'], inplace=True)
    
    diff_check = df["TIMESTAMP"].diff()
#    ic(diff_check) 
    for diff in diff_check:
        if diff == pd.Timedelta(15,'min'):
            print("This is a 15-min file")
            break 
#    sixty_check = (diff_check == pd.Timedelta(60,'min')).head():
#        if any(sixty_check == True):
#            print("This is a 60-minute file")
    else:
        print("This is not a proper data structure, or the timestamp is not a valid interval")
    #        df_new = df.reindex(pd.date_range(start=df.index[1], end=df.index[-1], freq='60min'))  
#        df_new.index.name = 'TIMESTAMP'
    return df

'''Define Aggregation'''
def agg_type(df_new):
   # df = file_read(file_path)
    df = df_new
    df_avg = col_filter(avg_keys)
    df_tot = col_filter(tot_keys)
    df_max = col_filter(max_keys)
    df_min = col_filter(min_keys)
    df_vec = col_filter(vector_keys)
    
#average the columns from an hour ending format i.e 00:00 to 00:15 is the 00:15 data point
    col = [col for col in df if any(key in col for key in (df_avg))]
#    ic(df[col])

#    timestamp= df["TIMESTAMP"]
#    diff_check = timestamp.diff()
#    fifteen_check = (dif_check == pd.Timedelta(15,'min')).head()
#    if any(fifteen_check == True):
#        print(check)


'''Export the new time file'''
def time_file():
    date = datetime.datetime.now()
    styled_df = agg_type(time_check())
    styled_df.to_csv(date.strftime("%Y%m%d") + '-' + f"{file_path}", index=True)

if __name__ == '__main__':
    while True:
        file_path = input("Enter the path to the CSV file (or 'exit' to quit): ")

        if file_path == 'exit':
            print("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            print("This is not a CSV file. Please try again.")

    time_file()
