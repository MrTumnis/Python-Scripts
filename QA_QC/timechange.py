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
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
#        if df['TIMESTAMP'].isna().any():
#                ic("Value in TIMESTAMP column is 'Not a Time'")

        df.dropna(subset=['TIMESTAMP'], inplace=True)
        df.set_index('TIMESTAMP', inplace=True)
        df_new = df.reindex(pd.date_range(start=df.index[1], end=df.index[-1], freq='60min'))  
        df_new.index.name = 'TIMESTAMP'
        ic(df_new)
        return df_new

'''Define Aggregation'''
def agg_type():
    df = file_read(file_path)
    df_avg = col_filter(avg_keys)
    df_tot = col_filter(tot_keys)
    df_max = col_filter(max_keys)
    df_min = col_filter(min_keys)
    df_vec = col_filter(vector_keys)
    timestamp = []

#average the columns from an hour ending format i.e 00:00 to 00:15 is the 00:15 data point
   # if df["TIMESTAMP"].diff() == 1:
    col = [col for col in df if any(key in col for key in (df_avg))]
    ic(df[col])

#Timestamp differnce check 
    for time in df["TIMESTAMP"].loc[:].head():
        time.strip('TIMESTAMP')
        timestamp.append(time)
        print(timestamp)
    #df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
#    new = df["TIMESTAMP"].iloc[3] - df["TIMESTAMP"].iloc[4] 
    #ic(df["TIMESTAMP"].diff())

'''Export the new time file'''
def time_file():
    date = datetime.datetime.now()
    styled_df = agg_type()
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
