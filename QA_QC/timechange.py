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
        df = pd.read_csv(file_path, index_col=False)  
    #df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
        #df.reset_index(drop=True, inplace=True) 
        #df.set_index('TIMESTAMP', inplace=True) 
        df.columns = df.columns.str.strip()
        return df

    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
        

def col_filter(name_key,filter_key=None):
    df = file_read(file_path)
    headers = [] 
    df_columns = [col for col in df.columns if any(key in col for key in (name_key))]
    
    if filter_key is not None:
        filter = [col for col in df_columns if any(key in col for key in (filter_key))]
        for column in df_columns:
            if any(key in column for key in filter):
                headers.append(column)
                return headers 
    else:
        return df_columns

    return None 


'''Define Timestamps'''

'''The ultimate goal is to read the timestamp automatically, and give the user an option to choose the frequency that they would like the data to change to. I.E if the file is 15-min, then change it to 1-hour. Use ".resample().mean() for data that is an  "Avg" aggregation. You can specify closed="right" or "left" to make the data hour ending or hour beginig"'''

def time_check():
    df = file_read(file_path)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
    df.dropna(subset=['TIMESTAMP'], inplace=True)
    diff_check = df['TIMESTAMP'].diff().head()

    time15 = True if (diff for diff in diff_check == pd.Timedelta(15,'min')) else print("This is not a proper time format")
    time60= True if (diff for diff in diff_check == pd.Timedelta(60,'min')) else print("This is not a proper time format")
    time24= True if (diff for diff in diff_check == pd.Timedelta(1440,'min')) else print("This is not a proper time format")
    time5 = True if (diff for diff in diff_check == pd.Timedelta(5,'min')) else print("This is not a proper time format")
    print(time60)  

#    time15 = time60 = time24 = time5 = False

    # Loop through the differences to check for time intervals
#    for diff in diff_check:
#        if diff == pd.Timedelta(15, 'min'):
#            print("This is a 15-min file")
#            time15 = True

#        elif diff == pd.Timedelta(60, 'min'):
#            print("This is a 60-min file")
#            time60 = True
#
#        elif diff == pd.Timedelta(1, 'day'):
#            print("This is a Daily file")
#            time24 = True
#        
#        elif diff == pd.Timedelta(5, 'min'):
#            print("This is a 5-min file")
#            time5 = True
    return time15, time60, time24, time5 


'''How do I handle columns that do not specify an aggregation? I.E EvapHr for Trench station. Should I enable the user to choose?'''

def time_change(df):
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
    df.dropna(subset=['TIMESTAMP'], inplace=True)
    df.set_index('TIMESTAMP', inplace=True)

    if time_check() == time15:
        print(df) 
        df_new = df.resample('1h').mean()
        ic(df_new)
#    df_new.index.name = 'TIMESTAMP'

   # print("This is not a proper data structure, or the timestamp is not a valid interval")


'''Define Aggregation'''
def agg_type(df):
   # df = file_read(file_path)
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
    styled_df = time_check(df)
    styled_df.to_csv(date.strftime("%Y%m%d") + '-' + f"{file_path}", index=True)

if __name__ == '__main__':
    while True:
   #     file_path = input("Enter the path to the CSV file (or 'exit' to quit): ")
        file_path = ("Met.csv") 
        if file_path == 'exit':
            print("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            print("This is not a CSV file. Please try again.")
    df = file_read(file_path)
    time_change(df) 
#    time_file()
