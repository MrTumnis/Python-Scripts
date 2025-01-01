#!/home/thomas/myvenv/bin/python

import sys 
from keys import *
import numpy as np
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
        for row in df.head(n=1).itertuples():
            if any(type(item) == type('float') for item in row):
                df = df.drop(0)

        df1 = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        df1 = df1.to_frame()
        df = df.iloc[:,1:].astype(float)
        df_new = pd.concat([df1, df], axis=1)
#        df.set_index('TIMESTAMP', inplace=True) 
#        df.fillna(df.iloc[:,[0]], inplace=True)
        df.columns = df.columns.str.strip()
        return df_new

    except Exception as e:
        print(f"Error occurred: {e}")
    return None

        
'''can likely simplify and merge 'agg_type' function as there is no need (yet) to filter a single column'''
#def col_filter(name_key,filter_key=None):
#    df = file_read(file_path)
#    headers = [] 
#    df_columns = [col for col in df.columns if any(key in col for key in (name_key))]
#    df_columns = [col for col in df if any(key in col for key in (name_key))]
#    df1 = df[df_columns].iloc[:]
#    print(df1)

#    if filter_key is not None:
#        filter = [col for col in df_columns if any(key in col for key in (filter_key))]
#        for column in df_columns:
#            if any(key in column for key in filter):
#                headers.append(column)
#                return headers 
#    else:
#        return df_columns

#    return None 

'''How can I pass in multiple "keys" to be filtered into one dataframe? Store the passed keys in a list, then run them through the column checker, into another list, then concat the dataframe based on the original df column index???'''

def col_filter(*name_key,**filter_key):
    df = file_read(file_path)
    headers = [] 
    df_columns = [col for col in df.columns if any(key in col for key in (name_key))]
#    df_columns = [col for col in df if any(key in col for key in (name_key))]
    df1 = df[df_columns].iloc[:]
    print(df1)

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

    df['diff_check'] = df.iloc[:6,0].diff().head()
    one_hour = str('0 days 01:00:00')
    fifteen_min = str('0 days 00:15:00')
    five_min = str('0 days 00:05:00')
    thirty_min = str('0 days 00:30:00')
    one_day = str('1 days 00:00:00')

    time = str(df['diff_check'].mode()[0]) 

    if time == fifteen_min:
        print(f"This is a 15-min file")
    
    elif time == thirty_min:
        print(f"This is a 30-min file")

    elif time == one_hour:
        print(f"This is a 60-min file")

    elif time == one_day:
        print(f"This is a Daily file")

    elif time == five_min:
        print(f"This is a five-min file")

    else:
        print(f"{time} is not a valid time interval")



'''How do I handle columns that do not specify an aggregation? I.E EvapHr for Trench station. Should I enable the user to choose?'''

'''SETUP: need to set up an check for user input incase something other than a time is entered. Possibly use a tkinter drop down??'''
def time_change():
    df = file_read(file_path)
    df.set_index('TIMESTAMP', inplace=True)
    freq = [15, 30, 60, 1440]
#    time = input(str("What frequency would you like to change the file to? 15, 30, 60, 1440? "))
    time = int(60)
    print(f"File will be converted to a {time}-min datafile")
    new_time = [item for item in freq if item == time]
  
    if new_time[0] == int(60):
        df_avg = col_filter(avg_keys)
        ic(type(df_avg))
        df_new = df.resample('h').mean()
   # print("This is not a proper data structure, or the timestamp is not a valid interval")


'''Define Aggregation'''
def agg_type(df):
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
#    df = file_read(file_path)
    file_read(file_path)
    time_check() 
    time_change() 
#    time_file()
