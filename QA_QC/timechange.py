#!/home/thomas/myvenv/bin/python

import sys 
from keys import *
import numpy as np
import datetime
import pandas as pd
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
    time_check()
    df = file_read(file_path)
    df.set_index('TIMESTAMP', inplace=True)
    freq = [15, 30, 60, 1440]
    time = input("What frequency would you like to change the file to? 15, 30, 60, 1440? ")
    time = int(time)
    print(f"File will be converted to a {time}-min datafile")
    new_time = [item for item in freq if item == time]
   
    #STILL NEED TO FIND A WAY TO HANDLE COLUMNS WIIHOUT OBVIOUS HEADERS. I.E EVAPHR OR SIMPLE NAMING CONVENTIONS LIKE PRECIP 
    avg_fil = df.filter(regex=r"vg$", axis=1)
    max_fil = df.filter(regex=r"ax$", axis=1)
    min_fil = df.filter(regex=r"in$", axis=1)
    tot_fil = df.filter(regex=r"ot$", axis=1)
    win_fil = df.filter(regex=r"^W.*(?<![n,x])$", axis=1)
    sig_fil = df.filter(regex=r"^S.*(?<![n,x,g])$", axis=1)
    col_order = df.columns 
    freq = 'h'
    if new_time[0] == int(60):
        df_avg = avg_fil.resample((freq), closed='right', label='right').mean()
        df_max = max_fil.resample('h', closed='right', label='right').mean()
        df_min = min_fil.resample('h', closed='right', label='right').mean()
        df_tot = tot_fil.resample('h', closed='right', label='right').sum()
        df_win = win_fil.resample('h', closed='right', label='right').mean()
        df_sig = sig_fil.resample('h', closed='right', label='right').mean()
        
        df_or  = pd.concat([df_avg,df_max,df_min,df_win,df_sig,df_tot], axis=1)
        df_reordered = df_or[col_order] 
        df_new = df_reordered.apply(lambda x: round(x,2))
        print(df_new)
        return df_new, time
    #    df_new = df.resample('h').mean()
   # print("This is not a proper data structure, or the timestamp is not a valid interval")


'''Export the new time file'''
def time_file():
    date = datetime.datetime.now()
    items = time_change()
    styled_df = items[0]
    time = items[1]
    styled_df.to_csv(date.strftime("%Y%m%d") + '-' f"{time}-min_{file_path}", index=True)

if __name__ == '__main__':
    while True:
        file_path = input("Enter the path to the CSV file (or 'exit' to quit): ")
   #     file_path = ("Met.csv") 
        if file_path == 'exit':
            print("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            print("This is not a CSV file. Please try again.")
#    df = file_read(file_path)
#    file_read(file_path)
#    col_filter() 
#    time_change() 
    time_file()
