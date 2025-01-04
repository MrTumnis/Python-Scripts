#!/home/thomas/myvenv/bin/python

import sys 
import numpy as np
import datetime
import pandas as pd
from icecream import ic
from rich import print as rprint #***Used for a better looking and colorful output. I.E errors in cli
from rich.console import Console
from rich.theme import Theme
from rich.prompt import Prompt

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

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
        df.columns = df.columns.str.strip()
        return df_new

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None

        


'''Define Timestamps'''
def time_check():
    df = file_read(file_path)

    df['diff_check'] = df.iloc[:6,0].diff().head()
    one_hour = str('0 days 01:00:00')
    fifteen_min = str('0 days 00:15:00')
    five_min = str('0 days 00:05:00')
    thirty_min = str('0 days 00:30:00')
    one_day = str('1 days 00:00:00')

    time = str(df['diff_check'].mode()[0]) 
    try:
        if time == fifteen_min:
            rprint(f"This is a 15-min file")
            time = 15 
        elif time == thirty_min:
            rprint(f"This is a 30-min file")
            time = 30
        elif time == one_hour:
            rprint(f"This is a 60-min file")
            time = 60
        elif time == one_day:
            rprint(f"This is a Daily file")
            time = 1440
        elif time == five_min:
            rprint(f"This is a five-min file")
            time = 5
    except:
        raise Exception (f"{time} is not a valid time interval")
    return time 

'''NEED TO RAISE ERROR FOR NON INT TYPE FOR INPUT'''
def time_change():
    check = time_check()
    df = file_read(file_path)
    df.set_index('TIMESTAMP', inplace=True)
    freq = [15, 30, 60, 1440]
    try:
        time = Prompt.ask("What frequency would you like to change the file to? [violet]15, 30, 60, 1440?[/violet] (or [red]'exit'[/red] to quit) ")
        if time == 'exit':
            sys.exit()
        time = int(time)
        if time < check:
            raise Exception ("Cannot convert to a time lower than that of the file") 
    except ValueError:
        console.print(f"Please specify a valid time as an integer ({freq})", style='warning')
        time_file()

    #STILL NEED TO FIND A WAY TO HANDLE COLUMNS WIIHOUT OBVIOUS HEADERS. I.E EVAPHR OR SIMPLE NAMING CONVENTIONS LIKE PRECIP 
    avg_fil = df.filter(regex=r"Avg$", axis=1)
    max_fil = df.filter(regex=r"Max$", axis=1)
    min_fil = df.filter(regex=r"Min$", axis=1)
    tot_fil = df.filter(regex=r"Tot$", axis=1)
    win_fil = df.filter(regex=r"^W.*(?<![n,x])$", axis=1)
    sig_fil = df.filter(regex=r"^S.*(?<![n,x,g])$", axis=1)
    col_order = df.columns 
#    check = time_check()
    try:    
        new_time = [item for item in freq if item == time]

        if new_time[0] == int(15):
            freq = '15min'
        elif new_time[0] == int(30):
            freq = '30min'
        elif new_time[0] == int(60):
            freq = 'h'
        elif new_time[0] == int(1440):
            freq = 'd'
         
    except Exception as e:
        console.print(f"#2 Error occurred: {e}", style='error')
        sys.exit()

    df_avg = avg_fil.resample((freq), closed='right', label='right').mean()
    df_max = max_fil.resample((freq), closed='right', label='right').mean()
    df_min = min_fil.resample((freq), closed='right', label='right').mean()
    df_win = win_fil.resample((freq), closed='right', label='right').mean()
    df_sig = sig_fil.resample((freq), closed='right', label='right').mean()
    df_tot = tot_fil.resample((freq), closed='right', label='right').sum()
        
    try: 
        df_or = pd.concat([df_avg,df_max,df_min,df_win,df_sig,df_tot], axis=1)
        df_list = df.columns.to_list()
        df_or_list = df_or.columns.to_list()
        df_list.extend(df_or_list)

        if len(df_list) == len(df_or_list):
            df_reor = df_or[col_order] 
            df_new = df_reor.apply(lambda x: round(x,2))
            return df_new, time

#lengthy way to handle columns not easily found via regex
        elif len(df_list) != len(df_or_list):
            df_list = list(set(df_list) ^ set(df_or_list))
            console.print(f"Rows [green]{df_list}[/green] were not recognized.", style='warning')
            user_input = {}
            i = 0
            while i < len(df_list): 
                for item in df_list:
                    input = Prompt.ask(f"What would you like to do for [bold green]{item}[/bold green]? Enter: Avg or Sum. ")
                    value = input.lower()
                    user_input.update({item:value})
                    df_temp = df[item]
                    df_new = pd.concat([df_temp])
                    i+=1
            val_avg = [key for key, val in user_input.items() if val == 'avg']
            val_sum = [key for key, val in user_input.items() if val == 'sum']
            df_avg = df[val_avg]
            df_sum = df[val_sum]
            df_cus = df_avg.resample((freq), closed='right', label='right').mean()
            df_cus = df_sum.resample((freq), closed='right', label='right').sum()
            df1= pd.concat([df_or,df_cus,df_avg], axis=1)
            df_reor = df1[col_order]
            df_new = df_reor.apply(lambda x: round(x,2))
            ic(df_new)
            return df_new, time 
        else:
            raise 
            sys.exit() 
       # console.print(f"Success! File has been converted to a {time}-min datafile", style='info')
    
        #return df_new, time

    except Exception as e:
        console.print(f"#3 Error occurred: {e}", style='error')
        sys.exit()

        
'''Export the new time file'''
def time_file():
    date = datetime.datetime.now()
    try: 
        items = time_change()
        styled_df = items[0]
        time = items[1]
        styled_df.to_csv(date.strftime("%Y%m%d") + '-' f"{time}-min_{file_path}", index=True)
        console.print(f"Success! File has been converted to a {time}-min datafile", style='info')
    
    except Exception as e:
        console.print(f"#4 Error occurred: {e}", style='error')
        sys.exit()

if __name__ == '__main__':
    while True:
        file_path = Prompt.ask("Enter the path to the CSV file (or [red]'exit'[/red] to quit): ")
   #     file_path = ("Met.csv") 
        if file_path == 'exit':
            rprint("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            console.print("This is not a CSV file. Please try again.", style='error')
#    df = file_read(file_path)
#    file_read(file_path)
#    col_filter() 
#    time_change() 
    time_file()
