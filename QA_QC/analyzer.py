#!/{HOME}/{USER}/myenv/bin/python

import sys 
from keys import *
import numpy
import datetime
from openpyxl.workbook import Workbook
import pandas as pd
#import decimal #***Used for handling decimals used in equations 
from rich import print #***Used for a better looking and colorful output. I.E errors in cli
from icecream import ic
from functools import lru_cache
from rich.console import Console
from rich.theme import Theme
from rich.prompt import Prompt

custom_theme = Theme({
    "info": "dim cyan",
    "success" : "dodger_blue2",
    "warning": "magenta",
    "error": "bold red"
})

'''Wind Compass'''
north   = [337.6, 22.5]
north_e = [22.6, 67.5]
east    = [67.6,112.5]
south_e = [112.6,157.5]  
south   = [157.6,202.5]
south_w = [202.6,247.5]
west    = [247.6,292.5]
north_w = [292.6, 337.6]

cardinal = {"N","NE","E","SE","S","SW","W","NW","N"}




def file_read(file_path):
    try:
        df = pd.read_csv(file_path)  
        df.columns = df.columns.str.strip()
        return df

    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
       


def col_filter(name_key,filter_key,non_key=None):
    df = time_check()
    headers = [] 
    df_columns = [col for col in df.columns if any(key in col for key in (name_key))]
    filter = [col for col in df_columns if any(key in col for key in (filter_key))]

    for column in df_columns:
        if any(key in column for key in filter):
            headers.append(column)
    
    if non_key is not None:
        for header in headers:
            if any(nk in header for nk in non_key):
                headers.remove(header)
                string = (''.join(headers))
                return string.strip()
    else:
        string = (''.join(headers))
        return string.strip()

    return None 


def cell_color(df, condition, color, col, col2=None):

    if isinstance(df, pd.DataFrame):
        df_style = pd.DataFrame('', index=df.index, columns=df.columns)  

    elif isinstance(df, pd.Series):
        df_style = pd.Series()
        
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")
   
        if col2 is not None and col2 not in df.columns:
            raise ValueError(f"Column '{col2}' not found in the DataFrame.")
   
    # Apply coloring based on the condition
    if col2:
        df_style.loc[condition, [col, col2]] = f'background-color: {color}' 
    else: 
        df_style.loc[condition, col] = f'background-color: {color}' 

    return df_style


'''QA checks'''
#check for missing timestamps and fill in NANs if missing
def time_check():
    df = file_read(file_path)
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'].iloc[1:], errors='coerce')
        df.dropna(subset=['TIMESTAMP'], inplace=True)
        df.set_index('TIMESTAMP', inplace=True) 
        df_new = df.reindex(pd.date_range(start=df.index[0], end=df.index[-1], freq='15min')) 
        df_new.index.name = 'TIMESTAMP'
   
        return df_new

'''Each function needs a condition if it requires a colored cell for flagging'''
def ws_check():
    df = file_read(file_path)
    ws_header_10m = col_filter(ws_keys, vector_keys, twenty_m_keys)
    ws_header_20m = col_filter(ws_keys, vector_keys, ten_m_keys)
    wd_header = col_filter(wd_keys, vector_keys, sigma_keys)
   
    # Create a mask for ws_col where values are less than or equal to 0.5
    zero_mask = (df[ws_header_10m] <= 0.5) 
#    condition = pd.Series(False, index=df.index)  
    try: 
        # Loop through the DataFrame, checking conditions on 4 rows
        for i in range(len(df) - 1):  # May need to change to len(df) - 3 to avoid index out of bounds
            if zero_mask[i:i + 3].all():  # Check if the first three values are all True
                # Check if the wd column values are within ±(x) of each other for four consecutive rows
                if (df[wd_header].iloc[i:i + 4].max() - df[wd_header].iloc[i:i + 4].min()) <= 3:
                     condition[i:i + 4] = True  # Set condition for these four rows
        return df.style.apply(lambda x: cell_color(x, condition, color='yellow', col=ws_header, col2=wd_header), axis=None)

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None

def wd_check():
    df = file_read(file_path)
    wd_header = col_filter(wd_keys, vector_keys, sigma_keys)
    zero_mask = df[wd_header] <= 0
    condition = pd.Series(False, index=df[wd_header])
    df[wd_header] = df[wd_header] <= 0
#    condition[+ 4] = True
    ic(condition)
    return df.style.apply(lambda x: cell_color(x, condition, color='lightred', col=wd_header, col2=None))

#    zero_mask = (df[wd_header] <= 0.5)
#    condition = pd.DataFrame() 
#    rolling = df[wd_header].rolling(window=4).apply(lambda x: x.max() - x.min())
#    condition = (rolling <= 1)
#    return df.style.apply(lambda x: cell_color(x, condition, color='lightred', col=wd_header, col2=None))



#check for 0's or extreme negatives. Compare to RTD 2m, if it exists. 
def temp_check():
    df = file_read(file_path)
    temp_header = col_filter(temp_keys, avg_keys, bp_keys)
    rtd2m_header = col_filter(rtd_keys, two_m_keys)
    rh_header = col_filter(rh_keys, avg_keys)
    zero_mask = (df[temp_header] == 0) 

    return df.style.apply(lambda x: cell_color(condition, color='lightred', col=temp_header, col2=rh_header), axis=None)


#def rtd_check():
#    rtd2m_title = str.strip(col_filter(rtd_keys, twom_keys))
#def rh_check():
#def sr_check():
#def precip_check():


'''Export the QA/QC file'''
def QAQC_file():
    try:
        date = datetime.datetime.now()
        styled_df = ws_check()
        styled_df.to_excel(date.strftime("%Y%m%d") + '-' + f"{file_path.strip('.csv')}" + '.xlsx', index=False)

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None
if __name__ == '__main__':
    while True:
      #  file_path = input("Enter the path to the 15-min CSV file (or 'exit' to quit): ")
        file_path = 'Met.csv'
        if file_path == 'exit':
            print("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            print("This is not a CSV file. Please try again.")

    QAQC_file()
