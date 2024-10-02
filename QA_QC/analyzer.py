#!/home/thomas/myvenv/bin/python

import sys 
import numpy
import datetime
from openpyxl.workbook import Workbook
import pandas as pd
#import decimal **Used for handling decimals used in equations 
#import rich ***Used for a better looking and colorful output. I.E errors in cli


'''Key words'''
ws_keys = ['WindSpeed', 'WS', 'windspeed', 'Ws', '_S_']
wd_keys = ['WindDirection', 'WD', 'WindDir', 'wind_direction', 'wind_dir', 'Wd', '_D_', '_D1_']
filter_keys = ['WVT', 'WVt', 'Wvt', 'wvt', 'WVC', 'WVc', 'Wvc', 'wvc']
avg_keys = ['AVG', 'Avg', 'avg']
min_keys = ['MIN', 'Min', 'min']
max_keys = ['MAX', 'Max', 'max']
airtemp_keys = ['Temperature', 'temperature', 'temp', 'Temp', 'AirTC', 'TC']
rh_keys = ['Relative_Humidity', 'relative_humidity', 'RH', 'rh']
rtdtemp_keys = ['RTD', 'DeltaT', 'delta', 'rtd']
sr_keys = ['Solar_Radiation', 'solar_radiation', 'Solar_Rad', 'solar_rad', 'SR', 'sr']
precip_keys = ['Precipitation', 'precipitation', 'Precip', 'precip']
bp_keys = ['Barometric_Pressure', 'barometric_pressure', 'BP', 'bp', 'baro']


def file_read(file_path):
    try:
        df = pd.read_csv(file_path)  
        df.columns = df.columns.str.strip()
        return df

    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
    
while True:
    file_path = input("Enter the path to the CSV file (or 'exit' to quit): ")
    
    if file_path == 'exit':
        print("Exiting the program.")
        sys.exit()
    elif file_path.endswith(".csv"):
        break  
    else:
        print("This is not a CSV file. Please try again.")

        
def col_filter(name_key,filter_key):
    df = time_check()
    df_columns = df[[col for col in df.columns if any(keyword in col for keyword in (name_key))]]
    df_index = df_columns.columns  
    new_keys = df[[col for col in df.columns if any(keyword in col for keyword in (filter_key))]]
    for column in df_index:
        if any(new_key in column for new_key in new_keys):  
            return column
    return None 


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

def highlight(df,y,z):
    df = df
    color = 'background-color: yellow'
    df_color = pd.DataFrame('', index=df.index, columns=df.columns)
    df_style[y] = color
    df_sytle[z] = color
    return df_color

#check if ws is 0 for more than 3 hours, and if wd is in the same direction but ws is low. 
def ws_check():
    df = time_check()
    ws_title = str.strip(col_filter(ws_keys, filter_keys))
    wd_title = str.strip(col_filter(wd_keys, filter_keys))
    ws_column = df.loc[:,ws_title]
    wd_column = df.loc[:,wd_title]
    for ws in ws_column:
        for wd in wd_column:
            print(ws,wd)
            #            if ws == 2.005 and wd == 337.1:
#                highlight(df,ws_column,wd_column)
#                return df.style.apply(highlight, axis=None) 
    df[[ws_column,wd_column]] = df[[ws_column,wd_column]]
    
    return df


'''Export the QA/QC file'''
def QAQC_file():
    ws_check().to_excel('temp.xlsx') #, index=False)

check = str(file_path)

if  len(check) == 0:
    sys.exit()
else:    
    QAQC_file() 
    #ws_check()
#can be used for nesting code if you do not want it to run when calling functions from another script. 
#if __name__ == '__main__' 
