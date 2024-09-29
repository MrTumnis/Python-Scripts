#!/home/thomas/myvenv/bin/python

import sys 
import numpy
import datetime
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
            break
    df = file_read(file_path)
    if df is not None:
       break


def col_filter(name_key,filter_key):
    df = time_check()
    df_columns = df[[col for col in df.columns if any(keyword in col for keyword in (name_key))]]
    df_index = df_columns.columns  
    new_keys = df[[col for col in df.columns if any(keyword in col for keyword in (filter_key))]]
    for column in df_index:
        if any(new_key in column for new_key in new_keys):  
            print(column)
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


def ws_check():
    df = time_check()
    




'''Export the QA/QC file'''#specify the file and then add it here 
def QAQC_file():
    wind_check().to_csv('temp.csv', index=False)

check = str(file_path)

if  len(check) == 0:
    sys.exit()
else:    
    col_filter(ws_keys, filter_keys)
    #ws_check()
#can be used for nesting code if you do not want it to run when calling functions from another script. 
#if __name__ == '__main__' 
