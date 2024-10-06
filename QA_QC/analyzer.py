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

'''Highlight Colors'''
yellow = 'yellow'
lblue = 'lightblue'
lred = 'lightred'


def file_read(file_path):
    try:
        df = pd.read_csv(file_path)  
        df.columns = df.columns.str.strip()
        return df

    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
    
        
def col_filter(name_key,filter_key):
    df = time_check()
    df_columns = df[[col for col in df.columns if any(keyword in col for keyword in (name_key))]]
    df_index = df_columns.columns  
    new_keys = df[[col for col in df.columns if any(keyword in col for keyword in (filter_key))]]
    for column in df_index:
        if any(new_key in column for new_key in new_keys):  
            return column
    return None 


def cell_color(df, condition, col, col2=None):
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in the DataFrame.")
    if col2 is not None and col2 not in df.columns:
        raise ValueError(f"Column '{col2}' not found in the DataFrame.")

    df_style = pd.DataFrame('', index=df.index, columns=df.columns)  # Initialize style DataFrame

    # Apply coloring based on the condition
    if col2:
        df_style.loc[condition, [col, col2]] = f'background-color: {yellow}'  # Color for both columns
    else:
        df_style.loc[condition, col] = f'background-color: {lblue}'  # Color only for ws_col

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

def ws_check(df, ws_col, wd_col):
    zero_mask = (df[ws_col] <= .5) # Identify non-negative values in ws_col
    condition = pd.Series(False, index=df.index)  # Initialize a condition Series

    for i in range(len(df) - 1):  # Iterate over the DataFrame
        if zero_mask[i:i + 3].all():  
            # Check if the wd_col values are within ±1 of each other for three consecutive rows
            if (df[wd_col].iloc[i:i + 4].max() - df[wd_col].iloc[i:i + 4].min()) <= 3:
                condition[i:i + 4] = True  # Set the condition to True for these indices

    return condition  # Return the condition Series

def ws_test():
    df = file_read(file_path)
    ws_title = str.strip(col_filter(ws_keys, filter_keys))
    wd_title = str.strip(col_filter(wd_keys, filter_keys))
    
    condition = ws_check(df, ws_title, wd_title)  # Get the condition from ws_check
    
    # Apply cell_color based on the condition
    return df.style.apply(lambda x: cell_color(x, condition, col=ws_title, col2=wd_title), axis=None)

'''Export the QA/QC file'''
def QAQC_file():
    date = datetime.datetime.now()
    styled_df = ws_test()
    styled_df.to_excel(date.strftime("%Y%m%d") + '-' + 'temp.xlsx', index=False)

if __name__ == '__main__':
    while True:
        file_path = input("Enter the path to the 15-min CSV file (or 'exit' to quit): ")

        if file_path == 'exit':
            print("Exiting the program.")
            sys.exit()
        elif file_path.endswith(".csv"):
            break  
        else:
            print("This is not a CSV file. Please try again.")

    QAQC_file()
