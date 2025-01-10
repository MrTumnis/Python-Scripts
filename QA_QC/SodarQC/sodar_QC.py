#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import pandas as pd
import pyarrow as pa
import sys 
import datetime
import timechange
from xlsxwriter import Workbook
from icecream import ic
from rich import print as rprint
from rich.console import Console
from rich.theme import Theme
from rich.prompt import Prompt
from rich.traceback import install

install()

custom_theme = Theme({
    "info": "dim cyan",
    "success" : "dodger_blue2",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

def pandas_read(file_path):
    try:
        df = pd.read_csv(file_path)#, dtype_backend='pyarrow')
        for row in df.head(n=1).itertuples():
            if any(isinstance(item, float) for item in row):
                df = df.drop(0)
        for col in df.columns:
            if col == 'STATION':
                df = df.drop('STATION', axis=1)
        df1 = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        df1 = df1.to_frame()
        df = df.iloc[:,1:].astype(float)
        df = pd.concat([df1, df], axis=1)
        df.columns = df.columns.str.strip()
       
        return df 

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None

def time_change():
    df = pandas_read(file)
    df = (
        pl.from_pandas(df)
    )
    ic(type(df))

    #chagned time freq based on full dataframe, but it only kept the corresponding values in each row. 
    df = (df.upsample(time_column='TIMESTAMP', every='1h')
    .select(pl.all()
    .forward_fill())
    )

       
    ic("Time", df)

def time_check():
    df = pandas_read(file)
    #    df = pl.DataFrame(df).lazy()
    df = (
        pl.from_pandas(df).lazy()
    )

    ic(type(df))
    
    df1 = (
            df.group_by('TIMESTAMP', maintain_order = True)
            .min()
            .collect()
    )
    ic(type(df1))

    return df1

def test():
 #   df = time_check()
    df1 = time_change()
#    df1 = pd.read_csv(file, dtype_backend='pyarrow')
    rprint(df1)

def QAQC_file(file):
    try:
        date = datetime.datetime.now()
        styled_df = test()
        ic(styled_df)
        styled_df.write_excel(date.strftime("%Y%m%d") + '-' + f"{file.strip('.csv')}" + '.xlsx')

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file = 'Met.csv'
    QAQC_file(file)
#    file_scan(file)
#    file_read(file)
