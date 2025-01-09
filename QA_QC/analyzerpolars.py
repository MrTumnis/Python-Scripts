#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import sys 
import datetime
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


'''Write a check to see if first row contains a string'''
def file_read(file):
    df = pl.read_csv(file, raise_if_empty=True, try_parse_dates=True)
    indexes_to_drop = [0]  # This is the index list to drop
    df_dropped = df.filter(~pl.Series(range(len(df))).is_in(indexes_to_drop))
    df = df_dropped
    return df


def file_scan(file):
    df = pl.scan_csv(file, skip_rows=0, raise_if_empty=True, try_parse_dates=True)
    df1 = df.collect()
    return df1
    

def scan_test(file):
    df = file_scan(file).select(['TIMESTAMP','STATION' ])
    df1 = file_read(file)
#    df2 = df1.transpose() 
#    ic(df)
    ic(df1)
#    ic(df2)

#def time_check():
 #   df = 

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file = 'Met.csv'
    scan_test(file)
#    file_scan(file)
#    file_read(file)
