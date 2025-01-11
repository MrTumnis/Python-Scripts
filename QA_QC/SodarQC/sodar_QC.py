#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import pandas as pd
import pyarrow as pa
import sys 
import datetime
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

schema = {
    'VectorWindSpeed':'Float32', 'VectorWindDirection':'Int32', 'SpeedDirectionReliability':'Int32',
    'W_Speed':'Float32', 'W_Reliability':'Int32', 'W_Count':'Int32', 'W_StandardDeviation':'Float32', 'W_Amplitude':'Int32', 'W_Noise':'Int32', 'W_SNR':'Float32', 'W_ValidCount':'Int32',
    'V_Speed':'Float32', 'V_Reliability':'Int32', 'V_Count':'Int32', 'V_StandardDeviation':'Float32', 'V_Amplitude':'Int32', 'V_Noise':'Int32', 'V_SNR':'Float32', 'V_ValidCount':'Int32',
    'U_Speed':'Float3', 'U_Reliability':'Int32','U_Count':'Int32', 'U_StandardDeviation':'Float32', 'U_Amplitude':'Int32', 'U_Noise':'Int32', 'U_SNR':'Float32', 'U_ValidCount':'Int32'
}


def read_file(file_path):
#    try:
       # file_lf = pl.scan_csv(file_path, has_header=True,try_parse_dates=True, skip_rows_after_header=1).collect()
        #ic(file_lf)
    df = pl.read_csv(file_path)
    # Drop first row due to string type 
    index = [0]
    df = df.filter(~pl.Series(range(len(df))).is_in(index))
#    df = df.select(
#        pl.col('TIMESTAMP').str
#        .extract("(....-..-.. ..:..:..)", 1)
#        .alias("TIMESTAMP2")
#        .str.to_datetime("%Y-%m-%d %H:%M:%S")
#    )
    df = df.with_columns(
        pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%d %H:%M:%S",strict=True)
    )
    df.cast(schema, strict=False)
    ic('new',df)

    return df 

#    except Exception as e:
#        console.print(f"#1 Error occurred: {e}", style='error')
#    return None

#return the proper interval and check for missing data
def time_check():
    df = read_file(file)
    ic(type(df))
    #    df = pl.DataFrame(df).lazy()
    df = df.upsample(time='TIMESTAMP', every='15m')
    df = pl.from_pandas(df).lazy()
    df = df.fill_null(9999)
    null = df.select(pl.col('TIMESTAMP').null_count())
    null_pos = df.filter(pl.col('TIMESTAMP').is_null()).collect()
    ic(null)
    ic(null_pos)
    

    return df



def test():
    read_file(file)
  #  df = time_check()
#    df1 = pd.read_csv(file, dtype_backend='pyarrow')
 #   rprint("time",df)
    return None
    

def QAQC_file(file):
 #   try:
    date = datetime.datetime.now()
    styled_df = test()
    ic(styled_df)
    styled_df.write_excel(date.strftime("%Y%m%d") + '-' + f"{file.strip('.csv')}" + '.xlsx')

#    except Exception as e:
#        console.print(f"#1 Error occurred: {e}", style='error')
#    return None

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file = 'SODAR.csv'
    QAQC_file(file)
#    file_scan(file)
#    file_read(file)
