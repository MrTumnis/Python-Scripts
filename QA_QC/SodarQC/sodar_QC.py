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
    'TIMESTAMP':pl.Datetime ,'VectorWindSpeed':pl.Float32, 'VectorWindDirection':pl.Int32, 'SpeedDirectionReliability':pl.Int32,
    'W_Speed':pl.Float32, 'W_Reliability':pl.Int32, 'W_Count':pl.Int32, 'W_StandardDeviation':pl.Float32,
    'W_Amplitude':pl.Int32, 'W_Noise':pl.Int32, 'W_SNR':pl.Float32, 'W_ValidCount':pl.Int32,
    'V_Speed':pl.Float32, 'V_Reliability':pl.Int32, 'V_Count':pl.Int32, 'V_StandardDeviation':pl.Float32,
    'V_Amplitude':pl.Float32, 'V_Noise':pl.Int32, 'V_SNR':pl.Float32, 'V_ValidCount':pl.Int32,
    'U_Speed':pl.Float32, 'U_Reliability':pl.Int32,'U_Count':pl.Int32, 'U_StandardDeviation':pl.Float32,
    'U_Amplitude':pl.Int32, 'U_Noise':pl.Int32, 'U_SNR':pl.Float32, 'U_ValidCount':pl.Int32
}

# Read the csv file and change the data columns to the proper data types
def read_file_batch(file_path):
     null = ['TIMESTAMP', 'm/s', '\u00B0', ""]
     lf =(
        pl
        .scan_csv(file_path, has_header=True, null_values=null, raise_if_empty=True)
        .with_columns(pl.col('TIMESTAMP').str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False))
        .cast(schema, strict=True)
   #     .select(['VectorWindSpeed', 'VectorWindDirection'])
    #    .sort('TIMESTAMP', descending=True)
     #   .limit(10)
    )

    # Execute the query and collect the results
     final_df = lf.collect()
     ic(final_df)   
     return final_df


def read_file(file_path):
    try:
        df = pl.read_csv(file_path, infer_schema=False)
        # Drop first row due to string type 
        index = [0]
        df = df.filter(~pl.Series(range(len(df))).is_in(index))
    # Needed as polars will not recognize datetime and will fill with 'null'
        df = df.with_columns(
            pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%d %H:%M:%S",strict=True)
       ).cast(schema, strict=False)

        return df 

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None

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
    df = read_file_batch(file)
  #  df = time_check()
#    df1 = pd.read_csv(file, dtype_backend='pyarrow')
 #   rprint("time",df)
    return df 
    

def QAQC_file(file):
 #   try:
    date = datetime.datetime.now()
    styled_df = test()
    ic(styled_df)
   # styled_df.write_excel(date.strftime("%Y%m%d") + '-' + f"{file.strip('.csv')}" + '.xlsx')
    styled_df.write_excel(date.strftime("%Y%m%d") + '-' + 'SODAR_df' + '.xlsx')

#    except Exception as e:
#        console.print(f"#1 Error occurred: {e}", style='error')
#    return None

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file = 'SODAR_data/Wauna_SODAR*'
    QAQC_file(file)
#    file_scan(file)
#    file_read(file)
