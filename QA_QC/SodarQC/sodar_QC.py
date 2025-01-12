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


null = ['TIMESTAMP', 'm/s', '\u00B0', ""]

schema = {
    'TIMESTAMP':pl.Datetime ,'VectorWindSpeed':pl.Float32, 'VectorWindDirection':pl.Int32, 'SpeedDirectionReliability':pl.Int32,
    'W_Speed':pl.Float32, 'W_Reliability':pl.Int32, 'W_Count':pl.Int32, 'W_StandardDeviation':pl.Float32,
    'W_Amplitude':pl.Int32, 'W_Noise':pl.Int32, 'W_SNR':pl.Float32, 'W_ValidCount':pl.Int32,
    'V_Speed':pl.Float32, 'V_Reliability':pl.Int32, 'V_Count':pl.Int32, 'V_StandardDeviation':pl.Float32,
    'V_Amplitude':pl.Float32, 'V_Noise':pl.Int32, 'V_SNR':pl.Float32, 'V_ValidCount':pl.Int32,
    'U_Speed':pl.Float32, 'U_Reliability':pl.Int32,'U_Count':pl.Int32, 'U_StandardDeviation':pl.Float32,
    'U_Amplitude':pl.Int32, 'U_Noise':pl.Int32, 'U_SNR':pl.Float32, 'U_ValidCount':pl.Int32
}

columns = {
    'time':'TIMESTAMP', 'vec_ws':'VectorWindSpeed', 'vec_wd':'VectorWindDirection', 'sdir_rel':'SpeedDirectionReliability',
    'w_spd':'W_Speed', 'w_rel':'W_Reliability', 'w_cnt':'W_Count', 'w_std':'W_StandardDeviation',
    'w_amp':'W_Amplitude', 'w_n':'W_Noise', 'w_snr':'W_SNR', 'w_valcnt':'W_ValidCount',
    'v_spd':'V_Speed', 'v_rel':'V_Reliability', 'v_cnt':'V_Count', 'v_snd':'V_StandardDeviation',
    'v_amp':'V_Amplitude', 'v_n':'V_Noise', 'v_snr':'V_SNR', 'v_valcnt':'V_ValidCount',
    'u_spd':'U_Speed', 'u_rel':'U_Reliability', 'u_cnt':'U_Count', 'u_std':'U_StandardDeviation',
    'u_amp':'U_Amplitude', 'u_n':'U_Noise', 'u_snr':'U_SNR', 'u_valcnt':'U_ValidCount'
}


#Return all lazy files in a dictionary for easy reference  
def file_path(lf):
    global null

    lazy_frame = {'30':'','35':'','40':'','45':'','50':'','55':'',
              '60':'','65':'','70':'','75':'','80':'','85':'',
              '90':'','95':'','100':'','105':'','110':'','115':'',
              '120':'','125':'','130':'','135':'','140':''}

    for h in lazy_frame.keys():
        file = (
            pl
            .scan_csv(f'SODAR_data/Wauna_SODAR{h}_Table15.csv', has_header=True, null_values=null, raise_if_empty=True)
            .with_columns(
                pl
                .col('TIMESTAMP').str
                .to_datetime('%Y-%m-%d %H:%M:%S', strict=False))
        )
        lazy_frame.update({h:file})
    ic(lazy_frame[lf].collect())
    return 


# Read the csv files into one dataframe using glob and change the data columns to the proper data types
def read_file_batch():
     file_path = './SODAR_data/Wauna_SODAR*'
     global null
     lf =(
        pl
        .scan_csv(file_path, has_header=True, null_values=null, raise_if_empty=True)
        .with_columns(
            pl
            .col('TIMESTAMP').str
            .to_datetime('%Y-%m-%d %H:%M:%S', strict=False))
        .cast(schema, strict=True)
    )

    # Execute the query and collect the results
     df = lf.collect()
     ic(df)   
     return df

#return full dataframe with proper schema
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


def component_speed_profile_check():
    lf = file_path() 
    ic(type(lf))
    

    lf = lf.with_columns(pl.col('W_Speed')
                                 )

    lf = lf.collect() 
    ic(lf)
     

    return lf


def test():
    file_path('80')
  #  df = component_speed_profile_check()
  #  df = read_file_batch()
#    df1 = pd.read_csv(file, dtype_backend='pyarrow')
 #   rprint("time",df)
    return None 
    

def QAQC_file():
 #   try:
    date = datetime.datetime.now()
    styled_df = test()
    ic(styled_df)
    styled_df.write_csv(date.strftime("%Y%m%d %H%m%s") + '-' + 'SODAR_QA-QC' + '.csv', include_header=True)
   # styled_df.write_excel(date.strftime("%Y%m%d") + '-' + 'SODAR_df' + '.xlsx')

#    except Exception as e:
#        console.print(f"#1 Error occurred: {e}", style='error')
#    return None

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    QAQC_file()
#    file_scan(file)
#    file_read(file)
