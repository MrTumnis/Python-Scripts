#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import pandas as pd
import pyarrow as pa
import sys 
import logging
import datetime
import zipfile
from icecream import ic
from rich import print as print
from rich.console import Console
from rich.theme import Theme
from rich.prompt import Prompt
from rich.traceback import install

install()

logging.basicConfig(filename = 'log_sodarQC.log',
                    format = '%(asctime)s %(message)s',
                    filemode='w')


logger = logging.getLogger(__name__)


schema = {
    'TIMESTAMP':pl.Datetime ,'VectorWindSpeed':pl.Float32, 'VectorWindDirection':pl.Float32, 'SpeedDirectionReliability':pl.Float32,
    'W_Speed':pl.Float32, 'W_Reliability':pl.Float32, 'W_Count':pl.Float32, 'W_StandardDeviation':pl.Float32,
    'W_Amplitude':pl.Float32, 'W_Noise':pl.Float32, 'W_SNR':pl.Float32, 'W_ValidCount':pl.Float32,
    'V_Speed':pl.Float32, 'V_Reliability':pl.Float32, 'V_Count':pl.Float32, 'V_StandardDeviation':pl.Float32,
    'V_Amplitude':pl.Float32, 'V_Noise':pl.Float32, 'V_SNR':pl.Float32, 'V_ValidCount':pl.Float32,
    'U_Speed':pl.Float32, 'U_Reliability':pl.Float32,'U_Count':pl.Float32, 'U_StandardDeviation':pl.Float32,
    'U_Amplitude':pl.Float32, 'U_Noise':pl.Float32, 'U_SNR':pl.Float32, 'U_ValidCount':pl.Float32
}


columns = {
    'TIMESTAMP':'','VectorWindSpeed':'','VectorWindDirection':'','SpeedDirectionReliability':'',
    'W_Speed':'','W_Reliability':'', 'W_Count':'','W_StandardDeviation':'',
    'W_Amplitude':'','W_Noise':'','W_SNR':'','W_ValidCount':'',
    'V_Speed':'','V_Reliability':'','V_Count':'','V_StandardDeviation':'',
    'V_Amplitude':'','V_Noise':'','V_SNR':'','V_ValidCount':'',
    'U_Speed':'','U_Reliability':'','U_Count':'','U_StandardDeviation':'',
    'U_Amplitude':'','U_Noise':'','U_SNR':'','U_ValidCount':''
}


#Return all lazy files in a dictionary for easy reference and append height of each range gate to column name 
def read_file(file_path, height=None) -> dict:

    null_items = ['TIMESTAMP', 'm/s', '\u00B0', ""]
    
    #range gates
    lazy_dict = {'30':'','35':'','40':'','45':'','50':'','55':'',
              '60':'','65':'','70':'','75':'','80':'','85':'',
              '90':'','95':'','100':'','105':'','110':'','115':'',
              '120':'','125':'','130':'','135':'','140':''}

    try:
        if file_path.endswith('.zip'):
            with zipfile.ZipFile(f'{file_path}', 'r') as zip_file:
                file_path = file_path.strip('.zip')
                zip_file.extractall(f'{file_path}')


    except Exception as e:
        logging.error(f"Error occured processing zip file {e}")
        raise

    try:
        for h in lazy_dict.keys():
            file = (
                pl
                .scan_csv(f'{file_path}/Wauna_SODAR{h}_Table15.csv', has_header=True, null_values=null_items, raise_if_empty=True)
                .with_columns(
                    pl
                    .col('TIMESTAMP').str
                    .to_datetime('%Y-%m-%d %H:%M:%S', strict=False)).cast(schema)
            )
            lazy_dict.update({h:file})

        #Append the file height to the column names
        for key, value in lazy_dict.items():
            lf = lazy_dict[key]
            lf1 = lf.rename(lambda column_name:column_name[0:] + f'_{key}')
            #must 'collect' dataframe before returning the lazyframe or the ragne gates will not append to column names
            df = lf1.rename({f'TIMESTAMP_{key}':'TIMESTAMP'}).collect() 
            lf = df.lazy()
            lazy_dict.update({key:lf})

        #Return a single lazy frame based on height of data recordings 
        if height is not None:
            lf = lazy_dict[height]
            return lf 

        #Return all files in a dictionary
        else:
            return lazy_dict

    except Exception as e:
        logging.error(f"Error occured processing file {e}")



#Merge dataframes with a single header starting with the 30m file
def lf_merge() -> pl.LazyFrame:
    df_dic = read_file(file_path)  
    df_list = []

    for i in range(35,141,5):
       df_list.append(df_dic[str(i)])

    df = df_dic['30']

    for item in df_list:
        df = df.lazy().join(item.lazy(), on='TIMESTAMP', how='inner') 

    return df




#return adjacent columns as lazyframes, or a dataframe of 3 columns in the same range gate
def column_filter(col=None, col_series=None):
    lf = lf_merge() 
#    try:
    if col is not None:
        adj_dict = {r:r+5 for r in range(35,136,5)}
        df_pairs = []
        
        #create 2 column dataframes based on adjacent range gates
        for key, val in adj_dict.items():     
            ver_ws = lf.select(cs.by_name(f'{col}_{key}',f'{col}_{val}'))
            df_pairs.append(ver_ws)
            ic('Test')

        return df_pairs
                 

    '''Used for testing. May need to move this inside of std dev check function'''
    if col_series is not None:
        series = [] 
        i = 30
        while i < 145:
            ic(i)
            lf = lf_merge() 
            height.append(i)
            lf = lf.select(cs.by_name(f'W_StandardDeviation_{i}', f'U_StandardDeviation_{i}', f'V_StandardDeviation_{i}'))
            series.append(lf)
            i+=5

    return None


def speed_profile_check(col_list, diff) -> list:

    lf_list = []

    try:  
        i = 0 
        while i < len(col_list):
            for col in col_list:
                col_letter = col.strip('_Speed')

                lf = column_filter(col)
                for item in lf:
                    df = item 
                    #return names as a list while maintaining lazyframe
                    lf = df.collect_schema().names()
                    col1 = lf[0]
                    col2 = lf[1]

                    h = col1.strip(f'{col_letter}Speed_')
                    
                    '''perform a difference check on the adjacent range gates and return a 9 if pass, and a 2 to flag '''
                    df_col= df.with_columns(
                        (pl.when(
                                pl.col(col1).abs() - 
                                pl.col(col2).abs() >= (diff))
                            .then(2)
                            .otherwise(9)
                        ).alias(f'{col_letter}_Reliability_{h}')
                        .cast(pl.UInt32)
                    )
                    
                    df = df_col
                    lf_list.append(df) 
            i+=1

            return lf_list 

    except Exception as e:
         logging.error(f"Error performing speed_profile_check {e}")


def standard_dev_check():
    lf = lf_merge()
    lf_col = lf.collect_schema().names()
    lf_list = []
    h = 30
    while h < 145:          
        condition = (
               pl.col(f'W_StandardDeviation_{h}') > 1) | (
                   pl.col(f'U_StandardDeviation_{h}') + pl.col(f'W_StandardDeviation_{h}') > 5) | (
                   pl.col(f'V_StandardDeviation_{h}') / pl.col(f'U_StandardDeviation_{h}') > 5)
            
        df = lf.select(
                    pl.when(condition)
                    .then(2)
                    .otherwise(9)
                    .alias(f'std_reliability_{h}')
                     .cast(pl.UInt32)
                )
        
        h += 5

        lf_list.append(df)
    ic(lf_list)
    return lf_list 

        # except Exception as e:
        #     logging.error(f"Error performing standard deviation check {e}")

def df_concat() -> pl.DataFrame:
#    vec_list = ['VectorWindSpeed']
#    com_list = ['W_Speed', 'U_Speed', 'V_Speed']
#    vector_df = speed_profile_check(vec_list, diff = 5)
#    com_df = speed_profile_check(vec_list, diff = 2)
    
    std_df = standard_dev_check()
    ic(std_df)
    return None 

def QAQC_file():
#    try:
    date = datetime.datetime.now()
    styled_df = df_concat()
    styled_df.write_csv(date.strftime("%Y%m%d") + '-' + 'SODAR_QA-QC' + '.csv', include_header=True)
   # styled_df.write_excel(date.strftime("%Y%m%d") + '-' + 'SODAR_df' + '.xlsx')
 #   except Exception as e:
 #       logging.error(f"Error writing to csv {e}")

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file_path = 'GPWauna_data.zip'
    QAQC_file()
#    df_concat()
