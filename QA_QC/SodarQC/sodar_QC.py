#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import pandas as pd
import pyarrow as pa
import sys 
import datetime
from icecream import ic
from rich import print as print
from rich.console import Console
from rich.theme import Theme
from rich.prompt import Prompt
from rich.traceback import install

install()


null_items = ['TIMESTAMP', 'm/s', '\u00B0', ""]

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
    'TIMESTAMP':'','VectorWindSpeed':'','VectorWindDirection':'','SpeedDirectionReliability':'',
    'W_Speed':'','W_Reliability':'', 'W_Count':'','W_StandardDeviation':'',
    'W_Amplitude':'','W_Noise':'','W_SNR':'','W_ValidCount':'',
    'V_Speed':'','V_Reliability':'','V_Count':'','V_StandardDeviation':'',
    'V_Amplitude':'','V_Noise':'','V_SNR':'','V_ValidCount':'',
    'U_Speed':'','U_Reliability':'','U_Count':'','U_StandardDeviation':'',
    'U_Amplitude':'','U_Noise':'','U_SNR':'','U_ValidCount':''
}


#Return all lazy files in a dictionary for easy reference  
def read_file(height=None):
    global null_items

    lazy_dic = {'30':'','35':'','40':'','45':'','50':'','55':'',
              '60':'','65':'','70':'','75':'','80':'','85':'',
              '90':'','95':'','100':'','105':'','110':'','115':'',
              '120':'','125':'','130':'','135':'','140':''}

    for h in lazy_dic.keys():
        file = (
            pl
            .scan_csv(f'SODAR_data/Wauna_SODAR{h}_Table15.csv', has_header=True, null_values=null_items, raise_if_empty=True)
            .with_columns(
                pl
                .col('TIMESTAMP').str
                .to_datetime('%Y-%m-%d %H:%M:%S', strict=False))
        )
        lazy_dic.update({h:file})

    '''Map the file height to the column names'''
    for key, value in lazy_dic.items():
        lf = lazy_dic[key]
        lf1 = lf.rename(lambda column_name:column_name[0:] + '_' + f'{key}')
        lf = lf1.rename({f'TIMESTAMP_{key}':'TIMESTAMP'}).collect()
        lazy_dic.update({key:lf})

    #Return a single lazy frame based on height of data recordings 
    if height is not None:
        lf = lazy_dic[height]
        return lf 

    #Return all files in a dictionary
    else:
        return lazy_dic

'''Merge dataframes with a single header'''
def lf_merge():
    df_dic = read_file()  
    df_list = []
    for i in range(35,141,5):
       df_list.append(df_dic[str(i)])
    df = df_dic['30']
    i = 35
    while i < 140:
        for item in df_list:
            df = df.join(item, on='TIMESTAMP', how='inner') 
            i+=5
    return df



'''Begin Quality Checks'''
#compare vertical('W') and horizontal('U/V')  wind speed at adjacent levels 
def component_speed_profile_check():
    lf = lf_merge().lazy() 
    
    for r in range(35,136,5):
        adj_dic = {} 
        adj_dic.update({r:r+5})
        ic(adj_dic.items())
    i = 30
    while i < 140:
        for key,items in adj_dic.items():     
            ic(key)
            for value in adj_dic.values():
                ic(value)
                df_pairs = []
                ver_ws = lf.select(cs.by_name(f'W_Speed_{key}',f'W_Speed_{value}'))
                df_pairs.append(ver_ws)
                i+=5
     #   for l in df_pairs:
      #      ic(l.collect())
    
      #  ver_ws = lf.select(cs.by_name(f'W_Speed_{key}',f'W_Speed_{value}'))
    #$ver_ws = lf.select(cs.matches('^W_Sp.{2}'))
    ic(ver_ws.collect())
    return lf





#    lf_list = []
# for key, value in lf_dic.items():
#    for i in range(30,141,5):      
#        lf_list.append(f'lf_{i}')    
#    for key, value in lf_dic.items():
#        for idx, i in enumerate(lf_list):
#            ic(idx,i)
#            i = lf_dic[key]



def test():
    df = component_speed_profile_check()
  #  df = read_file_batch()
#    df1 = pd.read_csv(file, dtype_backend='pyarrow')
 #   rprint("time",df)
    return df 
    

def QAQC_file():
 #   try:
    date = datetime.datetime.now()
    styled_df = test()
    styled_df.write_csv(date.strftime("%Y%m%d") + '-' + 'SODAR_QA-QC' + '.csv', include_header=True)
   # styled_df.write_excel(date.strftime("%Y%m%d") + '-' + 'SODAR_df' + '.xlsx')

#    except Exception as e:
#        console.print(f"#1 Error occurred: {e}", style='error')
#    return None

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
     QAQC_file()
#    file_scan(file)
#    file_read(file)
