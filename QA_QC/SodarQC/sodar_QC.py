#!/{HOME}/{USER}/myenv/bin/python

import polars as pl
import polars.selectors as cs
import logging
import datetime
import zipfile
from icecream import ic
from rich import print as print
from rich.traceback import install

install()

logging.basicConfig(filename = 'log_sodarQC.log',
                    format = '%(asctime)s %(message)s',
                    filemode='w')


logger = logging.getLogger(__name__)


schema = {
    'TIMESTAMP':pl.Datetime ,'VectorWindSpeed':pl.Float32, 'VectorWindDirection':pl.Float32, 'SpeedDirectionReliability':pl.Float32,
    'W_Speed':pl.Float32, 'W_Reliability':pl.Float32, 'W_Count':pl.Float32, 'W_StandardDeviation':pl.Float32,
    'W_Amplitude':pl.Int32, 'W_Noise':pl.Float32, 'W_SNR':pl.Float32, 'W_ValidCount':pl.Float32,
    'V_Speed':pl.Float32, 'V_Reliability':pl.Float32, 'V_Count':pl.Float32, 'V_StandardDeviation':pl.Float32,
    'V_Amplitude':pl.Int32, 'V_Noise':pl.Float32, 'V_SNR':pl.Float32, 'V_ValidCount':pl.Float32,
    'U_Speed':pl.Float32, 'U_Reliability':pl.Float32,'U_Count':pl.Float32, 'U_StandardDeviation':pl.Float32,
    'U_Amplitude':pl.Int32, 'U_Noise':pl.Float32, 'U_SNR':pl.Float32, 'U_ValidCount':pl.Float32
}


columns = {
    'TIMESTAMP','VectorWindSpeed','VectorWindDirection','SpeedDirectionReliability',
    'W_Speed','W_Reliability', 'W_Count','W_StandardDeviation',
    'W_Amplitude','W_Noise','W_SNR','W_ValidCount',
    'V_Speed','V_Reliability','V_Count','V_StandardDeviation',
    'V_Amplitude','V_Noise','V_SNR','V_ValidCount',
    'U_Speed','U_Reliability','U_Count','U_StandardDeviation',
    'U_Amplitude','U_Noise','U_SNR','U_ValidCount'
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
                .with_columns(pl
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

        #Return a lazy frame based on height of data recordings 
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




def speed_profile_check() -> list:
    lf = lf_merge()
    speed_list = []
    h = 30
    h2 = h + 5
    
    '''perform a difference check on the adjacent range gates and return a 9 if pass, and a 2 to flag '''
    while h < 145: 
        w_df = lf.select(pl
                         .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & (pl.col(f'W_Speed_{h}') - pl.col(f'W_Speed_{h2}').abs() >= 2))
                         .then(2)
                         .otherwise(9)
                         .alias(f'W_Speed_Check_{h}')).cast(pl.UInt32)
        
        speed_list.append(w_df)
        u_df = lf.select(pl
                        .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & (pl.col(f'U_Speed_{h}') - pl.col(f'U_Speed_{h2}').abs() >= 2))
                        .then(2)
                        .otherwise(9)
                        .alias(f'U_Speed_Check_{h}')).cast(pl.UInt32)

        speed_list.append(u_df)
        v_df = lf.select(pl
                       .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & (pl.col(f'V_Speed_{h}') - pl.col(f'V_Speed_{h2}').abs() >= 2))
                       .then(2)
                       .otherwise(9)
                       .alias(f'V_Speed_Check_{h}')).cast(pl.UInt32)
        
        speed_list.append(v_df)
        vec_df = lf.select(pl
                       .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & (pl.col(f'VectorWindSpeed_{h}') - pl.col(f'VectorWindSpeed_{h2}').abs() >= 5))
                       .then(2)
                       .otherwise(9)
                       .alias(f'VectorWindSpeed_Check_{h}')).cast(pl.UInt32)
        
        speed_list.append(vec_df)
        h +=5 
    return speed_list
    

def standard_dev_check() -> list:
    lf = lf_merge()
    lf_list = []
    try:
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
                        .alias(f'STD_Reliability_{h}')
                         .cast(pl.UInt32)
                    )
            
            h += 5
            lf_list.append(df)

        return lf_list 

    except Exception as e:
        logging.error(f"Error performing standard deviation check {e}")

def noise_check() -> list:
    lf = lf_merge()
    noise_list = []
    try:
        h = 30
        h2 = h + 5
        while h < 145: 
            con_list = [(pl.col(f'W_Amplitude_{h}') < pl.col(f'W_Amplitude_{h2}')) & (
                    pl.col(f'VectorWindSpeed_{h}') > pl.col(f'VectorWindSpeed_{h2}')), 
                           (pl.col(f'U_Amplitude_{h}') < pl.col(f'U_Amplitude_{h2}')) & (
                    pl.col(f'VectorWindSpeed_{h}') > pl.col(f'VectorWindSpeed_{h2}')), 
                           (pl.col(f'V_Amplitude_{h}') < pl.col(f'V_Amplitude_{h2}')) & (
                    pl.col(f'VectorWindSpeed_{h}') > pl.col(f'VectorWindSpeed_{h2}'))]

            for condition in con_list:        
                dr = lf.select(pl
                               .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & condition)
                               .then(2)
                               .otherwise(9)
                               .alias(f'Noise_Check_{h}')
                               ).cast(pl.UInt32)

            noise_list.append(dr)
            h +=5 
           # lf = pl.join(df)
        return noise_list

    except Exception as e:
        logging.error(f"Error performing noise check {e}")

def echo_check() -> list:
    lf = lf_merge()
    echo_list = []
    try:
        h = 30
        h2 = h + 5
        while h < 145: 
            con_list = [(pl.col(f'W_Amplitude_{h}') < pl.col(f'W_Amplitude_{h2}')) & (
                    pl.col(f'W_Speed_{h}') > pl.col(f'W_Speed_{h2}')), 
                           (pl.col(f'U_Amplitude_{h}') < pl.col(f'U_Amplitude_{h2}')) & (
                    pl.col(f'U_Speed_{h}') > pl.col(f'U_Speed_{h2}')), 
                           (pl.col(f'V_Amplitude_{h}') < pl.col(f'V_Amplitude_{h2}')) & (
                    pl.col(f'V_Speed_{h}') > pl.col(f'V_Speed_{h2}'))]

            for condition in con_list:        
                df = lf.select(pl
                               .when(pl.col('TIMESTAMP').dt.hour().is_between(10,17) & condition)
                               .then(2)
                               .otherwise(9)
                               .alias(f'Echo_Check_{h}')
                               ).cast(pl.UInt32)

            echo_list.append(df)
            h +=5 

        return echo_list 

    except Exception as e:
        logging.error(f"Error performing echo check {e}")

def precip_check():
    lf = lf_merge()
    precip_list = []
    try:
        h = 30
        while h < 145:
            df = lf.select(pl
                           .when(pl.col(f'W_Speed_{h}') < -3)
                           .then(3)
                           .otherwise(9)
                           .alias(f'Precip_Check_{h}')
                           ).cast(pl.UInt32)
            h += 5
            precip_list.append(df)

        return precip_list 
    except Exception as e:
        logging.error(f"Error performing precip check {e}")


#def echo_check(): could add later, but not recommended for QA/QC
    


#merge all QC columns into one Dataframe and compare the validity of each range gate, then return the lowest number as the valid code. 
def df_merge() -> pl.DataFrame:
    lf = lf_merge()
    lf1 = pl.concat(speed_profile_check(), how='horizontal', parallel=True)
    lf2 = pl.concat(standard_dev_check(), how='horizontal', parallel=True)
    lf3 = pl.concat(noise_check(), how='horizontal', parallel=True)
    lf4 = pl.concat(echo_check(), how='horizontal', parallel=True)
    lf5 = pl.concat(precip_check(), how='horizontal', parallel=True)
    df1 = pl.concat([lf,lf1,lf2,lf3,lf4,lf5], how='horizontal', parallel=True)#.collect()

    '''Return the minimum validity code for each check as the overall reliabilty. Precip_Check is left as a seperate validity column'''
    h = 30
    while h < 145:
        w_df = df1.select(pl
            .when(pl.col(f'Precip_Check_{h}') < 4)
            .then(3)
            .otherwise(pl
            .min_horizontal([(f'W_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
            .alias(f'W_Reliability_{h}')
        )

        
        v_df = df1.select(
            pl.when(pl.col(f'Precip_Check_{h}') < 4)
            .then(3)
            .otherwise(
                pl.min_horizontal([(f'V_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
            .alias(f'V_Reliability_{h}')
        )

        u_df = df1.select(
            pl.when(pl.col(f'Precip_Check_{h}') < 4)
            .then(3)
            .otherwise(
                pl.min_horizontal([(f'U_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
            .alias(f'U_Reliability_{h}')
        )

        h += 5

        print(w_df.collect())
   # return w_df.collect() 

def QAQC_file():
#    try:
    date = datetime.datetime.now()
    merged_df = df_merge()
    merged_df.write_csv(date.strftime("%Y%m%d") + '-' + 'SODAR_QA-QC' + '.csv', include_header=True)
   # styled_df.write_excel(date.strftime("%Y%m%d") + '-' + 'SODAR_df' + '.xlsx')
 #   except Exception as e:
 #       logging.error(f"Error writing to csv {e}")

if __name__ == '__main__': 
    
#file = input("What is the path to the .csv file ")
    file_path = 'GPWauna_data.zip'
    QAQC_file()
#    df_concat()
