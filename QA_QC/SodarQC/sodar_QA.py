#!/.venv/bin/python

import polars as pl
import polars.selectors as cs
import logging
import datetime
import zipfile
import glob
import os
import argparse
import sys

logging.basicConfig(filename = 'log_sodarQC.log',
                    format = '%(asctime)s %(message)s',
                    filemode='a')


schema = {
    'TIMESTAMP':pl.Datetime ,'VectorWindSpeed':pl.Float32, 'VectorWindDirection':pl.Float32, 'SpeedDirectionReliability':pl.Float32,
    'W_Speed':pl.Float32, 'W_Reliability':pl.UInt32, 'W_Count':pl.Float32, 'W_StandardDeviation':pl.Float32,
    'W_Amplitude':pl.Int32, 'W_Noise':pl.Float32, 'W_SNR':pl.Float32, 'W_ValidCount':pl.Float32,
    'V_Speed':pl.Float32, 'V_Reliability':pl.UInt32, 'V_Count':pl.Float32, 'V_StandardDeviation':pl.Float32,
    'V_Amplitude':pl.Int32, 'V_Noise':pl.Float32, 'V_SNR':pl.Float32, 'V_ValidCount':pl.Float32,
    'U_Speed':pl.Float32, 'U_Reliability':pl.UInt32,'U_Count':pl.Float32, 'U_StandardDeviation':pl.Float32,
    'U_Amplitude':pl.Int32, 'U_Noise':pl.Float32, 'U_SNR':pl.Float32, 'U_ValidCount':pl.Float32
}

#for reference
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
def read_file(height=None) -> dict:

    null_items = ['TIMESTAMP', 'm/s', '\u00B0', ""]
    
    #range gates 
    lazy_dict = {'30':'','35':'','40':'','45':'','50':'','55':'',
              '60':'','65':'','70':'','75':'','80':'','85':'',
              '90':'','95':'','100':'','105':'','110':'','115':'',
              '120':'','125':'','130':'','135':'','140':''}
 
    try:
        file = glob.glob(os.path.join('*GPWauna*'))
        file_path = file[0]
        
    except: 
        logging.error(f"No file detected")
        print(f"No file detected")
        sys.exit()

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
                    .to_datetime('%Y-%m-%d %H:%M:%S',time_unit=None, time_zone=None, strict=False)).cast(schema)
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
        logging.error(f"Error occured processing file: {e}")



#Merge dataframes with a single header starting with the 30m file
def lf_merge() -> pl.LazyFrame:
    df_dic = read_file()  
    df_list = []
    try:
        for i in range(35,141,5):
           df_list.append(df_dic[str(i)])

        df = df_dic['30']

        for item in df_list:
            df = df.lazy().join(item.lazy(), on='TIMESTAMP', how='inner') 

        return df
    except Exception as e:
        logging.error(f"Error occured merging lazyframe: {e}")




def speed_profile_check() -> list:
    lf = lf_merge()
    speed_list = []
    try:
        h = 30
        h2 = h + 5
        
        '''perform a difference check on the absolute value of adjacent range gates and return a 9 if pass, and a 2 to flag '''
        while h < 145: 

            w_df = lf.select(pl
                             .when(pl.col(f'W_Speed_{h}').abs() - pl.col(f'W_Speed_{h2}').abs() >= 2)
                             .then(2)
                             .otherwise(9)
                             .alias(f'W_Speed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(w_df)

            u_df = lf.select(pl
                            .when(pl.col(f'U_Speed_{h}').abs() - pl.col(f'U_Speed_{h2}').abs() >= 2)
                            .then(2)
                            .otherwise(9)
                            .alias(f'U_Speed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(u_df)
            
            v_df = lf.select(pl
                           .when(pl.col(f'V_Speed_{h}').abs() - pl.col(f'V_Speed_{h2}').abs() >= 2)
                           .then(2)
                           .otherwise(9)
                           .alias(f'V_Speed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(v_df)

            vec_df = lf.select(pl
                           .when(pl.col(f'VectorWindSpeed_{h}').abs() - pl.col(f'VectorWindSpeed_{h2}').abs() >= 5)
                           .then(2)
                           .otherwise(9)
                           .alias(f'VectorWindSpeed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(vec_df)

            h +=5 

        return speed_list

    except Exception as e:
        logging.error(f"Error performing speed profile check: {e}")
    


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
        logging.error(f"Error performing standard deviation check: {e}")



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

        return noise_list

    except Exception as e:
        logging.error(f"Error performing noise check: {e}")



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
        logging.error(f"Error performing precip check: {e}")


#def echo_check(): can add later, but not recommended for QA/QC
    


#merge all QC columns into one Dataframe and compare the validity of each range gate, then return the lowest number as the valid code. 
def df_merge(args):
    try:
        lf = lf_merge()
        lf1 = pl.concat(speed_profile_check(), how='horizontal', parallel=True)
        lf2 = pl.concat(standard_dev_check(), how='horizontal', parallel=True)
        lf3 = pl.concat(noise_check(), how='horizontal', parallel=True)
        lf4 = pl.concat(echo_check(), how='horizontal', parallel=True)
        lf5 = pl.concat(precip_check(), how='horizontal', parallel=True)
        df1 = pl.concat([lf,lf1,lf2,lf3,lf4,lf5], how='horizontal', parallel=True).collect()
        check_dict = {} 
        df_time = df1.select('TIMESTAMP')

        '''Return the minimum validity code (2 or 9) for each check as the reliabilty for each wind component. Precip_Check flag sets the validity to 3'''
        h = 30
        while h < 141:
            df2 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'W_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'W_Reliability_{h}').cast(pl.UInt32)
            )
            
            df3 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'V_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'V_Reliability_{h}').cast(pl.UInt32)
            )

            df4 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'U_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'U_Reliability_{h}').cast(pl.UInt32)
            )


            wvu_df = pl.concat([df2,df3,df4], how='horizontal')
            time_df = df_time.hstack(wvu_df)
            check_dict.update({h:time_df}) 

            h += 5

        h_list = []
        for h in range(35,141,5):
            h_list.append(check_dict[h])
        
        df5 = check_dict[30]

        for df in h_list: 
            df5 = df5.join(df, on='TIMESTAMP', how='inner', coalesce=False)
        
        # Replace columns in original dataframe with the check values
        lf2 = lf.collect()
        common_columns = set(df5.columns) & set(lf2.columns)
        long_df= lf.with_columns([df5[col].alias(col) for col in common_columns])
       
        fnl_dict = {}
        f = 30
        while f < 141 :
            f2 = f + 100 #quick way to exclude the files in the 100's in the iteration. i.e not returning 140m with the 40m file
            split_df = long_df.select('TIMESTAMP',(cs.ends_with(f'{f}')) & (cs.exclude(cs.ends_with(f'{f2}'))))
            named_df = split_df.rename(lambda column_name:column_name.strip(f'_{f}'))
            if args.dat:
                fnl_df = named_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S')) 
                fnl_dict.update({f:fnl_df}) 
            else:
                fnl_df = named_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S'))
                fnl_dict.update({f:fnl_df}) 

            f += 5


        if args.longform:
            longform = long_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S'))
            return longform

        else:
            return fnl_dict

    except Exception as e:
        logging.error(f"Error in processing final dataframe: {e}")


if __name__ == '__main__': 
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--longform', action='store_true' ,help="Export a file in csv of QC'd files combined")
    parser.add_argument('-d', '--dat', action='store_true' ,help="Export range seperated files as .dat for data upload (default is csv)")
    parser.add_argument('-v', '--version', action='store_true', help="Show the versions of libraries")
    args = parser.parse_args()

    if args.version:
        pl.show_versions()
        sys.exit()

    else:

        date = datetime.datetime.now()

        try:
            merged_df = df_merge(args)

            if args.longform:
                df = merged_df.collect()
                df.write_csv(date.strftime("%Y%m%d") + '-' + 'SODAR_QA-QC_longform' + '.csv', include_header=True)
    
            elif args.dat:
                for h, lf in merged_df.items():
                    df = lf.collect()
                    df.write_csv(f'SODAR{h}' + '.dat', include_header=False, quote_char='"', quote_style='non_numeric')

            else:
                for h, lf in merged_df.items():
                    df = lf.collect()
                    df.write_csv(date.strftime("%Y%m%d") + '-' + f'SODAR{h}_QA-QC' + '.csv', include_header=True)

        except Exception as e:
            logging.error(f"Error writing to csv: {e}")
            print(f"Error writing to csv: {e}")


