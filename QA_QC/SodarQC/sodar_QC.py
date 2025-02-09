#!/.venv/bin/python
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "polars",
#     "simple-term-menu",
# ]
# ///

import polars as pl
import polars.selectors as cs
import logging
import datetime
import zipfile
import glob
import os
import re
import argparse
import sys
from simple_term_menu import TerminalMenu


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
        # file_list = glob.glob(os.path.join('*GPWauna*'))
        # files_menu = TerminalMenu(file_list, title=(f'Choose the path of the Sodar files. (can be a .zip file)'))
        # selection = files_menu.show()
        # file_path = file_list[selection]
        file_path = 'GPWauna_data.zip'

        if file_path.endswith('.zip'):
            with zipfile.ZipFile(f'{file_path}', 'r') as zip_file:
                file_path = file_path.strip('.zip')
                zip_file.extractall(f'{file_path}')


        for h in lazy_dict.keys():
            file = (pl
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
            #must 'collect' dataframe before returning the lazyframe or the range gates will not append to column names
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
        print({e})



#Merge dataframes with a single header starting with the 30m file
def lf_merge(df) -> pl.LazyFrame:
    df_dic = df 
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




def speed_profile_check(lf) -> list:
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

            v_df = lf.select(pl
                           .when(pl.col(f'V_Speed_{h}').abs() - pl.col(f'V_Speed_{h2}').abs() >= 2)
                           .then(2)
                           .otherwise(9)
                           .alias(f'V_Speed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(v_df)

            u_df = lf.select(pl
                            .when(pl.col(f'U_Speed_{h}').abs() - pl.col(f'U_Speed_{h2}').abs() >= 2)
                            .then(2)
                            .otherwise(9)
                            .alias(f'U_Speed_Check_{h}')).cast(pl.UInt32)
            speed_list.append(u_df)
            
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
    


def standard_dev_check(lf) -> list:
    lf_list = []

    try:
        h = 30
        while h < 145:          
            condition = (
                       pl.col(f'W_StandardDeviation_{h}') > 1) | (
                       pl.col(f'U_StandardDeviation_{h}') + pl.col(f'W_StandardDeviation_{h}') > 5) | (
                       pl.col(f'V_StandardDeviation_{h}') / pl.col(f'U_StandardDeviation_{h}') > 5)
                
            df = lf.select(pl
                        .when(condition)
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



def noise_check(lf) -> list:
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



def echo_check(lf) -> list:
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



def precip_check(lf):
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
    


def df_merge(lf, args):
    try:
        lf1 = pl.concat(speed_profile_check(lf), how='horizontal', parallel=True)
        lf2 = pl.concat(standard_dev_check(lf), how='horizontal', parallel=True)
        lf3 = pl.concat(noise_check(lf), how='horizontal', parallel=True)
        lf4 = pl.concat(echo_check(lf), how='horizontal', parallel=True)
        lf5 = pl.concat(precip_check(lf), how='horizontal', parallel=True)
        df1 = pl.concat([lf,lf1,lf2,lf3,lf4,lf5], how='horizontal', parallel=True).collect()
        qa_file = pl.concat([lf1,lf2,lf3,lf4,lf5],how='horizontal',parallel=True).collect()
        check_dict = {} 
        df_time = df1.select('TIMESTAMP')

        # if args.qafile:
        #     df = qa_file 
        # else:
        #     print("remove me later, and asign df1")
        #     df = qa_file 

        '''Return the minimum validity code (0, 2, or 9) for each check as the reliabilty for each wind component. Precip_Check flag sets the validity to 3. *will likely need to seperate the "0" checks for each range gate'''
        h = 30
        while h < 141:
            df2 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'W_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'W_Zero{h}').cast(pl.UInt32)
            )
            df3 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'V_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'V_Zero{h}').cast(pl.UInt32)
            )
            df4 = df1.select(pl
                .when(pl.col(f'Precip_Check_{h}') < 4)
                .then(3)
                .otherwise(pl
                .min_horizontal([(f'U_Speed_Check_{h}'),(f'STD_Reliability_{h}'),(f'Echo_Check_{h}'),(f'Noise_Check_{h}')]))
                .alias(f'U_Zero{h}').cast(pl.UInt32)
            )
            zero_check = pl.concat([df1,df2,df3,df4], how='horizontal')

            w_df = zero_check.select(pl
                .when(pl.col(f'W_Reliability_{h}') == 0)
                .then(0)
                .otherwise(pl.col(f'W_Zero{h}'))
                .alias(f'W_Reliability_{h}').cast(pl.UInt32)
            )
            v_df = zero_check.select(pl
                .when(pl.col(f'V_Reliability_{h}') == 0)
                .then(0)
                .otherwise(pl.col(f'V_Zero{h}'))
                .alias(f'V_Reliability_{h}').cast(pl.UInt32)
            )
            u_df = zero_check.select(pl
                .when(pl.col(f'U_Reliability_{h}') == 0)
                .then(0)
                .otherwise(pl.col(f'U_Zero{h}'))
                .alias(f'U_Reliability_{h}').cast(pl.UInt32)
            )
            wvu_df = pl.concat([w_df,v_df,u_df], how='horizontal')
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
        long_df = lf.with_columns([df5[col].alias(col) for col in common_columns])
       
        if args.qafile | args.transpose:
            common_col = set(df5.columns) & set(qa_file.columns)
            qa_df = qa_file.with_columns([df5[col].alias(col) for col in common_columns])
            qc_dict = {}
            t = 30
            while t < 141:
                t2 = t + 100 
                sort = qa_df.select('TIMESTAMP', (cs.ends_with(f'{t}')) & (cs.exclude(cs.ends_with(f'{t2}'))))
                qc_dict.update({t:sort})
                t += 5

            df2 = qc_dict[30]
            i = 35
            while i < 145:
                df2 = df2.join(qc_dict[i],on='TIMESTAMP', how='inner')
                i+=5

            qc_df = df2.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S'))

            return qc_df 


        elif args.longform:
            longform = long_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S'))
            return longform

        else:
            fnl_dict = {}
            f = 30
            while f < 141 :
                f2 = f + 100 #quick way to exclude the files in the 100's from the iteration. i.e not returning 140m with the 40m file
                split_df = long_df.select('TIMESTAMP',(cs.ends_with(f'_{f}')) & (cs.exclude(cs.ends_with(f'_{f2}'))))
                
                named_df = split_df.rename(lambda column_name:column_name.strip(f'_{f}'))
                fnl_df = named_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d %H:%M:%S')).collect()
                fnl_dict.update({f:fnl_df}) 

                f += 5

            return fnl_dict

    except Exception as e:
       logging.error(f"Error in processing final dataframe: {e}")
       print({e})


if __name__ == '__main__': 
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dat', action='store_true' ,help="Export range seperated files as .dat for data upload (default is csv)")
    parser.add_argument('-l', '--longform', action='store_true' ,help="Export a file in csv of QC'd files combined")
    parser.add_argument('-q', '--qafile', action='store_true', help="output csv with all checks for comparison")
    parser.add_argument('-t', '--transpose', action='store_true', help="View the checks in csv horizontally")
    parser.add_argument('-v', '--version', action='store_true', help="Show the versions of libraries")
    args = parser.parse_args()

    #global dataframes 
    df = read_file()
    lf = lf_merge(df)

    if args.version:
        pl.show_versions()
        sys.exit()

    else:

        try:
            #likely an easier and better way to get start and end time of file range 
            date_df = lf 
            date = date_df.with_columns(pl.col('TIMESTAMP').dt.strftime('%Y-%m-%d')).collect()
            date_start = date[1,0]
            date_end = date[-1,0]
                    
        except:
            logging.error("Error collecting start and end dates")
        
        try:
            os.makedirs(f'{date_start}_{date_end}_QA-QC_SODAR', exist_ok=True)
            path = f'./{date_start}_{date_end}_QA-QC_SODAR/'
            merged_df = df_merge(lf, args)
            
            if args.qafile | args.transpose:
                if args.transpose:
                    df = merged_df.transpose(include_header=True)
                    file = str(date_start + '_' + date_end  + '_' + 'SODAR-Checkfile-Transpose' + '.csv')
                    df.write_csv(file=f'{path}/{file}',include_header=False, float_scientific=False, float_precision=2)
                else:
                    df = merged_df
                    file = str(date_start + '_' + date_end  + '_' + 'SODAR_QA-QC_Checkfile' + '.csv')
                    df.write_csv(file=f'{path}/{file}', include_header=True,float_scientific=False, float_precision=2)

            elif args.longform:
                df = merged_df.collect()
                file = str(date_start + '_' + date_end + '_' + 'SODAR-longform' + '.csv' )
                df.write_csv(file=f'{path}/{file}' ,include_header=True, float_scientific=False, float_precision=2)

            elif args.dat:
                for h, df in merged_df.items():
                    file = str(f'SODAR{h}' + '.dat') 
                    df.write_csv(file=f'{path}/{file}', include_header=False, quote_char='"', quote_style='non_numeric',float_precision=2)

            else:
                for h, df in merged_df.items():
                    file = str(date_start + '_' + date_end + '_' + f'SODAR{h}_QA-QC' + '.csv')
                    df.write_csv(file=f'{path}/{file}' ,include_header=True, float_precision=2)

        except Exception as e:
            logging.error(f"Error writing to csv: {e}")
            print(f"Error writing to csv: {e}")


