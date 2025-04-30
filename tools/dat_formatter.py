import polars as pl
from polars.exceptions import ColumnNotFoundError
import os
import sys
import argparse
import glob
import zipfile
from simple_term_menu import TerminalMenu
from datetime import datetime


"""Need to add column for RECORD number if does not exist"""

'''Schema for BAM data'''
schema = {'column_1': pl.String, 'column_2': pl.Float64, 'column_3': pl.Float64, 'column_4': pl.Float64, 'column_5': pl.Float64, 'column_6': pl.Float64, 'column_7': pl.Float64, 'column_8': pl.Float64, 'column_9': pl.Float64, 'column_10': pl.Int8,
          'column_11': pl.Int8, 'column_12': pl.Int8, 'column_13': pl.Int8, 'column_14': pl.Int8, 'column_15': pl.Int8, 'column_16': pl.Int8, 'column_17': pl.Int8, 'column_18': pl.Int8, 'column_19': pl.Int8, 'column_20': pl.Int8, 'column_21': pl.Int8}


def read_file(args):

    file_list = glob.glob(os.path.join('*.*'))
    if file_list == []:
        print('Files not found. Are they in the same directory? ')
        sys.exit()

    files_menu = TerminalMenu(file_list, title=(
        'Choose the file to convert. (can use .zip to unzip the file first)'))
    selection = files_menu.show()
    file_path = file_list[selection]

    if file_path.endswith('.zip'):
        with zipfile.ZipFile(f'{file_path}', 'r') as zip_file:
            file_path = file_path.strip('.zip')
            zip_file.extractall(f'{file_path}')

    df_file = (pl.scan_csv(f'{file_path}', separator=',', has_header=False,
               raise_if_empty=True, infer_schema_length=10000, infer_schema=False))

    col = df_file.collect_schema().names()

    df = df_file.collect()
    date_time = df.select('column_1').row(0)

    datetime_str = date_time[0]
    expected_format = '%Y-%m-%d %H:%M:%S'

    if datetime.strptime(datetime_str, expected_format):
        df_time = df_file

    else:
        df_csv = df_file.with_columns(
            [pl.col(cols).str.strip_chars().alias(cols) for cols in col])

        df_time = df_csv.with_columns(pl.col('column_1')
                                      .str.to_datetime('%m/%d/%y %H:%M', time_unit=None, time_zone=None, strict=False)
                                      .dt.strftime('%Y-%m-%d %H:%M:%S')
                                      .alias('column_1')).cast(schema)

    try:
        pm_name = ['PM10', 'PM2.5', 'PM25',
                   'pm10', 'pm2.5', 'pm25', 'BAM', 'bam']
        if any(name in file_path for name in pm_name):
            df = df_csv.cast(schema)

        df = df_time.collect()
        col1 = col[0]

        # strip file extension from file name
        name, file_n = file_path.rsplit('.', 1)

        if args.rec:
            size = df.select('column_1').count().row(0)

            i = 0
            rec_list = []
            while i < size[0]:
                rec_list.append(i)
                i += 1
            df1 = pl.Series('column_r', rec_list)

            df = df.insert_column(1, df1)

        if args.dat:
            df_fmt = df.select(
                [pl.format('"{}"', pl.col(f'{col1}')).alias('column')])

            df_fnl = pl.concat([df_fmt, df], how='horizontal').drop(col1)

            df_fnl.write_csv(file=f'{name}' + '.dat',
                             include_header=False, quote_style='never')
            print("Success!!")

        elif args.csv:
            if file_n == 'csv':
                print("This is already a .csv file")
            else:
                df.write_csv(file=f'{name}' + '.csv',
                             include_header=True, float_scientific=False)
        else:
            print("File not recognized")

    except ColumnNotFoundError:
        print("⚠️Need to delete header before trying to convert⚠️ ")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--csv', action='store_true',
                        help="Convert the file to .csv")
    parser.add_argument('-d', '--dat', action='store_true',
                        help="Convert/Reformat a file to a .dat for server uploading")
    parser.add_argument('-r', '--rec', action='store_true',
                        help="Add Record column for upload to central server")
    args = parser.parse_args()

    read_file(args)
