import polars as pl
import zipfile
import glob
import os
import sys


class DataCheck:
    def __init__(self, file_path, agg, null_items=None):
        self.file_path = file_path
        self.agg = agg  
        self.null = null_items 

    def read_file(self, name, height=None) -> dict:

        file_path = glob.glob(os.path.join(f'*{self.file_path}*'))

        if file_path.endswith('.zip'):
            with zipfile.ZipFile(f'{self.file_path}', 'r') as zip_file:
                file_path = file_path.strip('.zip')
                file = zip_file.extractall(f'{file_path}')


        for file_name in file:
            file = (pl
                .scan_csv(f'{self.file_path}/{file_name}_Table{self.agg}.csv', has_header=True, null_values=self.null_items, raise_if_empty=True)
                .with_columns(pl
                .col('TIMESTAMP').str
                .to_datetime('%Y-%m-%d %H:%M:%S',time_unit=None, time_zone=None, strict=False))
            )
