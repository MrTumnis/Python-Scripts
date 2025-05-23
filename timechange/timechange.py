#!/{HOME}/{USER}/myenv/bin/python
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "rich",
#     "simple-term-menu",
# ]
# ///

import sys
import os
import datetime
import glob
import pandas as pd
from rich import print as rprint
from rich.console import Console
from rich.theme import Theme
from simple_term_menu import TerminalMenu


custom_theme = Theme({
    "info": "dim cyan",
    "success": "dodger_blue2",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)


def file_read(file_path):
    try:
        df = pd.read_csv(file_path)
        for row in df.head(n=1).itertuples():
            if any(isinstance(item, float) for item in row):
                df = df.drop(0)

        for col in df.columns:
            if col == 'STATION':
                df = df.drop('STATION', axis=1)

        df1 = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        df1 = df1.to_frame()
        df = df.iloc[:, 1:].astype(float)
        df_new = pd.concat([df1, df], axis=1)
        df.columns = df.columns.str.strip()

        return df_new

    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style='error')
    return None


'''Define Timestamps'''


def time_check():
    df = file_read(file_path)

    df['diff_check'] = df.iloc[:6, 0].diff().head()
    one_hour = str('0 days 01:00:00')
    fifteen_min = str('0 days 00:15:00')
    five_min = str('0 days 00:05:00')
    thirty_min = str('0 days 00:30:00')
    one_day = str('1 days 00:00:00')

    time = str(df['diff_check'].mode()[0])

    if time == fifteen_min:
        rprint("This is a 15-min file")
        time = 15
    elif time == thirty_min:
        rprint("This is a 30-min file")
        time = 30
    elif time == one_hour:
        rprint("This is a 60-min file")
        time = 60
    elif time == one_day:
        rprint("This is a Daily file")
        time = 1440
    elif time == five_min:
        rprint("This is a five-min file")
        time = 5
    else:
        console.print(f"{time} is an unsupported time interval", style='error')
        sys.exit()
    return time


def time_change():
    check = time_check()
    df = file_read(file_path)
    df.set_index('TIMESTAMP', inplace=True)
    freq = [15, 30, 60, 1440]

    try:
        options = ['15', '30', '60', '1440', 'exit']
        files_menu = TerminalMenu(options, title=console.print(
            'What aggregation would you like to convert to?'))
        selection = files_menu.show()
        time = options[selection]

        if time == 'exit':
            sys.exit()

        time = int(time)

        if time < check:
            raise Exception(
                "Cannot convert to a time lower than that of the file")

    except ValueError:
        console.print(
            f"Please specify a valid time as an integer ({freq})", style='warning')

        time_file()

    avg_fil = df.filter(regex=r"Avg$", axis=1)
    max_fil = df.filter(regex=r"Max$", axis=1)
    min_fil = df.filter(regex=r"Min$", axis=1)
    tot_fil = df.filter(regex=r"Tot$", axis=1)
    win_fil = df.filter(regex=r"^W.*(?<![n,x])$", axis=1)
    sig_fil = df.filter(regex=r"^S.*(?<![n,x,g])$", axis=1)
    col_order = df.columns

    try:
        new_time = [item for item in freq if item == time]

        if new_time[0] == int(15):
            freq = '15min'
        elif new_time[0] == int(30):
            freq = '30min'
        elif new_time[0] == int(60):
            freq = 'h'
        elif new_time[0] == int(1440):
            freq = 'd'

    except Exception as e:
        console.print(f"#2 Error occurred: {e}", style='error')
        sys.exit()

    df_avg = avg_fil.resample((freq), closed='right', label='right').mean()
    df_max = max_fil.resample((freq), closed='right', label='right').mean()
    df_min = min_fil.resample((freq), closed='right', label='right').mean()
    df_win = win_fil.resample((freq), closed='right', label='right').mean()
    df_sig = sig_fil.resample((freq), closed='right', label='right').mean()
    df_tot = tot_fil.resample((freq), closed='right', label='right').sum()

    df_or = pd.concat([df_avg, df_max, df_min, df_win, df_sig, df_tot], axis=1)

    try:
        df_list = df.columns.to_list()
        df_or_list = df_or.columns.to_list()

        if len(df_list) == len(df_or_list):
            df_list.extend(df_or_list)
            df_reor = df_or[col_order]
            df_new = df_reor.apply(lambda x: round(x, 3))
            return df_new, time

    # lengthy way to handle columns not easily found via regex
        else:
            if len(df_list) != len(df_or_list):
                df_list = list(set(df_list) ^ set(df_or_list))
                console.print(
                    f"Rows [green]{df_list}[/green] were not recognized.", style='warning')
                ag_list = ['avg', 'sum', 'min', 'max']
                user_input = {}

                i = 0
                while i < len(df_list):
                    for item in df_list:
                        options = ['avg', 'sum', 'min', 'max']
                        files_menu = TerminalMenu(options, title=console.print(
                            f'What aggregation would you like for [bold green]{item}[/bold green]?'))
                        selection = files_menu.show()
                        value = options[selection]

                        if value in ag_list:
                            user_input.update({item: value})
                            df_temp = df[item]
                            df_new = pd.concat([df_temp])
                            i += 1

                        else:
                            raise Exception(
                                f"'{value}' is not a proper input. Please enter a valid aggregation: '{ag_list}'")

                val_avg = [key for key, val in user_input.items()
                           if val == 'avg']
                val_sum = [key for key, val in user_input.items()
                           if val == 'sum']
                val_min = [key for key, val in user_input.items()
                           if val == 'min']
                val_max = [key for key, val in user_input.items()
                           if val == 'max']
                df_avg = df[val_avg]
                df_sum = df[val_sum]
                df_min = df[val_min]
                df_max = df[val_max]
                df1 = df_avg.resample(
                    (freq), closed='right', label='right').mean()
                df2 = df_sum.resample(
                    (freq), closed='right', label='right').sum()
                df3 = df_min.resample(
                    (freq), closed='right', label='right').min()
                df4 = df_max.resample(
                    (freq), closed='right', label='right').max()
                df = pd.concat([df_or, df1, df2, df3, df4], axis=1)
                df_reor = df[col_order]
                # Could likely take a user input for the decimal so that variables like Evap will be a more precise representation
                df_new = df_reor.apply(lambda x: round(x, 3))

                return df_new, time

    except Exception as e:
        console.print(f"#3 Error occurred: {e}", style='error')
        sys.exit()


'''Export the new time file'''


def time_file():
    date = datetime.datetime.now()

    try:
        items = time_change()
        styled_df = items[0]
        time = items[1]
        styled_df.to_csv(date.strftime("%Y%m%d") +
                         '-' f"{time}-min_{file_path}", index=True)
        console.print(f"Success! File has been converted to a {
                      time}-min datafile", style='success')

    except Exception as e:
        console.print(f"#4 Error occurred: {e}", style='error')
        sys.exit()


if __name__ == '__main__':

    while True:
        files = glob.glob(os.path.join('*.csv'))
        files_menu = TerminalMenu(files, title='Select a csv file')
        index = files_menu.show()
        file_path = files[index]

        if file_path.endswith(".csv"):
            break

        else:
            console.print(
                "This is not a CSV file. Please try again.", style='error')

    time_file()
