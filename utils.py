from datetime import timedelta
import datetime as dt
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

import config

##################################################################################
### Change Timezone if Needed
##################################################################################
## my_list simply unpacks the list elements and pass each one of them as parameters to the print function
# print(*pytz.all_timezones, sep='\n')

# In windows command prompt try:
#     This gives current timezone: tzutil /g
#     This gives a list of timezones: tzutil /l
#     This will set the timezone: tzutil /s "Central America Standard Time"

#os.system('tzutil /s "Eastern Standard Time"')
#os.system('tzutil /s "Singapore Standard Time"')
#time.strftime('%Y-%m-

# Adjust from_time to include prior X entries for that interval for ema200
def adjust_from_time(from_time, interval, include_prior):
    delta = include_prior - 1

    # Possible Values: 1 3 5 15 30 60 120 240 360 720 "D" "W"
    if interval not in ["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W"]:
        return from_time

    if interval == 'W':
        from_time = from_time - timedelta(weeks=delta)
    elif interval == 'D':
        from_time = from_time - timedelta(days=delta)
    else:
        from_time = from_time - timedelta(minutes=int(interval) * delta)
    return from_time


# Convert an index value of type numpy.datetime64 to type datetime
def idx2datetime(index_value):
    return dt.datetime.utcfromtimestamp(index_value.astype('O') / 1e9)

def save_dataframe2file(test_num, exchange, symbol, from_time, to_time, interval, df,
                        exchange_data_file=False, include_time=False, verbose=True):
    test_num = str(test_num)

    if include_time:
        from_str = from_time.strftime('%Y-%m-%d %H.%M')
        to_str = to_time.strftime('%Y-%m-%d %H.%M')
    else:
        from_str = from_time.strftime('%Y-%m-%d')
        to_str = to_time.strftime('%Y-%m-%d')

    filename = f'{exchange} {symbol} [{interval}] {from_str} to {to_str}'

    if exchange_data_file:
        filename = config.HISTORICAL_FILES_PATH + '\\' + filename
    else:
        filename = config.RESULTS_PATH + f'\\{test_num} ' + filename + ' Trades'

    if 'csv' in config.OUTPUT_FILE_FORMAT:
        filename = filename + '.csv'
        df.to_csv(filename, index=True, header=True)
        if verbose:
            print(f'File created => [{filename}]')
    if 'xlsx' in config.OUTPUT_FILE_FORMAT:
        filename = filename + '.xlsx'
        df.to_excel(filename, index=True, header=True)
        #to_excel_formatted(df, filename)
        if verbose:
            print(f'File created => [{filename}]')

# TODO: Find a way to format the Excel workbook prior to saving to file
def to_excel_formatted(df, filename):
    wb = Workbook()
    ws = wb.active

    for r in dataframe_to_rows(df, index=True, header=True):
        ws.append(r)

    # TODO: Fix this code. It destroys index datetime format
    for cell in ws['A'] + ws[1]:
        cell.style = 'Pandas'

    wb.save(filename)

def convert_interval_to_min(interval):
    if interval == 'W':
        return 7 * 24 * 60
    elif interval == 'D':
        return 24 * 60
    return int(interval)


def convert_excel_to_dataframe(filename):
    wb = load_workbook(filename)
    ws = wb['Sheet1']

    # To convert a worksheet to a Dataframe you can use the value's property.
    # This is very easy if the worksheet has no headers or indices:
    # df = DataFrame(ws.values)

    # https://openpyxl.readthedocs.io/en/stable/pandas.html

    # If the worksheet does have headers or indices, such as one created by Pandas,
    # then a little more work is required:
    from itertools import islice
    data = ws.values
    cols = next(data)[1:]
    data = list(data)
    idx = [r[0] for r in data]
    data = (islice(r, 1, None) for r in data)
    df = pd.DataFrame(data, index=idx, columns=cols)

    return df