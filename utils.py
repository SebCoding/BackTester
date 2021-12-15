from datetime import timedelta
import datetime as dt
import pandas as pd
from openpyxl import load_workbook
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

# Adjust from_time to include prior 200 entries for that interval for ema200
def adjust_from_time(from_time, interval):
    delta = 199

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

def save_dataframe2file(test_num, symbol, from_time, to_time, interval, df, bybit_data_file=False, verbose=True):
    config.OUTPUT_FILE_FORMAT, config.RESULTS_PATH, config.HISTORICAL_FILES_PATH
    test_num = str(test_num)
    from_str = from_time.strftime('%Y-%m-%d')
    to_str = to_time.strftime('%Y-%m-%d')
    fname = f'{symbol}_{from_str}_to_{to_str}_[{interval}]'

    if bybit_data_file:
        fname = config.HISTORICAL_FILES_PATH + '\\' + fname
    else:
        fname = config.RESULTS_PATH + f'\\{test_num}_' + fname + '_Trades'

    if 'csv' in config.OUTPUT_FILE_FORMAT:
        fname = fname + '.csv'
        df.to_csv(fname, index=True, header=True)
        if verbose:
            print(f'Trades file created => [{fname}]')
    if 'xlsx' in config.OUTPUT_FILE_FORMAT:
        fname = fname + '.xlsx'
        df.to_excel(fname, index=True, header=True)
        if verbose:
            print(f'Trades file created => [{fname}]')


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