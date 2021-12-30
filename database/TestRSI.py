"""
    The purpose of these tests if to determine at what point
    the number of rows in a dataset no longer affect the RSI value
    Results:
    RSI     NB_ROWS     NB_ROWS/RSI

"""
import datetime as dt
import math
import time

import pandas as pd
import talib

import utils
from database.DbDataReader import DbDataReader

pair = 'BTCUSDT'
from_time = dt.datetime(2010, 1, 1)
to_time = dt.datetime(2021, 12, 31)
interval = '3m'

db_reader = DbDataReader('Bybit')
df = db_reader.get_candle_data('BTCUSDT', from_time, to_time, interval)

RSI_PERIOD = 200
tmp_df = pd.DataFrame()
tmp_df = tmp_df.assign(RSI=talib.RSI(df['close'], timeperiod=RSI_PERIOD))
ref_value = round(tmp_df.iloc[-1]['RSI'], 4)
print(f'\nref_value={ref_value}')
# exit(1)

len_df = len(df)
i = len_df - 20000
while True:
    df2 = df[i:len_df]
    df2 = df2.assign(RSI=talib.RSI(df2['close'], timeperiod=RSI_PERIOD))
    RSI_last_row = round(df2.iloc[-1]['RSI'], 4)
    if RSI_last_row != ref_value:
        # print(f'Slice From {i} to {len_df}')
        # print(df2.tail(5).to_string())
        print(f'REF={ref_value} Current={RSI_last_row}')
        print(f'Rows={len(df2)}')
        print()
        break
    i += 1







