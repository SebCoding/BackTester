"""
    The purpose of these tests if to determine at what point
    the number of rows in a dataset no longer affect the EMA value
    Results:
    EMA     NB_ROWS     NB_ROWS/EMA
    10	    72	        7.2
    20	    139	        6.95
    50	    423	        8.46
    100	    1026	    10.26
    200	    1799	    8.995
    500	    4378	    8.756
    1000	9513	    9.513
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

EMA_PERIOD = 200
tmp_df = pd.DataFrame()
tmp_df = tmp_df.assign(EMA=talib.EMA(df['close'], timeperiod=EMA_PERIOD))
ref_value = round(tmp_df.iloc[-1]['EMA'], 4)
print(f'\nref_value={ref_value}')
# exit(1)

len_df = len(df)
i = 0
while True:
    df2 = df[i:len_df]
    df2 = df2.assign(EMA=talib.EMA(df2['close'], timeperiod=EMA_PERIOD))
    ema_last_row = round(df2.iloc[-1]['EMA'], 4)
    if ema_last_row != ref_value:
        # print(f'Slice From {i} to {len_df}')
        # print(df2.tail(5).to_string())
        print(f'REF={ref_value} Current={ema_last_row}')
        print(f'Rows={len(df2)}')
        print()
        break







