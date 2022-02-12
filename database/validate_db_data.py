"""
    Code used to validate the historical candle data into the PostgreSQL database.
    Check that there are no gaps in time between candle data
"""

import datetime as dt
import sys
import time

import pandas as pd

import constants
import utils
from database.DbDataLoader import DbDataLoader
from database.DbDataReader import DbDataReader

def find_all_gaps(exchanges, pairs, intervals):
    start = dt.datetime(2000, 1, 1)
    end = dt.datetime(2030, 1, 1)
    for exchange in exchanges:
        for pair in pairs:
            for i in intervals:
                find_gaps(exchange, pair, start, end, i)


def find_gaps(exchange, pair, from_time, to_time, interval):
    print(f"Validating {exchange} {pair}[{interval}]: ", end='')
    reader = DbDataReader(exchange)
    df = reader.get_candle_data(pair, from_time, to_time, interval, include_prior=0, verbose=False)
    df.sort_index(axis=0, inplace=True)
    start = df.index[0]
    end = df.index[-1]
    # print(start, end)

    freq = interval.replace('M', 'MS').replace('w', 'W-MON').replace('d', 'D').replace('h', 'H').replace('m', 'min')
    range = pd.date_range(start=start, end=end, freq=freq)
    # print(range)

    # dates which are not in the sequence are returned
    result = range.difference(df.index)
    if result is not None and len(result) > 0:
        print(f"failed\nMissing Entries:")
        print(result)
        # print('Validation aborted')
        # sys.exit(1)
    else:
        print('Ok')
    # print(df.head(5).to_string())
    # print(df.tail(5).to_string())


exchanges = ['Binance', 'Bybit']
pairs = ['BTCUSD', 'ETHUSD', 'BTCUSDT', 'ETHUSDT']
intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w']

find_all_gaps(exchanges, pairs, intervals)
