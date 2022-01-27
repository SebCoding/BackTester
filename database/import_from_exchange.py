"""
    Code used to load the historical candle data into the PostgreSQL database
"""

import datetime as dt
import time

import utils
from database.DbDataLoader import DbDataLoader

# Example 1: load pair data for 1 timeframe
# pair = 'BTCUSDT'
# loader = DbDataLoader('Bybit')
# from_time = dt.datetime(2010, 1, 1)
# interval = '1m'
#
# execution_start = time.time()
# loader.load_candle_data(pair, from_time, interval, True)
# exec_time = utils.format_execution_time(time.time() - execution_start)
# print(f'Load completed. Execution Time: {exec_time}\n')


# Example 2: load pair data for all timeframes
# pair = 'ETHUSDT'
# loader = DbDataLoader('Binance')
# loader.load_pair_data_all_timeframes(pair)


# Example 3: Load all pairs, for all exchanges
exchanges = ['Binance', 'Bybit', 'Bybit_Testnet']
pairs = ['BTCUSDT', 'ETHUSDT']
for exchange in exchanges:
    for pair in pairs:
        loader = DbDataLoader(exchange)
        loader.load_pair_data_all_timeframes(pair)



