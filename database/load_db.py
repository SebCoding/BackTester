"""
    Code used to load the historical candle data into the PostgreSQL database
"""

import datetime as dt
from database.DbDataLoader import DbDataLoader

# Example1: load pair data for 1 timeframe
loader = DbDataLoader('Bybit')
from_time = dt.datetime(2010, 1, 1)
loader.load_candle_data('BTCUSDT', from_time, '1m', True)

# Example2: load pair data for all timeframes
# loader = DataLoader('Binance')
# loader.load_pair_data_all_timeframes('BTCUSDT')

# Example3: load pair data for all timeframes
# loader = DataLoader('Bybit')
# loader.load_pair_data_all_timeframes('BTCUSDT')