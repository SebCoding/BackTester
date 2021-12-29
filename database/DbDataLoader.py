"""
    Code to load all historical data from exchange to local PostgreSQL database
"""
import datetime as dt
import time

import ccxt
import pandas as pd

import config
import utils
from database.BaseDbData import BaseDbData


class DbDataLoader(BaseDbData):

    def __init__(self, exchange_name):
        super().__init__(exchange_name)
        # Exchange
        self.exchange = getattr(ccxt, exchange_name.lower())()
        self.exchange.options['defaultType'] = 'future'
        self.exchange.timeout = 300000  # number in milliseconds, default 10000
        self.exchange.load_markets()

    def validate_interval(self, interval):
        valid_intervals = list(self.exchange.timeframes.keys())
        valid_intervals_str = ' '
        valid_intervals_str = valid_intervals_str.join(valid_intervals)
        if interval not in valid_intervals:
            raise Exception(f'\nInvalid Interval [{interval}]. Expected values: {valid_intervals_str}')

    def validate_pair(self, pair):
        market = self.exchange.market(pair)
        if market is None:
            raise Exception(f'\nInvalid [{pair}] for exchange {self.exchange.name}.')



    def load_candle_data(self, pair, from_time, interval, verbose=False):
        self.validate_pair(pair)
        self.validate_interval(interval)
        self.delete_all_pair_interval_data(pair, interval)

        table_name = self.get_table_name(pair, interval)
        start_time = from_time
        last_datetime_stamp = start_time.timestamp() * 1000

        while True:
            if verbose:
                # from_time_str = from_time.strftime('%Y-%m-%d')
                # to_time_str = to_time.strftime('%Y-%m-%d')
                print(f'Loading {pair} data from {self.exchange.name} into the [{table_name}] table.',
                      f'From[{dt.datetime.fromtimestamp(last_datetime_stamp / 1000)}] => ', end='')

            result = self.exchange.fetch_ohlcv(
                symbol=pair,
                timeframe=interval,
                since=int(last_datetime_stamp)
            )
            print('done.')
            df = pd.DataFrame(result, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or (len(df.index) == 0):
                break
            df.index = [dt.datetime.fromtimestamp(x / 1000) for x in df.open_time]

            # Set proper data types
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)

            # Write data into the table in PostgreSQL database
            df.to_sql(table_name, self.engine, index=True, if_exists='append')
            # Add 1s to the last row we received
            last_datetime_stamp = float(max(df.open_time) + 1000)  # Add (1000ms = 1s) to last data received

        # Make the index column the Primary Key
        query = f'ALTER TABLE IF EXISTS public."{table_name}" DROP CONSTRAINT IF EXISTS "{table_name}_pkey"; ' \
                f'ALTER TABLE public."{table_name}" ADD PRIMARY KEY (index);'
        # print(query)
        self.exec_sql_query(query)

    # delete all data in the database for this pair and this interval
    def delete_all_pair_interval_data(self, pair, interval):
        table_name = self.get_table_name(pair, interval)
        print(f'Deleting table [{self.db_name}].[{table_name}]')
        query = 'DROP TABLE IF EXISTS public."<table_name>"'
        query = query.replace('<table_name>', table_name)
        self.exec_sql_query(query)

    def load_pair_data_all_timeframes(self, pair):
        execution_start = time.time()
        for interval in reversed(config.VALID_INTERVALS):
        # for interval in reversed('1m'):
            from_time = dt.datetime(2000, 1, 1)
            self.load_candle_data(pair, from_time, interval, True)
        exec_time = utils.format_execution_time(time.time() - execution_start)
        print(f'Load completed. Execution Time: {exec_time}\n')









