import datetime as dt
import logging
import math
import time

import pandas as pd
import pybit
import requests
import urllib3.exceptions
from pybit import HTTP

import api_keys
import utils
from exchanges.BaseExchange import BaseExchange


class Bybit(BaseExchange):
    NAME = 'Bybit'

    # Modify maker/taker fees here for the Bybit exchange
    # Make sure these values are floats, use decimal notation with a dot
    MAKER_FEE_PCT = -0.025
    TAKER_FEE_PCT = 0.075

    USE_TESTNET = False  # Boolean True/False
    TESTNET_API_ENDPOINT = 'https://api-testnet.bybit.com'
    MAINNET_API_ENDPOINT = 'https://api.bybit.com'

    interval_map = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",  # 1 Hour
        "2h": "120",  # 2 Hours
        "4h": "240",  # 4 Hours
        "6h": "360",  # 6 Hours
        "12h": '720',  # 12 Hours
        "1d": "D",
        "1w": "W"
    }

    # Use these values to handle timeouts in subclasses
    RETRY_WAIT_TIME = 10  # Wait time in seconds
    MAX_RETRIES = 20

    def __init__(self):
        super().__init__()
        self.my_api_key = api_keys.BYBIT_API_KEY
        self.my_api_secret_key = api_keys.BYBIT_API_SECRET

        if self.USE_TESTNET:
            # Testnet
            self.my_api_endpoint = self.TESTNET_API_ENDPOINT
        else:
            # Mainnet
            self.my_api_endpoint = self.MAINNET_API_ENDPOINT

        force_retry = True
        max_retries = 4  # default is 3
        retry_delay = 3  # default is 3 seconds
        request_timeout = 30  # default is 10 seconds
        log_requests = True
        logging_level = logging.INFO  # default is logging.INFO
        spot = False  # spot or futures

        # Authenticated
        self.session_authenticated = HTTP(
            endpoint=self.my_api_endpoint,
            api_key=self.my_api_key,
            api_secret=self.my_api_secret_key,
            request_timeout=request_timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            force_retry=force_retry,
            log_requests=log_requests,
            logging_level=logging_level,
            spot=spot)

        # Unauthenticated
        self.session_unauthenticated = HTTP(
            endpoint=self.my_api_endpoint,
            request_timeout=request_timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            force_retry=force_retry,
            log_requests=log_requests,
            logging_level=logging_level,
            spot=spot)

        # self.load_markets()

    def load_markets(self):
        print(f'Loading {self.NAME} markets.')
        markets = self.session_unauthenticated.query_symbol()['result']
        self.markets_df = pd.DataFrame.from_dict(markets, orient='columns')
        self.markets_df = self.markets_df[['name', 'alias', 'status', 'base_currency',
                                           'quote_currency', 'taker_fee', 'maker_fee']]
        # print(self.markets_df.to_string())

    def get_maker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'maker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "maker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'maker_fee'].iat[0])
        return -0.00025

    def get_taker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'taker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "taker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'taker_fee'].iat[0])
        return 0.00075

    def get_candle_data(self, test_num, pair, from_time, to_time, interval, include_prior=0, write_to_file=True,
                        verbose=False):
        self.validate_pair(pair)
        self.validate_interval(interval)

        # Use locally saved data if it exists
        cached_df = self.get_cached_exchange_data(pair, from_time, to_time, interval, prior=include_prior)

        from_time_str = from_time.strftime('%Y-%m-%d')
        to_time_str = to_time.strftime('%Y-%m-%d')
        if cached_df is not None:
            if verbose:
                print(f'Using locally cached data for {pair} from {self.NAME}.',
                      f'Interval [{interval}], From[{from_time_str}], To[{to_time_str}]')
            return cached_df

        # The issue with Bybit API is that you can get a maximum of 200 bars from it.
        # So if you need to get data for a large portion of the time you have to call it multiple times.

        if verbose:
            print(
                f'Fetching {pair} data from {self.NAME}. Interval [{interval}], From[{from_time_str}], To[{to_time_str}]')

        df_list = []
        start_time = from_time

        # Adjust from_time for example to add 200 additional prior entries for ema200
        if include_prior > 0:
            start_time = utils.adjust_from_time(from_time, interval, include_prior)

        last_datetime_stamp = start_time.timestamp()
        to_time_stamp = to_time.timestamp()

        while last_datetime_stamp < to_time_stamp:
            result = self.session_authenticated.query_kline(
                symbol=pair,
                interval=self.interval_map[interval],
                **{'from': last_datetime_stamp})[
                'result']
            self.CURRENT_REQUESTS_COUNT += 1
            tmp_df = pd.DataFrame(result)

            if tmp_df is None or (len(tmp_df.index) == 0):
                break

            tmp_df.index = [dt.datetime.fromtimestamp(x) for x in tmp_df.open_time]
            # tmp_df.index = [dt.datetime.utcfromtimestamp(x) for x in tmp_df.open_time]
            df_list.append(tmp_df)
            last_datetime_stamp = float(max(tmp_df.open_time) + 1)  # Add 1 sec to last data received

            # time.sleep(2) # Sleep for x seconds, to avoid being locked out

        if df_list is None or len(df_list) == 0:
            return None

        df = pd.concat(df_list)

        # Drop rows that have a timestamp greater than to_time
        df = df[df.open_time <= int(to_time.timestamp())]

        # Only keep relevant columns OHLC(V)
        df.rename(columns={'symbol': 'pair'}, inplace=True)
        df = df.loc[:, ['pair', 'open', 'high', 'low', 'close', 'volume']]
        # df = df[['pair', 'open', 'high', 'low', 'close', 'volume']]

        # Set proper data types
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # Write to file
        if write_to_file:
            self.save_candle_data(pair, from_time, to_time, interval, df, prior=include_prior,
                                  include_time=True if interval == '1' else False, verbose=False)
        return df
