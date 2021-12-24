import datetime as dt

import pandas as pd
from pybit import HTTP

import api_keys
import utils
from exchanges.IExchange import IExchange


class ByBit(IExchange):
    NAME = 'ByBit'

    # Modify maker/taker fees here for the ByBit exchange
    # Make sure these values are floats, use decimal notation with a dot
    MAKER_FEE_PCT = -0.025
    TAKER_FEE_PCT = 0.075

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

    def __init__(self):
        super().__init__()
        self.my_api_key = api_keys.BYBIT_API_KEY
        self.my_api_secret_key = api_keys.BYBIT_API_SECRET
        self.my_api_endpoint = 'https://api.bybit.com'
        # Unauthenticated
        self.session_unauth = HTTP(endpoint=self.my_api_endpoint)
        # Authenticated
        self.session_auth = HTTP(endpoint=self.my_api_endpoint, api_key=self.my_api_key,
                                 api_secret=self.my_api_secret_key)
        # self.load_markets()

    def load_markets(self):
        print(f'Loading {self.NAME} markets.')
        markets = self.session_unauth.query_symbol()['result']
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

        # The issue with ByBit API is that you can get a maximum of 200 bars from it.
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
            result = self.session_auth.query_kline(symbol=pair, interval=self.interval_map[interval],
                                                   **{'from': last_datetime_stamp})['result']
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
        df = df[['pair', 'open', 'high', 'low', 'close', 'volume']]

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

# Testing Class
# ex = ExchangeByBit()
# from_time = pd.Timestamp(year=2021, month=10, day=1)
# to_time = pd.Timestamp(year=2021, month=12, day=31)
# ex.get_candle_data(0, 'BTCUSDT', from_time, to_time, "60", include_prior=0, write_to_file=True, verbose=True)
