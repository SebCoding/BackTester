import datetime as dt

import ccxt
import pandas as pd

import utils
from exchanges.IExchange import IExchange


class ByBit2(IExchange):
    NAME = 'ByBit2'

    # Modify maker/taker fees here for the ByBit exchange
    # Make sure these values are floats, use decimal notation with a dot
    MAKER_FEE_PCT = -0.025
    TAKER_FEE_PCT = 0.075

    USE_TESTNET = False  # Boolean True/False

    interval_map = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",  # 1 Hour
        "2h": "2h",  # 2 Hours
        "4h": "4h",  # 4 Hours
        "6h": "6h",  # 6 Hours
        "12h": '12h',  # 12 Hours
        "1d": "1d",
        "1w": "1w"
    }

    def __init__(self):
        super().__init__()
        # self.my_api_key = api_keys.BYBIT_API_KEY
        # self.my_api_secret_key = api_keys.BYBIT_API_SECRET

        self.bybit = ccxt.bybit()
        if self.USE_TESTNET:
            # Testnet
            self.bybit.set_sandbox_mode(True)
            self.NAME += '-Testnet'
        else:
            # Mainnet
            self.bybit.set_sandbox_mode(False)
        self.bybit.options['defaultType'] = 'future'
        self.bybit.options['adjustForTimeDifference'] = False
        self.bybit.timeout = 30000  # number in milliseconds, default 10000


    def get_maker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'maker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "maker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'maker_fee'].iat[0])
        market = self.bybit.market(pair)
        return market['maker']

    def get_taker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'taker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "taker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'taker_fee'].iat[0])
        # return 0.00075
        market = self.bybit.market(pair)
        return market['taker']

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
            print(f'Fetching {pair} data from {self.NAME}. Interval [{interval}],',
                  f' From[{from_time_str}], To[{to_time_str}]')

        df_list = []
        start_time = from_time

        # Adjust from_time for example to add 200 additional prior entries for ema200
        if include_prior > 0:
            start_time = utils.adjust_from_time(from_time, interval, include_prior)

        last_datetime_stamp = start_time.timestamp() * 1000
        to_time_stamp = to_time.timestamp() * 1000

        while last_datetime_stamp < to_time_stamp:
            result = self.bybit.fetch_ohlcv(
                symbol=pair,
                timeframe=self.interval_map[interval],
                since=last_datetime_stamp
            )
            self.CURRENT_REQUESTS_COUNT += 1
            tmp_df = pd.DataFrame(result, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            # Add pair column
            tmp_df['pair'] = pair

            if tmp_df is None or (len(tmp_df.index) == 0):
                break

            tmp_df.index = [dt.datetime.fromtimestamp(x / 1000) for x in tmp_df.timestamp]
            df_list.append(tmp_df)
            last_datetime_stamp = float(max(tmp_df.timestamp) + 1)  # Add 1 sec to last data received

            # time.sleep(2) # Sleep for x seconds, to avoid being locked out

        if df_list is None or len(df_list) == 0:
            return None

        df = pd.concat(df_list)

        # Drop rows that have a timestamp greater than to_time
        df = df[df.timestamp <= int(to_time.timestamp() * 1000)]

        # Only keep relevant columns OHLCV and re-order
        df = df.loc[:, ['pair', 'open', 'high', 'low', 'close', 'volume']]

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


