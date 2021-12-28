import datetime as dt
from os.path import exists

# Abstract Exchange Class
import ccxt
import pandas as pd

import api_keys
import config
import utils


class ExchangeCCXT:
    NAME = None

    # Modify maker/taker fees here for the ByBit exchange
    # Make sure these values are floats, use decimal notation with a dot
    MAKER_FEE_PCT = -0.025
    TAKER_FEE_PCT = 0.075

    USE_TESTNET = False  # Boolean True/Falsee.

    def __init__(self, name):
        super().__init__()
        self.NAME = name.capitalize()
        self.exchange = getattr(ccxt, name)()

        # exchange_id = name
        # exchange_class = getattr(ccxt, exchange_id)
        # self.exchange = exchange_class({
        #     'apiKey': api_keys.BYBIT_API_KEY,
        #     'secret': api_keys.BYBIT_API_SECRET,
        #     'enableRateLimit': True,
        #     'options': {
        #          'defaultType': 'future',  # 'spot', 'future', 'margin', 'delivery'
        #     },
        # })

        if self.USE_TESTNET:
            # Testnet
            self.exchange.set_sandbox_mode(True)
            self.NAME += '-Testnet'
        else:
            # Mainnet
            self.exchange.set_sandbox_mode(False)

        self.exchange.options['defaultType'] = 'future'
        self.exchange.options['adjustForTimeDifference'] = False
        self.exchange.timeout = 30000  # number in milliseconds, default 10000
        self.exchange.load_markets()

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
            result = self.exchange.fetch_ohlcv(
                symbol=pair,
                timeframe=interval,
                since=int(last_datetime_stamp)
            )
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

    def get_exchange_data_filename_no_ext(self, pair, from_time, to_time, interval, prior=0, include_time=False):
        pair = pair.replace('/', '-')
        if include_time:
            from_str = from_time.strftime('%Y-%m-%d %H.%M')
            to_str = to_time.strftime('%Y-%m-%d %H.%M')
        else:
            from_str = from_time.strftime('%Y-%m-%d')
            to_str = to_time.strftime('%Y-%m-%d')
        if prior > 0:
            filename = config.HISTORICAL_FILES_PATH + '\\' + f'{self.NAME} {pair} [{interval}] {from_str} to {to_str} [-{prior}]'
        else:
            filename = config.HISTORICAL_FILES_PATH + '\\' + f'{self.NAME} {pair} [{interval}] {from_str} to {to_str}'
        return filename

    def save_candle_data(self, pair, from_time, to_time, interval, df, prior=0, include_time=False, verbose=True):

        filename = self.get_exchange_data_filename_no_ext(pair, from_time, to_time, interval, prior, include_time)

        if 'csv' in config.OUTPUT_FILE_FORMAT:
            filename = filename + '.csv'
            df.to_csv(filename, index=True, header=True)
            if verbose:
                print(f'File created => [{filename}]')
        if 'xlsx' in config.OUTPUT_FILE_FORMAT:
            filename = filename + '.xlsx'
            df.to_excel(filename, index=True, header=True)
            # to_excel_formatted(df, filename)
            if verbose:
                print(f'File created => [{filename}]')

    # returns dataframe if file already exists locally and None otherwise
    def get_cached_exchange_data(self, pair, from_time, to_time, interval, prior=0, include_time=False):
        filename = self.get_exchange_data_filename_no_ext(pair, from_time, to_time, interval, prior, include_time)

        if 'csv' in config.OUTPUT_FILE_FORMAT:
            filename += '.csv'
            if exists(filename):
                df = utils.read_csv_to_dataframe(filename)
                # print(df.head().to_string())
                return df
        elif 'xlsx' in config.OUTPUT_FILE_FORMAT:
            filename += '.xlsx'
            if exists(filename):
                return utils.read_excel_to_dataframe(filename)
        else:
            return None

    def validate_interval(self, interval):
        valid_intervals = list(self.exchange.timeframes.keys())
        valid_intervals_str = ' '
        valid_intervals_str = valid_intervals_str.join(valid_intervals)
        if interval not in valid_intervals:
            raise Exception(f'\nInvalid Interval [{interval}]. Expected values: {valid_intervals_str}')

    def validate_pair(self, pair):
        # valid_pairs = self.markets_df['name'].tolist()
        # valid_pairs_str = ' '
        # valid_pairs_str = valid_pairs_str.join(valid_pairs)
        # if pair not in valid_pairs:
        #     raise Exception(f'Invalid pair [{pair}]. Expected list of values: {valid_pairs_str}')
        pass

    def get_maker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'maker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "maker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'maker_fee'].iat[0])
        market = self.exchange.market(pair)
        return market['maker']

    def get_taker_fee(self, pair):
        # self.validate_pair(pair)
        # print(f'taker_fee: {self.markets_df.loc[self.markets_df["name"] == pair, "taker_fee"].iat[0]}')
        # return float(self.markets_df.loc[self.markets_df['name'] == pair, 'taker_fee'].iat[0])
        # return 0.00075
        market = self.exchange.market(pair)
        return market['taker']
