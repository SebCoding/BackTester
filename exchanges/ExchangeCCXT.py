import datetime as dt
from os.path import exists

import ccxt
import pandas as pd

import constants
import utils
from Configuration import Configuration


class ExchangeCCXT:
    NAME = None

    def __init__(self, name, pair):
        self.config = Configuration.get_config()
        self.exchange = getattr(ccxt, name)()
        self.NAME = self.exchange.name
        self.use_testnet = self.config['exchange']['use_testnet']

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

        if self.use_testnet:
            # Testnet
            self.exchange.set_sandbox_mode(True)
            self.NAME += '_Testnet'
        else:
            # Mainnet
            self.exchange.set_sandbox_mode(False)

        self.exchange.timeout = 300000  # number in milliseconds, default 10000

        if self.exchange.name == 'Binance' and pair.endswith('USD'):
            #pair = pair.replace('USD', '/USD')
            self.exchange.options['defaultType'] = 'delivery'
        else:
            self.exchange.options['defaultType'] = 'future'
        self.exchange.load_markets()

    def get_candle_data(self, pair, from_time, to_time, interval, include_prior=0, write_to_file=True,
                        verbose=False):
        self.validate_pair(pair)
        self.validate_interval(interval)

        if self.exchange.name == 'Binance' and pair.endswith("USD"):
            pair = pair.replace('USD', '/USD')


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
            print(f'Fetching {pair} data from {self.NAME}. Interval [{interval}],',
                  f' From[{from_time_str}], To[{to_time_str}]')

        df_list = []
        start_time = from_time

        # Adjust from_time for example to add 200 additional prior entries for example ema200
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
            last_datetime_stamp = float(max(tmp_df.timestamp) + 1000)  # Add (1000ms = 1 sec) to last data received

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
        filename = f"{self.config['output']['historical_files_path']}\\{self.NAME} {pair} [{interval}] {from_str} to {to_str}"
        if prior > 0:
            filename += f" [-{prior}]"
        return filename

    def save_candle_data(self, pair, from_time, to_time, interval, df, prior=0, include_time=False, verbose=True):

        filename = self.get_exchange_data_filename_no_ext(pair, from_time, to_time, interval, prior, include_time)

        if 'csv' in self.config['output']['output_file_format']:
            filename = filename + '.csv'
            df.to_csv(filename, index=True, header=True)
            if verbose:
                print(f'File created => [{filename}]')
        if 'xlsx' in self.config['output']['output_file_format']:
            filename = filename + '.xlsx'
            df.to_excel(filename, index=True, header=True)
            # to_excel_formatted(df, filename)
            if verbose:
                print(f'File created => [{filename}]')

    # returns dataframe if file already exists locally and None otherwise
    def get_cached_exchange_data(self, pair, from_time, to_time, interval, prior=0, include_time=False):
        filename = self.get_exchange_data_filename_no_ext(pair, from_time, to_time, interval, prior, include_time)

        if 'csv' in self.config['output']['output_file_format']:
            filename += '.csv'
            if exists(filename):
                df = utils.read_csv_to_dataframe(filename)
                # print(df.head().to_string())
                return df
        elif 'xlsx' in self.config['output']['output_file_format']:
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
        if self.exchange.name == 'Binance' and pair.endswith("USD"):
            pair = pair.replace('USD', '/USD')
        market = self.exchange.market(pair)
        if market is None:
            raise Exception(f'\nInvalid [{pair}] for exchange {self.NAME}.')

    def get_maker_fee(self, pair):
        if self.exchange.name == 'Binance' and pair.endswith("USD"):
            pair = pair.replace('USD', '/USD')
        market = self.exchange.market(pair)
        return market['maker']

    def get_taker_fee(self, pair):
        if self.exchange.name == 'Binance' and pair.endswith("USD"):
            pair = pair.replace('USD', '/USD')
        market = self.exchange.market(pair)
        return market['taker']
