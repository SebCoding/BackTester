"""
    Base Abstract Exchange Class.
    Exchange classes that do not use the ccxt library inherit from this class
"""
import math
from abc import ABC, abstractmethod
from os.path import exists

import constants
import utils
from Configuration import Configuration


class BaseExchange(ABC):
    NAME = 'abstract'

    # Dictionary of pairs used by exchange to define intervals for candle data
    interval_map = None

    CURRENT_REQUESTS_COUNT = 0

    def __init__(self):
        super().__init__()
        self.config = Configuration.get_config()
        self.markets_df = None
        self.config = Configuration.get_config()

    # from_time and to_time are being passed as pandas._libs.tslibs.timestamps.Timestamp
    @abstractmethod
    def get_candle_data(self, test_num, pair, from_time, to_time, interval, include_prior=0, write_to_file=True,
                        verbose=False):
        pass

    @abstractmethod
    def get_maker_fee(self, pair):
        pass

    @abstractmethod
    def get_taker_fee(self, pair):
        pass

    def get_exchange_data_filename_no_ext(self, pair, from_time, to_time, interval, prior=0, include_time=False):
        if include_time:
            from_str = from_time.strftime('%Y-%m-%d %H.%M')
            to_str = to_time.strftime('%Y-%m-%d %H.%M')
        else:
            from_str = from_time.strftime('%Y-%m-%d')
            to_str = to_time.strftime('%Y-%m-%d')
        filename = f"{self.config['output']['historical_files_path']}\\{self.NAME} {pair} [{interval}] {from_str} to {to_str}"
        if prior > 0:
            filename += " [-{prior}] "
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
        valid_intervals = list(self.interval_map.keys())
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

    # generated random timeouts for testing purposes
    def random_timeout(self):
        import random
        # Generate timeouts 1 out x times
        x = 4
        rand = math.floor(random.uniform(0, x))
        # print(f'random_timeout={rand}')
        if rand == 0:
            raise TimeoutError(f'Randomly generated TimeoutError from {self.NAME} testing method')
