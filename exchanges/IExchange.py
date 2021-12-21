from abc import ABC, abstractmethod

import pandas as pd

import api_keys
from os.path import exists

# Abstract Exchange Class
import config
import utils


class IExchange(ABC):


    NAME = 'abstract'

    # Dictionary of symbols used by exchange to define intervals for candle data
    interval_map = None

    def __init__(self):
        self.my_api_key = api_keys.BYBIT_API_KEY
        self.my_api_secret = api_keys.BYBIT_API_SECRET
        self.my_api_endpoint = None

    # from_time and to_time are being passed as pandas._libs.tslibs.timestamps.Timestamp
    @abstractmethod
    def get_candle_data(self, test_num, symbol, from_time, to_time, interval, include_prior=0, write_to_file=True,
                        verbose=False):
        pass

    def get_exchange_data_filename_no_ext(self, symbol, from_time, to_time, interval, prior=0, include_time=False):
        if include_time:
            from_str = from_time.strftime('%Y-%m-%d %H.%M')
            to_str = to_time.strftime('%Y-%m-%d %H.%M')
        else:
            from_str = from_time.strftime('%Y-%m-%d')
            to_str = to_time.strftime('%Y-%m-%d')
        if prior > 0:
            filename = config.HISTORICAL_FILES_PATH + '\\' + f'{self.NAME} {symbol} [{interval}] {from_str} to {to_str} [-{prior}]'
        else:
            filename = config.HISTORICAL_FILES_PATH + '\\' + f'{self.NAME} {symbol} [{interval}] {from_str} to {to_str}'
        return filename

    def save(self, symbol, from_time, to_time, interval, df, prior=0, include_time=False, verbose=True):

        filename = self.get_exchange_data_filename_no_ext(symbol, from_time, to_time, interval, prior, include_time)

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
    def get_cached_exchange_data(self, symbol, from_time, to_time, interval, prior=0, include_time=False):
        filename = self.get_exchange_data_filename_no_ext(symbol, from_time, to_time, interval, prior, include_time)

        if 'csv' in config.OUTPUT_FILE_FORMAT:
            filename += '.csv'
            if exists(filename):
                return pd.read_csv(filename)
        elif 'xlsx' in config.OUTPUT_FILE_FORMAT:
            filename += '.xlsx'
            if exists(filename):
                return utils.convert_excel_to_dataframe(filename)
        else:
            return None


