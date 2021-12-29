"""
    Class that defines a DataReader that allows to read historical data
    stored in the PostgreSQL database.
"""
import pandas as pd

import utils
from database.BaseDbData import BaseDbData


class DbDataReader(BaseDbData):

    def __init__(self, exchange_name):
        super().__init__(exchange_name)

    def get_candle_data(self, pair, from_time, to_time, interval, include_prior=0, verbose=True):
        # self.validate_pair(pair)
        # self.validate_interval(interval)

        # Adjust from_time for example to add 200 additional prior entries for example ema200
        if include_prior > 0:
            start_time = utils.adjust_from_time(from_time, interval, include_prior)
        else:
            start_time = from_time

        table_name = self.get_table_name(pair, interval)
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        from_time_str = from_time.strftime('%Y-%m-%d %H:%M:%S')
        to_time_str = to_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"SELECT index, open, high, low, close, volume FROM public.\"{table_name}\" WHERE index BETWEEN TIMESTAMP'{start_time_str}' AND TIMESTAMP'{to_time_str}'"

        if verbose:
            print(f'Fetching {pair} data from database. Interval [{interval}],',
                  f' From[{from_time_str}], To[{to_time_str}]')
            # print(query)

        # Load data into the DataFrame using the read_sql() method from pandas
        data_df = pd.read_sql(query, self.engine)
        data_df.set_index(['index'], inplace=True)
        # print(data_df.tail().to_string())
        # exit(0)
        return data_df