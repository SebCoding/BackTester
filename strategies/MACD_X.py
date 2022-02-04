import datetime as dt

import numpy as np
import pandas as pd
import talib

import constants
import utils

from strategies.BaseStrategy_X import BaseStrategy_X
from strategies.MACD import MACD


# We inherit from BaseStrategy_X for the process_trades_fixed_pct() method
# We inherit from the parent strategy for the rest.
class MACD_X(BaseStrategy_X, MACD):

    def __init__(self, params):
        MACD.__init__(self, params)
        BaseStrategy_X.__init__(self, params)
        self.NAME = self.__class__.__name__

    # Find with a minute precision the first point where macd crossed macdsignal
    # and return the time and closing price for that point in time + delta minutes
    def find_exact_trade_entry(self, df, from_time, to_time, trade_type, delta=0):
        # We need to get an extra row to see the value at -1min in case the cross is on the first row
        to_time = to_time - dt.timedelta(minutes=1)

        if self.config['database']['historical_data_stored_in_db']:
            minutes_df = self.db_reader.get_candle_data(
                self.params['Pair'],
                from_time,
                to_time,
                "1m",
                include_prior=0,
                verbose=False)
        else:
            minutes_df = self.exchange.get_candle_data(
                self.params['Pair'],
                from_time,
                to_time,
                "1m",
                include_prior=0,
                write_to_file=False,
                verbose=False)

        # Only keep the close column
        # To remove warning use below syntax instead of: minutes_df = minutes_df[['close']]
        minutes_df = minutes_df.loc[:, ['close']]

        # Convert column type to float
        minutes_df['close'] = minutes_df['close'].astype(float)

        tmp_list = []
        for index, row in minutes_df.iterrows():
            # print(f'Row >>> [{index}]')
            df2 = df.copy()
            df2 = df2.append(row)

            macd, macdsignal, macdhist = talib.MACD(df2['close'],
                                                    fastperiod=self.MACD_FAST_PERIOD,
                                                    slowperiod=self.MACD_SLOW_PERIOD,
                                                    signalperiod=self.MACD_SIGNAL_PERIOD)
            df2['MACD'] = macd
            df2['MACDSIG'] = macdsignal

            # macdsignal over macd then 1, under 0
            df2['O/U'] = np.where(df2['MACDSIG'] >= df2['MACD'], 1, 0)

            # macdsignal crosses macd
            df2['cross'] = df2['O/U'].diff()

            #         print(df2.tail(20).to_string())
            #         print('\n')

            # Remove nulls
            # df2.dropna(inplace=True)

            # Just keep last row
            tmp_list.append(df2.iloc[[-1]])

        result_df = pd.concat(tmp_list)
        # print(f'result_df_len: {len(result_df)}')

        # print(result_df.to_string())
        # print('\n')

        # Find first occurrence of crossing. Delta optional (add delta minutes)
        price_on_crossing = 0.0  # Force float
        time_on_crossing = dt.datetime(1, 1, 1)
        close_col_index = result_df.columns.get_loc("close")
        for i, row in enumerate(result_df.itertuples(index=True), 0):
            if row.cross in [-1, 1]:
                # print(f'Found 1st Crossing at [{i}] + delta[{delta}]')
                price_on_crossing = result_df.iloc[i + delta, close_col_index]
                time_on_crossing = utils.idx2datetime(result_df.index.values[i + delta])
                break

        return time_on_crossing, price_on_crossing
