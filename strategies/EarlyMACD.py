import datetime as dt

import numpy as np
import pandas as pd
import talib

import utils

from strategies.IEarlyStrategy import IEarlyStrategy
from strategies.MACD import MACD


# We inherit from EarlyStrategy for the process_trades() method
# We inherit from the parent strategy for the rest.
class EarlyMACD(IEarlyStrategy, MACD):
    NAME = 'Early MACD'

    # Ratio of the total account balance allowed to be traded.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 1.0

    # Trend indicator: EMA - Exponential Moving Average
    EMA_PERIODS = 200

    # Trend following momentum indicator:
    # MACD - Moving Average Convergence Divergence
    MACD_FAST_PERIOD = 12
    MACD_SLOW_PERIOD = 26
    MACD_SIGNAL_PERIOD = 9

    def __init__(self, params):
        MACD.__init__(self, params)
        IEarlyStrategy.__init__(self, params)

    # Find with a minute precision the first point where macd crossed macdsignal
    # and return the time and closing price for that point in time + delta minutes
    def find_exact_trade_entry(self, df, from_time, to_time, delta=0):
        # We need to get an extra row to see the value at -1min in case the cross is on the first row
        to_time = to_time - dt.timedelta(minutes=1)

        minutes_df = self.exchange.get_candle_data(0, self.params['Pair'], from_time, to_time, "1m", include_prior=0,
                                                   write_to_file=False, verbose=False)
        # Used cached data (very slow)
        # minutes_df = self.get_minutes_from_cached_file(from_time, to_time)

        # Only keep the close column
        minutes_df = minutes_df[['close']]

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
