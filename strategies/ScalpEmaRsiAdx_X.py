import datetime as dt

import numpy as np
import pandas as pd
import talib

import constants
import utils
from Configuration import Configuration
from enums.TradeTypes import TradeTypes

from strategies.BaseStrategy_X import BaseStrategy_X
from strategies.ScalpEmaRsiAdx import ScalpEmaRsiAdx


# We inherit from BaseStrategy_X for the process_trades_fixed_pct() method
# We inherit from the parent strategy for the rest.
class ScalpEmaRsiAdx_X(BaseStrategy_X, ScalpEmaRsiAdx):
    """
        Implementation of the Scalping Strategy found here:
        https://www.youtube.com/watch?v=vBM0imYSzxI
        Using EMA RSI ADX Indicators. This _X version of the base strategy uses a minute precision
        for trade entries as opposed to the end of the interval.
    """
    def __init__(self, params):
        ScalpEmaRsiAdx.__init__(self, params)
        BaseStrategy_X.__init__(self, params)
        self.NAME = self.__class__.__name__
        self.config = Configuration.get_config()

    # Find with a minute precision the first point where we should enter the trade
    # and return the time and closing price for that point in time
    def find_exact_trade_entry(self, df, from_time, to_time, trade_type):
        # print(f'trade_type={trade_type}')

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

        # print(minutes_df.to_string())
        # exit(0)

        # Only keep the close column
        # To remove warning use below syntax instead of: minutes_df = minutes_df[['close']]
        minutes_df = minutes_df.loc[:, ['high', 'low', 'close']]

        # Convert column type to float
        minutes_df['high'] = minutes_df['high'].astype(float)
        minutes_df['low'] = minutes_df['low'].astype(float)
        minutes_df['close'] = minutes_df['close'].astype(float)
        # print(minutes_df.to_string())
        # print()

        tmp_list = []
        for index, row in minutes_df.iterrows():
            # print(f'Row >>> [{index}]')
            # TODO: check if we could use a subset of the data to obtain the same results like df.tail(1000)
            df2 = df.copy()
            df2 = df2.append(row)

            # Trend Indicator. EMA-50
            df2[self.ema_col_name] = talib.EMA(df2['close'], timeperiod=self.EMA)

            # Momentum Indicator. RSI-3
            df2[self.rsi_col_name] = talib.RSI(df2['close'], timeperiod=self.RSI)

            # Volatility Indicator. ADX-5
            df2[self.adx_col_name] = talib.ADX(df2['high'], df2['low'], df2['close'], timeperiod=self.ADX)

            # self.df['EMA_Tolerance'] = self.df[self.ema_col_name] * self.EMA_TOLERANCE
            df2['EMA_LONG'] = df2[self.ema_col_name] - df2[self.ema_col_name] * self.EMA_TOLERANCE
            df2['EMA_SHORT'] = df2[self.ema_col_name] + df2[self.ema_col_name] * self.EMA_TOLERANCE

            # print(f'result_df_len: {len(df2)}')
            # print(df2.to_string())
            # print()

            # Mark long entries
            if trade_type == TradeTypes.Long:
                df2.loc[
                    (
                            (df2['close'] > df2['EMA_LONG']) &  # price > EMA
                            (df2[self.rsi_col_name] > self.RSI_MIN_ENTRY) &  # RSI > RSI_MIN_ENTRY_THRESHOLD
                            (df2[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
                    ),
                    'enter'] = 1

            # Mark short entries
            if trade_type == TradeTypes.Short:
                df2.loc[
                    (
                            (df2['close'] < df2['EMA_SHORT']) &  # price < EMA-50
                            (df2[self.rsi_col_name] < self.RSI_MAX_ENTRY) &  # RSI > RSI_MAX_ENTRY_THRESHOLD
                            (df2[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
                    ),
                    'enter'] = -1

            # Just keep last row
            tmp_list.append(df2.iloc[[-1]])

        result_df = pd.concat(tmp_list)

        # print(f'result_df_len: {len(result_df)}')
        # print(result_df.to_string())
        # print('\n')
        # exit(1972)

        # Find first occurrence of crossing.
        entry_price = 0.0  # Force float
        entry_time = dt.datetime(1, 1, 1)
        close_col_index = result_df.columns.get_loc("close")
        for i, row in enumerate(result_df.itertuples(index=True), 0):
            if row.enter in [-1, 1]:
                entry_price = result_df.iloc[i, close_col_index]
                entry_time = utils.idx2datetime(result_df.index.values[i]) + dt.timedelta(minutes=1)
                break
        return entry_time, entry_price
