import datetime

import numpy as np
import talib

from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy


class ScalpEmaRsiAdx(BaseStrategy):
    # Ratio of the total account balance allowed to be traded.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 1.0

    # Trend indicator: EMA - Exponential Moving Average
    EMA_PERIODS = 60

    # % over/under the EMA that can be tolerated to determine if the long/short trade can be placed
    # Value should be between 0 and 1
    EMA_TOLERANCE = 0.02

    # Momentum indicator: RSI - Relative Strength Index
    RSI_PERIODS = 2
    RSI_MIN_SIGNAL_THRESHOLD = 31
    RSI_MAX_SIGNAL_THRESHOLD = 69

    # Trade entry RSI thresholds (by default equal to RSI min/max thresholds)
    RSI_MIN_ENTRY_THRESHOLD = 55
    RSI_MAX_ENTRY_THRESHOLD = 45

    # Volatility indicator: ADX - Average Directional Index
    ADX_PERIODS = 3
    ADX_THRESHOLD = 20

    # Additional filter: wait an extra candle to confirm the direction of the trend
    CONFIRMATION_FILTER = False  # Boolean True/False

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA_PERIODS

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA_PERIODS)
    rsi_col_name = 'RSI' + str(RSI_PERIODS)
    adx_col_name = 'ADX' + str(ADX_PERIODS)

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__

    def get_strategy_text_details(self):
        if self.CONFIRMATION_FILTER:
            condition_filter = 'On'
        else:
            condition_filter = 'Off'
        details = f'EMA({self.EMA_PERIODS}), EMA_TOLERANCE({self.EMA_TOLERANCE}), RSI({self.RSI_PERIODS}), ' \
                  f'RSI_SIGNAL({self.RSI_MIN_SIGNAL_THRESHOLD}, {self.RSI_MAX_SIGNAL_THRESHOLD}), ' \
                  f'RSI_ENTRY({self.RSI_MIN_ENTRY_THRESHOLD}, {self.RSI_MAX_ENTRY_THRESHOLD}), ' \
                  f'ADX({self.ADX_PERIODS}), ADX_THRESHOLD({self.ADX_THRESHOLD}), Filter({condition_filter}), ' \
                  f'ENTRY_AS_MAKER({self.ENTRY_AS_MAKER})'
        return details

    # Step 1: Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        # Set proper data types
        self.df['open'] = self.df['open'].astype(float)
        self.df['high'] = self.df['high'].astype(float)
        self.df['low'] = self.df['low'].astype(float)
        self.df['close'] = self.df['close'].astype(float)
        self.df['volume'] = self.df['volume'].astype(float)

        # Trend Indicator. EMA-50
        self.df[self.ema_col_name] = talib.EMA(self.df['close'], timeperiod=self.EMA_PERIODS)

        # Momentum Indicator. RSI-3
        self.df[self.rsi_col_name] = talib.RSI(self.df['close'], timeperiod=self.RSI_PERIODS)

        # Volatility Indicator. ADX-5
        self.df[self.adx_col_name] = talib.ADX(self.df['high'], self.df['low'], self.df['close'],
                                               timeperiod=self.ADX_PERIODS)

    # Step 2: Add trade entry points
    # When we get a signal, we only enter the trade when the RSI exists the oversold/overbought area
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')

        # self.df['EMA_Tolerance'] = self.df[self.ema_col_name] * self.EMA_TOLERANCE
        self.df['EMA_LONG'] = self.df[self.ema_col_name] - self.df[self.ema_col_name] * self.EMA_TOLERANCE
        self.df['EMA_SHORT'] = self.df[self.ema_col_name] + self.df[self.ema_col_name] * self.EMA_TOLERANCE

        # Mark long signals
        self.df.loc[
            (
                (self.df['close'] > self.df['EMA_LONG']) &  # price > EMA
                (self.df[self.rsi_col_name] < self.RSI_MIN_SIGNAL_THRESHOLD) &  # RSI < RSI_MIN_THRESHOLD
                (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = 1

        # Mark short signals
        self.df.loc[
            (
                (self.df['close'] < self.df['EMA_SHORT']) &  # price < EMA-50
                (self.df[self.rsi_col_name] > self.RSI_MAX_SIGNAL_THRESHOLD) &  # RSI > RSI_MAX_THRESHOLD
                (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = -1

        # self.df.to_excel("out.xlsx", index=True, header=True)
        # print(self.df.to_string())

        self.df['signal_offset'] = None
        self.df['trade_status'] = None

        received_long_signal = False
        received_short_signal = False

        close_col_index = self.df.columns.get_loc("close")
        high_col_index = self.df.columns.get_loc("high")
        low_col_index = self.df.columns.get_loc("low")
        signal_offset_col_index = self.df.columns.get_loc("signal_offset")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        ema_col_index = self.df.columns.get_loc(self.ema_col_name)
        rsi_col_index = self.df.columns.get_loc(self.rsi_col_name)
        adx_col_index = self.df.columns.get_loc(self.adx_col_name)
        ema_long_col_index = self.df.columns.get_loc('EMA_LONG')
        ema_short_col_index = self.df.columns.get_loc('EMA_SHORT')

        # Index limit that can be used when the CONFIRMATION_FILTER is True
        i_max_condition_filter = len(self.df.index) - 1

        signal_offset = -1

        # Iterate over all data to identify the real trade entry points
        # Skip first 49 lines where EMA50 is null => offset
        offset = self.MIN_DATA_SIZE - 1
        for i, row in enumerate(self.df.iloc[offset:, :].itertuples(index=True), 0):
            i += offset
            # if we receive another signal while we are not done processing the prior one,
            # we ignore the new ones until the old one is processed
            if row.signal == 1 and not received_long_signal and not received_short_signal:
                received_long_signal = True
                signal_offset = i
            elif row.signal == -1 and not received_long_signal and not received_short_signal:
                received_short_signal = True
                signal_offset = i

            cur_high = self.df.iloc[i, high_col_index]
            cur_low = self.df.iloc[i, low_col_index]
            cur_close = self.df.iloc[i, close_col_index]
            cur_ema = self.df.iloc[i, ema_col_index]
            cur_rsi = self.df.iloc[i, rsi_col_index]
            cur_adx = self.df.iloc[i, adx_col_index]
            cur_ema_long = self.df.iloc[i, ema_long_col_index]
            cur_ema_short = self.df.iloc[i, ema_short_col_index]

            # If after receiving a long signal the EMA or ADX are no longer satisfied, cancel signal
            if received_long_signal and (cur_close < cur_ema_long or cur_adx < self.ADX_THRESHOLD):
                received_long_signal = False
                continue

            # If after receiving a short signal the EMA or ADX are no longer satisfied, cancel signal
            if received_short_signal and (cur_close > cur_ema_short or cur_adx < self.ADX_THRESHOLD):
                received_short_signal = False
                continue

            # RSI exiting oversold area. Long Entry
            if received_long_signal and cur_rsi > self.RSI_MIN_ENTRY_THRESHOLD:
                if self.CONFIRMATION_FILTER and i < i_max_condition_filter:
                    # Next candle must close higher than high of current
                    next_close = self.df.iloc[i + 1, close_col_index]
                    if next_close > cur_high:  # confirmation
                        self.df.iloc[i + 1, trade_status_col_index] = TradeStatuses.EnterLong
                        self.df.iloc[i + 1, signal_offset_col_index] = signal_offset - i
                    received_long_signal = False
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterLong
                    self.df.iloc[i, signal_offset_col_index] = signal_offset - i
                    received_long_signal = False
            # RSI exiting overbought area. Short Entry
            elif received_short_signal and cur_rsi < self.RSI_MAX_ENTRY_THRESHOLD:
                if self.CONFIRMATION_FILTER and i < i_max_condition_filter:
                    # Next candle must close lower than low of current
                    next_close = self.df.iloc[i + 1, close_col_index]
                    if next_close < cur_low:  # confirmation
                        self.df.iloc[i + 1, trade_status_col_index] = TradeStatuses.EnterShort
                        self.df.iloc[i + 1, signal_offset_col_index] = signal_offset - i
                    received_short_signal = False
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterShort
                    self.df.iloc[i, signal_offset_col_index] = signal_offset - i
                    received_short_signal = False

    # Check if there is a trade exit between current trade potential entry and signal that generated it.
    # If yes, that means this trade entry has been generated based on a signal that happened during another
    # trade and must be ignored
    def entry_is_valid(self, current_index):
        signal_offset_col_index = self.df.columns.get_loc("signal_offset")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        offset = self.df.iloc[current_index, signal_offset_col_index]
        exit_statuses = [TradeStatuses.ExitLong, TradeStatuses.ExitShort,
                         TradeStatuses.EnterExitLong, TradeStatuses.EnterExitShort]

        for i in range(current_index + offset, current_index, 1):
            if self.df.iloc[i, trade_status_col_index] in exit_statuses:
                # Erase invalid trade entry
                self.df.iloc[current_index, trade_status_col_index] = None
                self.df.iloc[current_index, signal_offset_col_index] = None
                return False
        return True

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for EMA
        self.df = self.df.dropna(subset=[self.ema_col_name])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
