import datetime
import sys

import numpy as np
import talib

from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy


class ScalpEmaRsiAdx(BaseStrategy):
    """
        Implementation of the Scalping Strategy found here:
        https://www.youtube.com/watch?v=vBM0imYSzxI
        Using EMA RSI ADX Indicators
    """
    # Trend indicator: EMA - Exponential Moving Average
    EMA = 50

    # % over/under the EMA that can be tolerated to determine if the long/short trade can be placed
    # Value should be between 0 and 1
    EMA_TOLERANCE = 0.0

    # Momentum indicator: RSI - Relative Strength Index
    RSI = 2
    RSI_MIN_SIGNAL = 20
    RSI_MAX_SIGNAL = 80

    # Trade entry RSI thresholds (by default equal to RSI min/max thresholds)
    RSI_MIN_ENTRY = 30
    RSI_MAX_ENTRY = 70

    # Volatility indicator: ADX - Average Directional Index
    ADX = 3
    ADX_THRESHOLD = 30

    # Additional filter: wait an extra candle to confirm the direction of the trend
    CONFIRM_FILTER = False  # Boolean True/False

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA)
    rsi_col_name = 'RSI' + str(RSI)
    adx_col_name = 'ADX' + str(ADX)

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__
        self.decode_param_settings()

    def decode_param_settings(self):
        """
            Expected dictionary format:
            {
                "EMA": 50,
                "EMA_TOLERANCE": 0,
                "RSI": 2,
                "RSI_MIN_SIGNAL": 20,
                "RSI_MAX_SIGNAL": 80,
                "RSI_MIN_ENTRY": 30,
                "RSI_MAX_ENTRY": 70,
                "ADX": 2,
                "ADX_THRESHOLD": 30,
                "CONFIRM_FILTER": False
            }
        """
        valid_keys = ['EMA', 'EMA_TOLERANCE', 'RSI', 'RSI_MIN_SIGNAL', 'RSI_MAX_SIGNAL',
                      'RSI_MIN_ENTRY', 'RSI_MAX_ENTRY', 'ADX', 'ADX_THRESHOLD', 'CONFIRM_FILTER']
        settings = self.params['StrategySettings']
        if settings:
            # Validate that all keys are valid
            for k in settings.keys():
                if k not in valid_keys:
                    print(f'Invalid key [{k}] in strategy settings dictionary.')
                    sys.exit(1)
            try:
                if settings['EMA']:
                    self.EMA = int(settings['EMA'])
                if settings['EMA_TOLERANCE']:
                    self.EMA_TOLERANCE = float(settings['EMA_TOLERANCE'])
                if settings['RSI']:
                    self.RSI = int(settings['RSI'])
                if settings['RSI_MIN_SIGNAL']:
                    self.RSI_MIN_SIGNAL = int(settings['RSI_MIN_SIGNAL'])
                if settings['RSI_MAX_SIGNAL']:
                    self.RSI_MAX_SIGNAL = int(settings['RSI_MAX_SIGNAL'])
                if settings['RSI_MIN_ENTRY']:
                    self.RSI_MIN_ENTRY = int(settings['RSI_MIN_ENTRY'])
                if settings['RSI_MAX_ENTRY']:
                    self.RSI_MAX_ENTRY = int(settings['RSI_MAX_ENTRY'])
                if settings['ADX']:
                    self.ADX = int(settings['ADX'])
                if settings['ADX_THRESHOLD']:
                    self.ADX_THRESHOLD = int(settings['ADX_THRESHOLD'])
                if settings['CONFIRM_FILTER']:
                    self.CONFIRM_FILTER = bool(settings['CONFIRM_FILTER'])
            except ValueError as e:
                print(f"Invalid value found in Strategy Settings Dictionary: {self.params['StrategySettings']}")
                raise e

    def get_strategy_text_details(self):
        if self.CONFIRM_FILTER:
            condition_filter = 'On'
        else:
            condition_filter = 'Off'
        details = f'EMA({self.EMA}), EMA_TOLERANCE({self.EMA_TOLERANCE}), RSI({self.RSI}), ' \
                  f'RSI_SIGNAL({self.RSI_MIN_SIGNAL}, {self.RSI_MAX_SIGNAL}), ' \
                  f'RSI_ENTRY({self.RSI_MIN_ENTRY}, {self.RSI_MAX_ENTRY}), ' \
                  f'ADX({self.ADX}), ADX_THRESHOLD({self.ADX_THRESHOLD}), Filter({condition_filter}), ' \
                  f'Entry_As_Maker({self.ENTRY_AS_MAKER}), Exit({self.params["Exit_Strategy"]})'
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
        self.df[self.ema_col_name] = talib.EMA(self.df['close'], timeperiod=self.EMA)

        # Momentum Indicator. RSI-3
        self.df[self.rsi_col_name] = talib.RSI(self.df['close'], timeperiod=self.RSI)

        # Volatility Indicator. ADX-5
        self.df[self.adx_col_name] = talib.ADX(self.df['high'], self.df['low'], self.df['close'],
                                               timeperiod=self.ADX)

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
                    (self.df[self.rsi_col_name] < self.RSI_MIN_SIGNAL) &  # RSI < RSI_MIN_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = 1

        # Mark short signals
        self.df.loc[
            (
                    (self.df['close'] < self.df['EMA_SHORT']) &  # price < EMA-50
                    (self.df[self.rsi_col_name] > self.RSI_MAX_SIGNAL) &  # RSI > RSI_MAX_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = -1

        # self.df.to_excel("out.xlsx", index=True, header=True)
        # print(self.df.to_string())

        self.df['signal_offset'] = None
        self.df['trade_status'] = None

        close_col_index = self.df.columns.get_loc("close")
        high_col_index = self.df.columns.get_loc("high")
        low_col_index = self.df.columns.get_loc("low")
        signal_col_index = self.df.columns.get_loc("signal")
        signal_offset_col_index = self.df.columns.get_loc("signal_offset")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        rsi_col_index = self.df.columns.get_loc(self.rsi_col_name)

        # Index limit that can be used when the CONFIRMATION_FILTER is True
        i_max_condition_filter = len(self.df.index) - 1

        signal_offset = -1

        # Iterate over all data to identify the real trade entry points
        # Skip first 49 lines where EMA50 is null => offset
        offset = self.MIN_DATA_SIZE - 1
        for i, row in enumerate(self.df.iloc[offset:, :].itertuples(index=True), 0):
            i += offset

            cur_high = self.df.iloc[i, high_col_index]
            cur_low = self.df.iloc[i, low_col_index]
            cur_rsi = self.df.iloc[i, rsi_col_index]
            # signal of prior row
            prev_signal = self.df.iloc[i-1, signal_col_index]

            # RSI exiting oversold area. Long Entry
            if prev_signal == 1 and cur_rsi > self.RSI_MIN_ENTRY:
                if self.CONFIRM_FILTER and i < i_max_condition_filter:
                    # Next candle must close higher than high of current
                    next_close = self.df.iloc[i + 1, close_col_index]
                    if next_close > cur_high:  # confirmation
                        self.df.iloc[i + 1, trade_status_col_index] = TradeStatuses.EnterLong
                        self.df.iloc[i + 1, signal_offset_col_index] = signal_offset - i
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterLong
                    self.df.iloc[i, signal_offset_col_index] = signal_offset - i
            # RSI exiting overbought area. Short Entry
            elif prev_signal == -1 and cur_rsi < self.RSI_MAX_ENTRY:
                if self.CONFIRM_FILTER and i < i_max_condition_filter:
                    # Next candle must close lower than low of current
                    next_close = self.df.iloc[i + 1, close_col_index]
                    if next_close < cur_low:  # confirmation
                        self.df.iloc[i + 1, trade_status_col_index] = TradeStatuses.EnterShort
                        self.df.iloc[i + 1, signal_offset_col_index] = signal_offset - i
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterShort
                    self.df.iloc[i, signal_offset_col_index] = signal_offset - i

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
