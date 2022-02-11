import sys

import numpy as np
import talib

from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy


class MACD(BaseStrategy):

    # Trend indicator: EMA - Exponential Moving Average
    EMA = 200

    # Trend following momentum indicator:
    # MACD - Moving Average Convergence Divergence
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9

    # ADX: Average Directional Index
    # Not initially in this strategy, but added as an optional parameter
    ADX = 14
    ADX_THRESHOLD = 0  # set to 0 to disable ADX

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA)
    adx_col_name = 'ADX' + str(ADX)

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__
        self.decode_param_settings()

    def decode_param_settings(self):
        """
            Expected dictionary format: {"EMA": 200, "MACD_FAST": 12, "MACD_SLOW": 26, "MACD_SIGNAL": 9,
                                         "ADX": 14, "ADX_THRESHOLD": 0}
        """
        valid_keys = ['EMA', 'MACD_FAST', 'MACD_SLOW', 'MACD_SIGNAL', 'ADX', 'ADX_THRESHOLD']
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
                if settings['MACD_FAST']:
                    self.MACD_FAST = int(settings['MACD_FAST'])
                if settings['MACD_SLOW']:
                    self.MACD_SLOW = int(settings['MACD_SLOW'])
                if settings['MACD_SIGNAL']:
                    self.MACD_SIGNAL = int(settings['MACD_SIGNAL'])
                if 'ADX' in settings.keys() and settings['ADX']:
                    self.ADX = int(settings['ADX'])
                if 'ADX_THRESHOLD' in settings.keys() and settings['ADX_THRESHOLD']:
                    self.ADX_THRESHOLD = int(settings['ADX_THRESHOLD'])
            except ValueError as e:
                print(f"Invalid value found in Strategy Settings Dictionary: {self.params['StrategySettings']}")
                raise e

    def get_strategy_text_details(self):
        details = f'EMA({self.EMA}), MACD(fast={self.MACD_FAST}, ' \
                  f'slow={self.MACD_SLOW}, signal={self.MACD_SIGNAL}), '
        if self.ADX_THRESHOLD > 0:
            details += f'ADX({self.ADX}, {self.ADX_THRESHOLD}), '
        details += f'Entry_As_Maker({self.ENTRY_AS_MAKER}), Exit({self.params["Exit_Strategy"]})'

        return details

    # Step 1: Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        # Keep only this list of columns, delete all other columns
        # final_table_columns = ['pair', 'interval', 'open', 'high', 'low', 'close']
        # self.df = self.df[self.df.columns.intersection(final_table_columns)]

        # MACD - Moving Average Convergence/Divergence
        macd, macdsignal, macdhist = talib.MACD(self.df['close'],
                                                fastperiod=self.MACD_FAST,
                                                slowperiod=self.MACD_SLOW,
                                                signalperiod=self.MACD_SIGNAL)
        self.df['MACD'] = macd
        self.df['MACDSIG'] = macdsignal

        # EMA - Exponential Moving Average 200
        self.df[self.ema_col_name] = talib.EMA(self.df['close'], timeperiod=self.EMA)

        # ADX
        self.df[self.adx_col_name] = \
            talib.ADX(self.df['high'], self.df['low'], self.df['close'], timeperiod=self.ADX)

        # Identify the trend
        # self.df.loc[self.df['close'] > self.df[self.ema_col_name], 'trend'] = 'Up'
        # self.df.loc[self.df['close'] < self.df[self.ema_col_name], 'trend'] = 'Down'

        # macdsignal over macd then 1, under 0
        self.df['O/U'] = np.where(self.df['MACDSIG'] >= self.df['MACD'], 1, 0)

        # macdsignal crosses macd
        self.df['cross'] = self.df['O/U'].diff()

    # Step 2: Identify the trade entries
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        self.df.loc[:, 'trade_status'] = None

        # Enter long trade
        self.df.loc[
            (
                    (self.df['close'] > self.df[self.ema_col_name]) &  # price > ema200
                    (self.df['MACDSIG'] < 0) &  # macdsignal < 0
                    (self.df['cross'] == -1) & # macdsignal crossed and is now under macd
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[
            (
                    (self.df['close'] < self.df[self.ema_col_name]) &  # price < ema200
                    (self.df['MACDSIG'] > 0) &  # macdsignal > 0
                    (self.df['cross'] == 1) & # macdsignal crossed and is now over macd
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'trade_status'] = TradeStatuses.EnterShort

        # We enter the trade on the next candle after the signal candle has completed
        # self.df['trade_status'] = self.df['trade_status'].shift(1)

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for macd, macdsignal or ema200
        self.df = self.df.dropna(subset=[self.ema_col_name])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))

        if self.ADX_THRESHOLD <= 0:
            del self.df[self.adx_col_name]
