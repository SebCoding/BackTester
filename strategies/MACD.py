import numpy as np
import talib

from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy


class MACD(BaseStrategy):

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

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA_PERIODS

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA_PERIODS)

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__

    # Step 1: Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        # Set proper data types
        self.df['open'] = self.df['open'].astype(float)
        self.df['high'] = self.df['high'].astype(float)
        self.df['low'] = self.df['low'].astype(float)
        self.df['close'] = self.df['close'].astype(float)
        self.df['volume'] = self.df['volume'].astype(float)

        # Keep only this list of columns, delete all other columns
        # final_table_columns = ['pair', 'interval', 'open', 'high', 'low', 'close']
        # self.df = self.df[self.df.columns.intersection(final_table_columns)]

        # MACD - Moving Average Convergence/Divergence
        macd, macdsignal, macdhist = talib.MACD(self.df['close'],
                                                fastperiod=self.MACD_FAST_PERIOD,
                                                slowperiod=self.MACD_SLOW_PERIOD,
                                                signalperiod=self.MACD_SIGNAL_PERIOD)
        self.df['MACD'] = macd
        self.df['MACDSIG'] = macdsignal

        # EMA - Exponential Moving Average 200
        self.df[self.ema_col_name] = talib.EMA(self.df['close'], timeperiod=self.EMA_PERIODS)

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
        # Enter long trade
        self.df.loc[
            (
                    (self.df['close'] > self.df[self.ema_col_name]) &  # price > ema200
                    (self.df['MACDSIG'] < 0) &  # macdsignal < 0
                    (self.df['cross'] == -1)  # macdsignal crossed and is now under macd
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[
            (
                    (self.df['close'] < self.df[self.ema_col_name]) &  # price < ema200
                    (self.df['MACDSIG'] > 0) &  # macdsignal > 0
                    (self.df['cross'] == 1)  # macdsignal crossed and is now over macd
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
