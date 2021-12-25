import pandas as pd
import talib

import stats
import utils
from enums.TradeStatus import TradeStatuses
from strategies.IStrategy import IStrategy


class Scalping1(IStrategy):
    NAME = 'Scalping1'

    # Ratio of the total account balance allowed to be traded.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 1.0

    # Trend indicator: EMA - Exponential Moving Average
    EMA_PERIODS = 50

    # Momentum indicator: RSI - Relative Strength Index
    RSI_PERIODS = 3
    RSI_MIN_LIMIT = 20
    RSI_MAX_LIMIT = 80

    # Trade entry RSI limits (by default equal to RSI min/max limits)
    RSI_MIN_LIMIT_ENTRY = 20
    RSI_MAX_LIMIT_ENTRY = 80

    # Volatility indicator: ADX - Average Directional Index
    ADX_PERIODS = 5
    ADX_THRESHOLD = 30

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA_PERIODS

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA_PERIODS)
    rsi_col_name = 'RSI' + str(RSI_PERIODS)
    adx_col_name = 'ADX' + str(ADX_PERIODS)

    def __init__(self, exchange, params, df):
        super().__init__(exchange, params, df)

    def mark_trade_entry_signals(self):
        # Mark long entries
        self.df.loc[
            (
                    (self.df['close'] > self.df[self.ema_col_name]) &  # price > EMA
                    (self.df[self.rsi_col_name] < self.RSI_MIN_LIMIT) &  # RSI < RSI_MIN_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = 1

        # Mark short entries
        self.df.loc[
            (
                    (self.df['close'] < self.df[self.ema_col_name]) &  # price < EMA-50
                    (self.df[self.rsi_col_name] > self.RSI_MAX_LIMIT) &  # RSI > RSI_MAX_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = -1

    # When we get a signal we only enter the trade when the RSI exists the oversold/overbought area
    def find_trade_entry_points(self):

        self.df['trade_status'] = None
        received_long_signal = False
        received_short_signal = False
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        rsi_col_index = self.df.columns.get_loc(self.rsi_col_name)

        # Iterate over all data to identify the real trade entry points
        for i, row in enumerate(self.df.itertuples(index=True), 0):
            # if we receive another signal while we are not done processing the prior one,
            # we ignore the new ones until the old one is processed
            if row.signal == 1 and not received_long_signal and not received_short_signal:
                received_long_signal = True
            elif row.signal == -1 and not received_long_signal and not received_short_signal:
                received_short_signal = True

            # RSI exiting oversold area
            if received_long_signal and self.df.iloc[i, rsi_col_index] > self.RSI_MIN_LIMIT_ENTRY:
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterLong
                received_long_signal = False
            # RSI exiting overbought area
            elif received_short_signal and self.df.iloc[i, rsi_col_index] < self.RSI_MAX_LIMIT_ENTRY:
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterShort
                received_short_signal = False

    # Calculate indicator values required to determine long/short signals
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

        # Identify the trend
        # self.df.loc[self.df['close'] > self.df['EMA-50'], 'trend'] = 'Up'
        # self.df.loc[self.df['close'] < self.df['EMA-50'], 'trend'] = 'Down'

        # Mark long/short signals
        self.mark_trade_entry_signals()

        # When we get a signal we only enter the trade when the RSI exists the oversold/overbought area
        self.find_trade_entry_points()

        # Add and Initialize new columns
        self.df['wallet'] = 0.0
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        return self.df

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for EMA
        self.df = self.df.dropna(subset=[self.ema_col_name])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
