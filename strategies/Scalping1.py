import pandas as pd
import talib

from stats import stats_utils
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
    RSI_MIN_THRESHOLD = 20
    RSI_MAX_THRESHOLD = 80

    # Trade entry RSI thresholds (by default equal to RSI min/max thresholds)
    RSI_MIN_THRESHOLD_ENTRY = 20
    RSI_MAX_THRESHOLD_ENTRY = 80

    # Volatility indicator: ADX - Average Directional Index
    ADX_PERIODS = 5
    ADX_THRESHOLD = 30

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
        # Mark long signals
        self.df.loc[
            (
                    (self.df['close'] > self.df[self.ema_col_name]) &  # price > EMA
                    (self.df[self.rsi_col_name] < self.RSI_MIN_THRESHOLD) &  # RSI < RSI_MIN_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = 1

        # Mark short signals
        self.df.loc[
            (
                    (self.df['close'] < self.df[self.ema_col_name]) &  # price < EMA-50
                    (self.df[self.rsi_col_name] > self.RSI_MAX_THRESHOLD) &  # RSI > RSI_MAX_THRESHOLD
                    (self.df[self.adx_col_name] > self.ADX_THRESHOLD)  # ADX > ADX_THRESHOLD
            ),
            'signal'] = -1

        self.df['trade_status'] = None
        received_long_signal = False
        received_short_signal = False

        close_col_index = self.df.columns.get_loc("close")
        high_col_index = self.df.columns.get_loc("high")
        low_col_index = self.df.columns.get_loc("low")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        rsi_col_index = self.df.columns.get_loc(self.rsi_col_name)

        # Index limit that can be used when the CONFIRMATION_FILTER is True
        i_max_condition_filter = len(self.df.index) - 1

        # Iterate over all data to identify the real trade entry points
        for i, row in enumerate(self.df.itertuples(index=True), 0):
            # if we receive another signal while we are not done processing the prior one,
            # we ignore the new ones until the old one is processed
            if row.signal == 1 and not received_long_signal and not received_short_signal:
                received_long_signal = True
            elif row.signal == -1 and not received_long_signal and not received_short_signal:
                received_short_signal = True

            # RSI exiting oversold area. Long Entry
            if received_long_signal and self.df.iloc[i, rsi_col_index] > self.RSI_MIN_THRESHOLD_ENTRY:
                if self.CONFIRMATION_FILTER and i < i_max_condition_filter:
                    # Next candle must close higher than high of current
                    next_close = self.df.iloc[i+1, close_col_index]
                    cur_high = self.df.iloc[i, high_col_index]
                    if next_close > cur_high:  # confirmation
                        self.df.iloc[i+1, trade_status_col_index] = TradeStatuses.EnterLong
                    received_long_signal = False
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterLong
                    received_long_signal = False
            # RSI exiting overbought area. Short Entry
            elif received_short_signal and self.df.iloc[i, rsi_col_index] < self.RSI_MAX_THRESHOLD_ENTRY:
                if self.CONFIRMATION_FILTER and i < i_max_condition_filter:
                    # Next candle must close lower than low of current
                    next_close = self.df.iloc[i+1, close_col_index]
                    cur_low = self.df.iloc[i, low_col_index]
                    if next_close < cur_low:  # confirmation
                        self.df.iloc[i+1, trade_status_col_index] = TradeStatuses.EnterShort
                    received_long_signal = False
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterShort
                    received_short_signal = False

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for EMA
        self.df = self.df.dropna(subset=[self.ema_col_name])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
