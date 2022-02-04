import numpy as np
import talib

from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy


class MACD_BB_Freeman(BaseStrategy):
    """
        Strategy MACD vs BB
        -------------------
        Strategy based on MACD and Bollinger Bands , where BBs are calculated from MACD signal.
        Strategy doesn't open at breakout of bands, but it waits for a pullback.
        Upper and lower bands are used as resistance and support.
        https://www.tradingview.com/v/muzLmNwb/

        MACD: Moving average convergence divergence (MACD) is a trend-following momentum indicator that shows
              the relationship between two moving averages of a security's price. The MACD is calculated by
              subtracting the 26-period exponential moving average (EMA) from the 12-period EMA.

        Bollinger Bands: Bollinger Bands are envelopes plotted at a standard deviation level above and below
                         a simple moving average of the price.
    """
    MA_CALCULATION_TYPE_VALID_VALUES = ['SMA', 'EMA', 'WMA', 'Linear']

    # Type of moving average used internally for the MACD calculation
    # Possible values: 'SMA', 'EMA', 'WMA', 'Linear'
    MA_CALCULATION_TYPE = 'WMA'

    # Trend following momentum indicator:
    # MACD - Moving Average Convergence Divergence
    MACD_FAST_PERIODS = 2
    MACD_SLOW_PERIODS = 11

    # Bollinger Bands calculated on the MACD value
    BB_PERIODS = 40

    # Bollinger Bands Mult
    # Number of non-biased standard deviations from the mean
    BB_MULT = 2.0

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__
        # Slow MA needs to be calculated first to then calculate BB
        self.MIN_DATA_SIZE = self.MACD_SLOW_PERIODS + self.BB_PERIODS
        assert(self.MA_CALCULATION_TYPE in self.MA_CALCULATION_TYPE_VALID_VALUES)

    def get_strategy_text_details(self):
        details = f'MA_CALC_TYPE({self.MA_CALCULATION_TYPE}), MACD_FAST({self.MACD_FAST_PERIODS}), ' \
                  f'MACD_SLOW({self.MACD_SLOW_PERIODS}), BB_PERIODS({self.BB_PERIODS}), BB_MULT({self.BB_MULT}), ' \
                  f'EXIT({self.params["Exit_Strategy"]})'
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

        match self.MA_CALCULATION_TYPE:
            case 'SMA':
                self.df['MA_Fast'] = talib.SMA(self.df['close'], timeperiod=self.MACD_FAST_PERIODS)
                self.df['MA_Slow'] = talib.SMA(self.df['close'], timeperiod=self.MACD_SLOW_PERIODS)
            case 'EMA':
                self.df['MA_Fast'] = talib.EMA(self.df['close'], timeperiod=self.MACD_FAST_PERIODS)
                self.df['MA_Slow'] = talib.EMA(self.df['close'], timeperiod=self.MACD_SLOW_PERIODS)
            case 'WMA':
                self.df['MA_Fast'] = talib.WMA(self.df['close'], timeperiod=self.MACD_FAST_PERIODS)
                self.df['MA_Slow'] = talib.WMA(self.df['close'], timeperiod=self.MACD_SLOW_PERIODS)
            case 'Linear':
                self.df['MA_Fast'] = talib.LINEARREG(self.df['close'], timeperiod=self.MACD_FAST_PERIODS)
                self.df['MA_Slow'] = talib.LINEARREG(self.df['close'], timeperiod=self.MACD_SLOW_PERIODS)

        self.df['MACD'] = self.df['MA_Fast'] - self.df['MA_Slow']

        self.df['BB_Upper'], self.df['BB_Basis'], self.df['BB_Lower'] = \
            talib.BBANDS(
                self.df['MACD'],
                timeperiod=self.BB_PERIODS,
                nbdevup=self.BB_MULT,  # Number of non-biased standard deviations from the mean
                nbdevdn=self.BB_MULT,  # Number of non-biased standard deviations from the mean
                matype=0  # Moving average type: simple moving average here
            )

        # Remove rows with null entries for BB because crossovers are invalid on row #1
        self.df = self.df.dropna(subset=['BB_Basis'])

        # Long Condition = ta.crossover(macd, lower)
        # Short Condition = ta.crossunder(macd, upper)

        # If MACD is over BB_Lower: 1 else 0
        self.df.loc[:, 'OverLower'] = 0
        self.df.loc[self.df['MACD'] > self.df['BB_Lower'], 'OverLower'] = 1

        # If MACD crosses over BB_Lower: 1
        # If MACD crosses under BB_Lower: -1
        self.df.loc[:, 'CrossOverLower'] = 0
        self.df.loc[:, 'CrossOverLower'] = self.df['OverLower'].diff()

        # If MACD is lower BB_Upper: 1 else 0
        self.df.loc[:, 'LowerUpper'] = 0
        self.df.loc[self.df['MACD'] < self.df['BB_Upper'], 'LowerUpper'] = 1

        # If MACD crosses over BB_Upper: -1
        # If MACD crosses under BB_Upper: 1
        self.df.loc[:, 'CrossUnderUpper'] = 0
        self.df.loc[:, 'CrossUnderUpper'] = self.df['LowerUpper'].diff()

    # Step 2: Add trade entry points
    # When we get a signal, we only enter the trade when the RSI exists the oversold/overbought area
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        # Enter long trade
        self.df.loc[(self.df['CrossOverLower'] == 1), 'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[(self.df['CrossUnderUpper'] == 1), 'trade_status'] = TradeStatuses.EnterShort
        
    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df.loc[:, 'take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df.loc[:, 'stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        self.df.drop(['OverLower', 'LowerUpper', 'BB_Basis'], axis=1, inplace=True)

        # Remove rows with nulls entries for indicators
        self.df = self.df.dropna(subset=['CrossUnderUpper'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))

