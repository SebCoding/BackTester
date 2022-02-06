import sys

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
    MA_CALCULATION_TYPE_VALUES = ['SMA', 'EMA', 'WMA', 'Linear']

    # Type of moving average used internally for the MACD calculation
    # Possible values: 'SMA', 'EMA', 'WMA', 'Linear'
    MA_TYPE = 'WMA'

    # Trend following momentum indicator:
    # MACD - Moving Average Convergence Divergence
    MACD_FAST = 2
    MACD_SLOW = 11

    # Bollinger Bands calculated on the MACD value
    BB_PERIODS = 40

    # Bollinger Bands Mult
    # Number of non-biased standard deviations from the mean
    BB_MULT = 2

    # Volatility indicator: ADX - Average Directional Index
    ADX = 3
    ADX_THRESHOLD = 0

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__

        # Slow MA needs to be calculated first to then calculate BB
        self.MIN_DATA_SIZE = self.MACD_SLOW + self.BB_PERIODS
        assert(self.MA_TYPE in self.MA_CALCULATION_TYPE_VALUES)

        self.up_arrow = u"\u2191"
        self.down_arrow = u"\u2193"

        self.decode_param_settings()

    def decode_param_settings(self):
        """
            Expected dictionary format:
            {
                "MA_TYPE": "SMA",
                "MACD_FAST": 2,
                "MACD_SLOW": 11,
                "BB_PERIODS": 40,
                "BB_MULT": 2,
                "ADX": 3,
                "ADX_THRESHOLD": 30
            }
        """
        valid_keys = ['MA_TYPE', 'MACD_FAST', 'MACD_SLOW', 'BB_PERIODS', 'BB_MULT', 'ADX', 'ADX_THRESHOLD']
        settings = self.params['StrategySettings']
        if settings:
            # Validate that all keys are valid
            for k in settings.keys():
                if k not in valid_keys:
                    print(f'Invalid key [{k}] in strategy settings dictionary.')
                    sys.exit(1)
            try:
                if settings['MA_TYPE']:
                    if settings['MA_TYPE'] not in self.MA_CALCULATION_TYPE_VALUES:
                        print(f"Invalid MA_TYPE: {settings['MA_TYPE']}")
                        raise ValueError
                    self.MA_TYPE = str(settings['MA_TYPE'])
                if settings['MACD_FAST']:
                    self.MACD_FAST = int(settings['MACD_FAST'])
                if settings['MACD_SLOW']:
                    self.MACD_SLOW = int(settings['MACD_SLOW'])
                if settings['BB_PERIODS']:
                    self.BB_PERIODS = int(settings['BB_PERIODS'])
                if settings['BB_MULT']:
                    self.BB_MULT = int(settings['BB_MULT'])
                if settings['ADX']:
                    self.ADX = int(settings['ADX'])
                if settings['ADX_THRESHOLD']:
                    self.ADX_THRESHOLD = int(settings['ADX_THRESHOLD'])
            except ValueError as e:
                print(f"Invalid value found in Strategy Settings Dictionary: {self.params['StrategySettings']}")
                raise e

    def get_strategy_text_details(self):
        details = f'MovAvg({self.MA_TYPE}), MACD(fast={self.MACD_FAST}, ' \
                  f'slow={self.MACD_SLOW}), BB(periods={self.BB_PERIODS}, mult={self.BB_MULT})'
        if self.ADX_THRESHOLD > 0:
            details += f', ADX(periods={self.ADX}, threshold={self.ADX_THRESHOLD})'
        details += f", Exit({self.params['Exit_Strategy']}), Entry_As_Maker({self.config['trades']['entry_as_maker']})"
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

        match self.MA_TYPE:
            case 'SMA':
                self.df['MA_Fast'] = talib.SMA(self.df['close'], timeperiod=self.MACD_FAST)
                self.df['MA_Slow'] = talib.SMA(self.df['close'], timeperiod=self.MACD_SLOW)
            case 'EMA':
                self.df['MA_Fast'] = talib.EMA(self.df['close'], timeperiod=self.MACD_FAST)
                self.df['MA_Slow'] = talib.EMA(self.df['close'], timeperiod=self.MACD_SLOW)
            case 'WMA':
                self.df['MA_Fast'] = talib.WMA(self.df['close'], timeperiod=self.MACD_FAST)
                self.df['MA_Slow'] = talib.WMA(self.df['close'], timeperiod=self.MACD_SLOW)
            case 'Linear':
                self.df['MA_Fast'] = talib.LINEARREG(self.df['close'], timeperiod=self.MACD_FAST)
                self.df['MA_Slow'] = talib.LINEARREG(self.df['close'], timeperiod=self.MACD_SLOW)

        # MACD
        self.df['MACD'] = self.df['MA_Fast'] - self.df['MA_Slow']

        # Volatility Indicator. ADX
        self.df['ADX'] = talib.ADX(self.df['high'], self.df['low'], self.df['close'], timeperiod=self.ADX)

        # Bollinger Bands
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
        self.df.loc[:, f'{self.up_arrow} BB Lower'] = 0
        self.df.loc[:, f'{self.up_arrow} BB Lower'] = self.df['OverLower'].diff()

        # If MACD is lower BB_Upper: 1 else 0
        self.df.loc[:, 'LowerUpper'] = 0
        self.df.loc[self.df['MACD'] < self.df['BB_Upper'], 'LowerUpper'] = 1

        # If MACD crosses over BB_Upper: -1
        # If MACD crosses under BB_Upper: 1
        self.df.loc[:, f'{self.down_arrow} BB Upper'] = 0
        self.df.loc[:, f'{self.down_arrow} BB Upper'] = self.df['LowerUpper'].diff()

    # Step 2: Add trade entry points
    # When we get a signal, we only enter the trade when the RSI exists the oversold/overbought area
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        self.df.loc[:, 'trade_status'] = None

        # Enter long trade
        self.df.loc[
            (
                (self.df[f'{self.up_arrow} BB Lower'] == 1) &
                (self.df['ADX'] > self.ADX_THRESHOLD)
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[
            (
                (self.df[f'{self.down_arrow} BB Upper'] == 1) &
                (self.df['ADX'] > self.ADX_THRESHOLD)
            ),
            'trade_status'] = TradeStatuses.EnterShort
        
    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df.loc[:, 'take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df.loc[:, 'stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        self.df.drop(['OverLower', 'LowerUpper', 'BB_Basis'], axis=1, inplace=True)

        # Remove rows with nulls entries for indicators
        self.df = self.df.dropna(subset=[f'{self.down_arrow} BB Upper'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))

