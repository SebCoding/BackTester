import sys

import numpy as np
import pandas as pd
import rapidjson
import talib

import utils
from enums.TradeStatus import TradeStatuses
from strategies.BaseStrategy import BaseStrategy
from datetime import timedelta


class UltimateScalper(BaseStrategy):
    """
        UltimateScalper Strategy
        ------------------------
        Inspired from these videos:
         - Best Crypto Scalping Strategy for the 5 Min Time Frame
           https://www.youtube.com/watch?v=V82HZbDO-rI
         - 83% WIN RATE 5 Minute ULTiMATE Scalping Trading Strategy!
           https://www.youtube.com/watch?v=XHhpCyIpJ50
    """

    settings = {
        # Trend indicator: EMA - Exponential Moving Average
        'EMA_Fast': 9,
        'EMA_Slow': 55,
        'EMA_Trend': 200,
        # Momentum indicator: RSI - Relative Strength Index
        'RSI': 4,
        'RSI_Low': 19,
        'RSI_High': 81,
        # ADX: Average Directional Index
        # Not initially in this strategy, but added as an optional parameter
        'ADX': 17,
        'ADX_Threshold': 24,  # set to 0 to disable ADX
        # Trend following momentum indicator:
        # MACD - Moving Average Convergence Divergence
        'MACD_Fast': 12,
        'MACD_Slow': 24,
        'MACD_Signal': 9,
        # Bollinger Bands around the MACD Histogram
        'BB_Length': 34,
        'BB_Mult': 1
    }

    # Cannot run Strategy on datasets less than this value
    MIN_DATA_SIZE = settings['EMA_Trend']

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__
        self.decode_param_settings()

        # The MACD Histogram is calculated based on the 1min timeframe
        self.df_1m = None
        self.get_1m_candle_data()

    def get_1m_candle_data(self):
        if self.config['database']['historical_data_stored_in_db']:
            self.df_1m = self.db_reader.get_candle_data(
                self.params['Pair'],
                self.params['From_Time'],
                self.params['To_Time'],
                '1m',
                include_prior=self.MIN_DATA_SIZE,
                verbose=True)
            if self.df_1m is None:
                raise Exception(f"No data returned by the database. Unable to backtest strategy.")
            elif len(self.df_1m) <= self.MIN_DATA_SIZE:
                print(
                    f'\nData rows = {len(self.df_1m)}, less than MIN_DATA_SIZE={self.MIN_DATA_SIZE}. '
                    f'Unable to backtest strategy.')
                raise Exception("Unable to Run Strategy on Data Set")
        else:
            self.df_1m = self.exchange.get_candle_data(
                self.params['Pair'],
                self.params['From_Time'],
                self.params['To_Time'],
                '1m',
                include_prior=self.MIN_DATA_SIZE,
                write_to_file=True,
                verbose=True)
            if self.df_1m is None:
                raise Exception(f"No data returned by {self.exchange.NAME}. Unable to backtest strategy.")
            elif len(self.df_1m) <= self.MIN_DATA_SIZE:
                print(
                    f'\nData rows = {len(self.df_1m)}, less than MIN_DATA_SIZE={self.MIN_DATA_SIZE}. '
                    f'Unable to backtest strategy.')
                raise Exception("Unable to Run Strategy on Data Set")

        # Set proper data types
        self.df_1m['open'] = self.df_1m['open'].astype(float)
        self.df_1m['high'] = self.df_1m['high'].astype(float)
        self.df_1m['low'] = self.df_1m['low'].astype(float)
        self.df_1m['close'] = self.df_1m['close'].astype(float)
        self.df_1m['volume'] = self.df_1m['volume'].astype(float)
        self.df_1m['end_time'] = self.df_1m.index + timedelta(minutes=1)

    def decode_param_settings(self):
        _settings = self.params['StrategySettings']
        if _settings:
            # Validate that all keys are valid
            for k in _settings.keys():
                if k not in self.settings.keys():
                    print(f'Invalid key [{k}] in strategy settings dictionary.')
                    sys.exit(1)

            # Parameters override default values hardcoded in the class
            for k in _settings.keys():
                self.settings[k] = _settings[k]

    def get_strategy_text_details(self):
        details = f"EMA({self.settings['EMA_Fast']}, {self.settings['EMA_Slow']}, {self.settings['EMA_Trend']}), " \
                  f"RSI({self.settings['RSI']}, {self.settings['RSI_Low']}, {self.settings['RSI_High']})"
        if self.settings['ADX_Threshold'] > 0:
            details += f", ADX({self.settings['ADX']}, {self.settings['ADX_Threshold']})"
        details += f", MACD({self.settings['MACD_Fast']}, {self.settings['MACD_Slow']}, {self.settings['MACD_Signal']})"
        details += f", BB({self.settings['BB_Length']}, {self.settings['BB_Mult']})"
        details += f', Entry_As_Maker({self.ENTRY_AS_MAKER}), Exit({self.params["Exit_Strategy"]})'
        return details

    # Step 1: Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        minutes = utils.convert_interval_to_min(self.params['Interval'])
        self.df['end_time'] = self.df.index + timedelta(minutes=minutes)

        # EMA: Exponential Moving Average
        self.df['EMA_Fast'] = talib.EMA(self.df['close'], timeperiod=self.settings['EMA_Fast'])
        self.df['EMA_Slow'] = talib.EMA(self.df['close'], timeperiod=self.settings['EMA_Slow'])
        self.df['EMA_Trend'] = talib.EMA(self.df['close'], timeperiod=self.settings['EMA_Trend'])

        # RSI: Momentum Indicator
        self.df['RSI'] = talib.RSI(self.df['close'], timeperiod=self.settings['RSI'])

        # ADX: Volatility Indicator
        self.df['ADX'] = talib.ADX(self.df['high'], self.df['low'], self.df['close'], timeperiod=self.settings['ADX'])

        # Drop rows with no EMA_Trend (usually first 200 rows for EMA200)
        self.df.dropna(subset=['EMA_Trend'], how='all', inplace=True)

        # Calculate MACD  and Bollinger bands on 1m timeframe
        macd, macdsignal, macdhist = talib.MACD(self.df_1m['close'],
                                                fastperiod=self.settings["MACD_Fast"],
                                                slowperiod=self.settings["MACD_Slow"],
                                                signalperiod=self.settings["MACD_Signal"])
        self.df_1m['MACDHist'] = macdhist
        self.df_1m['BB_Basis'] = talib.EMA(self.df_1m['MACDHist'], self.settings['BB_Length'])
        self.df_1m['BB_Mult'] = self.settings['BB_Mult']
        self.df_1m['BB_Dev'] = self.df_1m['BB_Mult'] * talib.STDDEV(self.df_1m['MACDHist'], self.settings['BB_Length'])
        self.df_1m['BB_Upper'] = self.df_1m['BB_Basis'] + self.df_1m['BB_Dev']
        self.df_1m['BB_Lower'] = self.df_1m['BB_Basis'] - self.df_1m['BB_Dev']

        # We use inner join here to trim rows at the beginning and the end where data is missing.
        # Candle data does not end at the same point for all intervals
        new_df = self.df.reset_index().merge(self.df_1m[['end_time', 'MACDHist', 'BB_Upper', 'BB_Lower']],
                                             on="end_time", how='inner').set_index('index')
        del new_df['end_time']

        # Do not delete these lines. Used for debugging.
        # print(self.df.tail(10).round(2).to_string() + '\n' + str(len(self.df)))
        # print(self.df_1m.tail(10).round(2).to_string() + '\n' + str(len(self.df_1m)))
        # print(new_df.tail(20).round(2).to_string() + '\n' + str(len(new_df)))
        # exit(1)
        self.df = new_df

    # Step 2: Identify the trade entries
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        self.df.loc[:, 'trade_status'] = None

        # Enter long trade
        self.df.loc[
            (
                    (self.df['EMA_Fast'] > self.df['EMA_Slow']) &
                    (self.df['EMA_Slow'] > self.df['EMA_Trend']) &
                    (self.df['RSI'] > self.settings['RSI_Low']) &
                    (self.df['ADX'] > self.settings['ADX_Threshold']) &
                    (self.df['MACDHist'] <= self.df['BB_Lower'])
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[
            (
                    (self.df['EMA_Fast'] < self.df['EMA_Slow']) &
                    (self.df['EMA_Slow'] < self.df['EMA_Trend']) &
                    (self.df['RSI'] < self.settings['RSI_High']) &
                    (self.df['ADX'] > self.settings['ADX_Threshold']) &
                    (self.df['MACDHist'] >= self.df['BB_Upper'])
            ),
            'trade_status'] = TradeStatuses.EnterShort

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for macd, macdsignal or ema200
        #self.df = self.df.dropna(subset=['EMA'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
