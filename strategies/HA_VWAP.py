import sys

import numpy as np
import pandas as pd
import rapidjson
import talib

import utils
from enums.ExitType import ExitType
from enums.TradeStatus import TradeStatuses
from enums.TradeType import TradeType
from strategies.BaseStrategy import BaseStrategy
from datetime import timedelta


class HA_VWAP(BaseStrategy):
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
        # Set to 0 to disable
        'EMA': 200,
        'DistVWAP_PCT': 0.05,
        'NB_SIGNALS': 2  # Values must be: 1, 2, 3, 4
    }

    # Cannot run Strategy on datasets less than this value
    MIN_DATA_SIZE = settings['EMA']

    def __init__(self, params):
        super().__init__(params)
        self.NAME = self.__class__.__name__
        self.decode_param_settings()
        # Used within decorator to access previous row when calculating Heikin Ashi
        self.prev_row_ha = {}

        if self.settings['NB_SIGNALS'] not in [1, 2, 3, 4]:
            print(f"Invalid value: {self.settings['NB_SIGNALS']} for NB_SIGNALS.")
            sys.exit(1)

    def validate_exit_strategy(self):
        if self.params["Exit_Strategy"] != 'VWAP_Touch':
            print(f'Exit strategy ({self.params["Exit_Strategy"]}) not supported by {self.params["Strategy"]}.')
            sys.exit(1)

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
        details = details = f"EMA({self.settings['EMA']}), DistVWAP_PCT({self.settings['DistVWAP_PCT']}), "
        details += f'Entry_As_Maker({self.ENTRY_AS_MAKER}), Exit({self.params["Exit_Strategy"]})'
        return details

    def ha_decorator(func):
        def wrapper(self, curr_row):
            ha_open, ha_close = func(self, curr_row, self.prev_row)
            self.prev_row.update(curr_row)
            self.prev_row['HA_Open'], self.prev_row['HA_Close'] = ha_open, ha_close
            return ha_open, ha_close
        return wrapper

    @ha_decorator
    def heikin_ashi(self, curr_row, prev_row):
        # HA_Close = (Open0 + High0 + Low0 + Close0)/4
        ha_close = (curr_row['open'] + curr_row['high'] + curr_row['low'] + curr_row['close']) / 4
        # HA_Open = (HAOpen(-1) + HAClose(-1))/2
        if not prev_row or len(prev_row) == 0:
            ha_open = (curr_row['open'] + curr_row['close']) / 2
        else:
            ha_open = (prev_row['HA_Open'] + prev_row['HA_Close']) / 2
        return ha_open, ha_close

    @staticmethod
    def vwap(df):
        volume = df['volume']
        price = (df['high'] + df['low'] + df['close']) / 3
        return df.assign(VWAP=(price * volume).cumsum() / volume.cumsum())

    # Step 1: Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        # Calculate Heikin Ashi
        self.df[['HA_Open', 'HA_Close']] = self.df.apply(self.heikin_ashi, axis=1).apply(pd.Series)

        # EMA: Exponential Moving Average
        self.df['EMA'] = talib.EMA(self.df['close'], timeperiod=self.settings['EMA'])

        # Drop rows with no EMA (usually first 200 rows for EMA200)
        self.df.dropna(subset=['EMA'], how='all', inplace=True)

        # VWAP: Volume-Weighted Average Price
        self.df = self.df.groupby(self.df.index.date, group_keys=False).apply(self.vwap)

        # Calculate distance from VWAP
        self.df['DistVWAP'] = abs(self.df['close'] - self.df['VWAP']) / self.df['close'] * 100

        # This does not work. VWAP has to be reset each day.
        # self.df['VWAP2'] = \
        #     np.cumsum(self.df['volume'] * (self.df['high']+self.df['low']+self.df['close'])/3) / np.cumsum(self.df['volume'])

        self.df.loc[:, 'signal'] = 0
        if self.settings['EMA'] != 0:
            # Long signal
            self.df.loc[
                (
                    (self.df['HA_Close'] < self.df['VWAP']) &
                    (self.df['HA_Close'] > self.df['EMA']) &
                    (self.df['DistVWAP'] >= self.settings['DistVWAP_PCT']) &
                    (self.df['HA_Close'] > self.df['HA_Close'].shift(1)) &
                    (self.df['HA_Close'] > self.df['HA_Open'].shift(1))
                ),
                'signal'] = 1
            # Short Signal
            self.df.loc[
                (
                    (self.df['HA_Close'] > self.df['VWAP']) &
                    (self.df['HA_Close'] < self.df['EMA']) &
                    (self.df['DistVWAP'] >= self.settings['DistVWAP_PCT']) &
                    (self.df['HA_Close'] < self.df['HA_Close'].shift(1)) &
                    (self.df['HA_Close'] < self.df['HA_Open'].shift(1))
                ),
                'signal'] = -1
        else:
            # Long Signal
            self.df.loc[
                (
                    (self.df['HA_Close'] < self.df['VWAP']) &
                    (self.df['DistVWAP'] >= self.settings['DistVWAP_PCT']) &
                    (self.df['HA_Close'] > self.df['HA_Close'].shift(1)) &
                    (self.df['HA_Close'] > self.df['HA_Open'].shift(1))
                ),
                'signal'] = 1
            # Short signal
            self.df.loc[
                (
                    (self.df['HA_Close'] > self.df['VWAP']) &
                    (self.df['DistVWAP'] >= self.settings['DistVWAP_PCT']) &
                    (self.df['HA_Close'] < self.df['HA_Close'].shift(1)) &
                    (self.df['HA_Close'] < self.df['HA_Open'].shift(1))
                ),
                'signal'] = -1

        # Do not delete these lines. Used for debugging.
        # print(self.df.tail(1000).round(2).to_string() + '\nRows: ' + str(len(self.df)))
        # exit(1)

    # Step 2: Identify the trade entries
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        self.df.loc[:, 'trade_status'] = None

        if self.settings['NB_SIGNALS'] == 1:
            self.df.loc[(self.df['signal'] == 1), 'trade_status'] = TradeStatuses.EnterLong
            self.df.loc[(self.df['signal'] == -1), 'trade_status'] = TradeStatuses.EnterShort
        elif self.settings['NB_SIGNALS'] == 2:
            self.df.loc[
                (
                    (self.df['signal'] == 1) &
                    (self.df['signal'].shift(1) == 1)
                ),
                'trade_status'] = TradeStatuses.EnterLong
            self.df.loc[
                (
                    (self.df['signal'] == -1) &
                    (self.df['signal'].shift(1) == -1)
                ),
                'trade_status'] = TradeStatuses.EnterShort
        elif self.settings['NB_SIGNALS'] == 3:
            self.df.loc[
                (
                    (self.df['signal'] == 1) &
                    (self.df['signal'].shift(1) == 1) &
                    (self.df['signal'].shift(2) == 1)
                ),
                'trade_status'] = TradeStatuses.EnterLong
            self.df.loc[
                (
                    (self.df['signal'] == -1) &
                    (self.df['signal'].shift(1) == -1) &
                    (self.df['signal'].shift(2) == -1)
                ),
                'trade_status'] = TradeStatuses.EnterShort
        elif self.settings['NB_SIGNALS'] == 4:
            self.df.loc[
                (
                    (self.df['signal'] == 1) &
                    (self.df['signal'].shift(1) == 1) &
                    (self.df['signal'].shift(2) == 1) &
                    (self.df['signal'].shift(3) == 1)
                ),
                'trade_status'] = TradeStatuses.EnterLong
            self.df.loc[
                (
                    (self.df['signal'] == -1) &
                    (self.df['signal'].shift(1) == -1) &
                    (self.df['signal'].shift(2) == -1) &
                    (self.df['signal'].shift(3) == -11)
                ),
                'trade_status'] = TradeStatuses.EnterShort

        # Do not delete these lines. Used for debugging.
        # print(self.df.tail(1000).round(2).to_string() + '\nRows: ' + str(len(self.df)))
        # exit(1)

        # Step 3: Mark start, ongoing and end of trades, as well as calculate statistics

    def process_trades(self):
        print(self.get_strategy_text_details())
        print(f"Processing trades using the [{self.NAME}, {self.params['Exit_Strategy']}] strategy.\n...")

        self.df.loc[:, 'wallet'] = 0.0
        self.df.loc[:, 'staked_amount'] = 0.0
        self.df.loc[:, 'entry_price'] = 0.0
        self.df.loc[:, 'take_profit'] = 0.0
        self.df.loc[:, 'stop_loss'] = 0.0
        self.df.loc[:, 'win'] = 0.0
        self.df.loc[:, 'loss'] = 0.0
        self.df.loc[:, 'entry_fee'] = 0.0
        self.df.loc[:, 'exit_fee'] = 0.0

        if self.params['Exit_Strategy'] == 'VWAP_Touch':
            self.prev_row = {}
            self.df[['trade_status', 'entry_price', 'take_profit', 'stop_loss', 'wallet',
                     'staked_amount', 'win', 'loss', 'entry_fee', 'exit_fee']] = \
                self.df.apply(self.get_all_trade_details_vwap_touch, axis=1).apply(pd.Series)
        else:
            print(f'Unimplemented exit strategy.')
            sys.exit(1)

        # Statistics
        self.stats.nb_wins = self.df['win'].astype(bool).sum(axis=0)
        self.stats.nb_losses = self.df['loss'].astype(bool).sum(axis=0)
        self.stats.total_wins = self.df['win'].sum()
        self.stats.total_losses = self.df['loss'].sum()
        self.stats.total_fees_paid = self.df['entry_fee'].sum() + self.df['exit_fee'].sum()

        # print()  # Jump to next line
        return self.df

    def get_all_trade_details_decorator(func):
        def wrapper(self, curr_row):
            v1, v2, v3, v4, v5, v6, v7, v8, v9, v10 = func(self, curr_row, self.prev_row)
            self.prev_row.update(curr_row)
            self.prev_row['trade_status'], self.prev_row['entry_price'], self.prev_row['take_profit'], \
            self.prev_row['stop_loss'], self.prev_row['wallet'], self.prev_row['staked_amount'], self.prev_row['win'], \
            self.prev_row['loss'], self.prev_row['entry_fee'], self.prev_row['exit_fee'] \
                = v1, v2, v3, v4, v5, v6, v7, v8, v9, v10
            return v1, v2, v3, v4, v5, v6, v7, v8, v9, v10
        return wrapper

    @get_all_trade_details_decorator
    def get_all_trade_details_vwap_touch(self, curr_row, prev_row):
        """
            ['trade_status', 'entry_price', 'take_profit', 'stop_loss', 'wallet',
            'staked_amount', 'win', 'loss', 'entry_fee', 'exit_fee']
        """
        if not prev_row or len(prev_row) == 0:
            return None, 0, 0, 0, float(self.params['Initial_Capital']), 0, 0, 0, 0, 0

        # Not in a trade
        elif (prev_row['trade_status'] is None or prev_row['trade_status'] in [TradeStatuses.ExitLong,
                                                                               TradeStatuses.ExitShort]) \
                and curr_row['trade_status'] is None:
            return None, 0, 0, 0, prev_row['wallet'], 0, 0, 0, 0, 0

        # Enter Long
        elif (prev_row['trade_status'] is None or prev_row['trade_status'] in [TradeStatuses.ExitLong,
                                                                               TradeStatuses.ExitShort]) \
                and curr_row['trade_status'] in [TradeStatuses.EnterLong]:
            take_profit = curr_row['close'] + (self.TP_PCT * curr_row['close'])
            stop_loss = curr_row['close'] - (self.SL_PCT * curr_row['close'])
            staked_amount, entry_fee = self.get_stake_and_entry_fee(prev_row['wallet'])
            if entry_fee < 0:  # Negative fee = credit/refund
                # remove staked amount from balance and add fee credit/refund
                account_balance = prev_row['wallet'] - staked_amount - entry_fee
            else:
                account_balance = prev_row['wallet'] - (staked_amount + entry_fee)
            return curr_row['trade_status'], curr_row[
                'close'], take_profit, stop_loss, account_balance, staked_amount, 0, 0, entry_fee, 0

        # Enter Short
        elif (prev_row['trade_status'] is None or prev_row['trade_status'] in [TradeStatuses.ExitLong,
                                                                               TradeStatuses.ExitShort]) \
                and curr_row['trade_status'] in [TradeStatuses.EnterShort]:
            take_profit = curr_row['close'] - (self.TP_PCT * curr_row['close'])
            stop_loss = curr_row['close'] + (self.SL_PCT * curr_row['close'])
            staked_amount, entry_fee = self.get_stake_and_entry_fee(prev_row['wallet'])
            if entry_fee < 0:  # Negative fee = credit/refund
                # remove staked amount from balance and add fee credit/refund
                account_balance = prev_row['wallet'] - staked_amount - entry_fee
            else:
                account_balance = prev_row['wallet'] - (staked_amount + entry_fee)
            return curr_row['trade_status'], curr_row[
                'close'], take_profit, stop_loss, account_balance, staked_amount, 0, 0, entry_fee, 0

        # Previous row in a long
        elif prev_row['trade_status'] in [TradeStatuses.EnterLong, TradeStatuses.Long]:
            exit_type = self.get_exit_type(TradeType.Long, curr_row['open'], curr_row['high'], curr_row['low'],
                                           prev_row['take_profit'], prev_row['stop_loss'])
            # Exit by stop loss
            if exit_type == ExitType.StopLoss:
                loss = prev_row['staked_amount'] * self.SL_PCT * -1
                exit_fee = self.get_stop_loss_fee(prev_row['staked_amount'] - loss)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                return TradeStatuses.ExitLong, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, 0, loss, 0, exit_fee
            # Exit by take profit
            elif exit_type == ExitType.TakeProfit:
                win = prev_row['staked_amount'] * self.TP_PCT
                exit_fee = self.get_take_profit_fee(prev_row['staked_amount'] + win)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                return TradeStatuses.ExitLong, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, win, 0, 0, exit_fee
            # Exit by crossing VWAP (loss)
            elif curr_row['close'] >= curr_row['VWAP'] and curr_row['close'] <= prev_row['entry_price']:
                loss = (curr_row['close'] - prev_row['entry_price']) / prev_row['entry_price'] * prev_row[
                    'staked_amount']
                exit_fee = self.get_stop_loss_fee(prev_row['staked_amount'] - loss)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                return TradeStatuses.ExitLong, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, 0, loss, 0, exit_fee
            # Exit by crossing VWAP (win)
            elif curr_row['close'] >= curr_row['VWAP'] and curr_row['close'] >= prev_row['entry_price']:
                win = (curr_row['close'] - prev_row['entry_price']) / prev_row['entry_price'] * prev_row[
                    'staked_amount']
                exit_fee = self.get_take_profit_fee(prev_row['staked_amount'] + win)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                return TradeStatuses.ExitLong, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, win, 0, 0, exit_fee
            # Continue long, no event
            else:
                return TradeStatuses.Long, prev_row['entry_price'], prev_row['take_profit'], prev_row['stop_loss'], \
                       prev_row['wallet'], prev_row['staked_amount'], 0, 0, 0, 0

        # Previous row in a short
        elif prev_row['trade_status'] in [TradeStatuses.EnterShort, TradeStatuses.Short]:
            exit_type = self.get_exit_type(TradeType.Short, curr_row['open'], curr_row['high'], curr_row['low'],
                                           prev_row['take_profit'], prev_row['stop_loss'])
            # Exit by stop loss
            if exit_type == ExitType.StopLoss:
                loss = prev_row['staked_amount'] * self.SL_PCT * -1
                exit_fee = self.get_stop_loss_fee(prev_row['staked_amount'] + loss)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                return TradeStatuses.ExitShort, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, 0, loss, 0, exit_fee
            # Exit by take profit
            elif exit_type == ExitType.TakeProfit:
                win = prev_row['staked_amount'] * self.TP_PCT
                exit_fee = self.get_take_profit_fee(prev_row['staked_amount'] + win)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                return TradeStatuses.ExitShort, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, win, 0, 0, exit_fee
            # Exit by crossing VWAP (loss)
            elif curr_row['close'] <= curr_row['VWAP'] and curr_row['close'] >= prev_row['entry_price']:
                loss = (prev_row['entry_price'] - curr_row['close']) / prev_row['entry_price'] * prev_row[
                    'staked_amount']
                exit_fee = self.get_stop_loss_fee(prev_row['staked_amount'] + loss)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                return TradeStatuses.ExitShort, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, 0, loss, 0, exit_fee
            # Exit by crossing VWAP (win)
            elif curr_row['close'] <= curr_row['VWAP'] and curr_row['close'] <= prev_row['entry_price']:
                win = (prev_row['entry_price'] - curr_row['close']) / prev_row['entry_price'] * prev_row[
                    'staked_amount']
                exit_fee = self.get_take_profit_fee(prev_row['staked_amount'] + win)
                account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                return TradeStatuses.ExitShort, prev_row['entry_price'], prev_row['take_profit'], prev_row[
                    'stop_loss'], account_balance, 0, win, 0, 0, exit_fee
            # Continue long, no event
            else:
                return TradeStatuses.Short, prev_row['entry_price'], prev_row['take_profit'], prev_row['stop_loss'], \
                       prev_row['wallet'], prev_row['staked_amount'], 0, 0, 0, 0
        else:
            print(f' *** unhandled case ***')
            raise Exception

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for macd, macdsignal or ema200
        #self.df = self.df.dropna(subset=['EMA'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
