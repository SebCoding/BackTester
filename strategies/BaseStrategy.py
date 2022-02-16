
import math
import sys
from abc import ABC, abstractmethod
import datetime as dt
from datetime import datetime

import numpy as np
import pandas as pd
import rapidjson

import constants
from Configuration import Configuration
from database.DbDataReader import DbDataReader
from enums.ExitType import ExitType
from enums.TradeType import TradeType
from exchanges.ExchangeCCXT import ExchangeCCXT
from stats import stats_utils
import utils
from enums.TradeStatus import TradeStatuses
from stats.Statistics import Statistics

# Do not remove these imports even if PyCharm says they're unused
from exchanges.Binance import Binance
from exchanges.Bybit import Bybit


class BaseStrategy(ABC):
    """
        Base Abstract Strategy Class.
        All strategies inherit from this class and must implement the abstract methods of this class

        The following logic is used to emulate order fills:
            - If the bar’s high is closer to bar’s open than the bar’s low,
              we assume that intrabar price was moving this way: open → high → low → close.
            - If the bar’s low is closer to bar’s open than the bar’s high,
              we assume that intrabar price was moving this way: open → low → high → close.
    """
    NAME = 'abstract'

    # Used to output on console a dot for each trade processed.
    PROGRESS_COUNTER_MAX = 100

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = 0

    def __init__(self, params):
        self.config = Configuration.get_config()
        self.df = None
        self.params = params
        self.progress_counter = 0
        self.TRADABLE_BALANCE_RATIO = self.config['trades']['tradable_ratio']
        self.ENTRY_AS_MAKER = self.config['trades']['entry_as_maker']
        self.TP_PCT = self.params['Take_Profit_PCT'] / 100
        self.SL_PCT = self.params['Stop_Loss_PCT'] / 100
        # self.exchange = globals()[params['Exchange']]()
        self.exchange = ExchangeCCXT(params['Exchange'].lower(), params['Pair'])
        self.MAKER_FEE_PCT = self.exchange.get_maker_fee(params['Pair'])
        self.TAKER_FEE_PCT = self.exchange.get_taker_fee(params['Pair'])
        self.stats = Statistics()
        if self.config['database']['historical_data_stored_in_db']:
            self.db_reader = DbDataReader(self.exchange.NAME)
            self.db_engine = self.db_reader.engine
        self.validate_exit_strategy()
        # Used within decorators to access previous row when processing trades
        self.prev_row = {}

    def run(self):
        self.get_candle_data()  # Step 0
        self.add_indicators_and_signals()  # Step1
        self.add_trade_entry_points()  # Step2
        self.process_trades()  # Step3
        self.validate_trades()  # Step 4
        self.save_trades_to_file()  # Step 5
        self.finalize_stats()  # Step 6

    # To be redefined on subclasses
    def validate_exit_strategy(self):
        if self.params["Exit_Strategy"] not in ['FixedPCT', 'ExitOnNextEntry']:
            print(f'Exit strategy ({self.params["Exit_Strategy"]}) not supported by {self.params["Strategy"]}.')
            sys.exit(1)

    # Calculate indicator values required to determine long/short signals
    @abstractmethod
    def add_indicators_and_signals(self):
        pass

    # Using signals set by add_trade_entry_signals(), add trade entry points
    @abstractmethod
    def add_trade_entry_points(self):
        pass

    # Cleanup and last minute formatting prior to saving trades dataframe to file
    @abstractmethod
    def clean_df_prior_to_saving(self):
        pass

    # For Statistics return a line of text that describes indicator details used
    # to generate these results
    @abstractmethod
    def get_strategy_text_details(self):
        pass

    def get_entry_fee(self, trade_amount):
        if self.ENTRY_AS_MAKER:
            return float(trade_amount) * self.MAKER_FEE_PCT
        else:
            return float(trade_amount) * self.TAKER_FEE_PCT

    def get_take_profit_fee(self, trade_amount):
        return float(trade_amount) * self.MAKER_FEE_PCT

    def get_stop_loss_fee(self, trade_amount):
        return float(trade_amount) * self.TAKER_FEE_PCT

    def get_exit_fee(self, trade_amount):
        if self.ENTRY_AS_MAKER:
            return float(trade_amount) * self.MAKER_FEE_PCT
        else:
            return float(trade_amount) * self.TAKER_FEE_PCT

    def get_stake_and_entry_fee(self, amount):
        staked_amount = amount * self.TRADABLE_BALANCE_RATIO
        if self.ENTRY_AS_MAKER:
            if self.ENTRY_AS_MAKER and self.MAKER_FEE_PCT > 0:
                staked_amount = math.floor(staked_amount / (1 + self.MAKER_FEE_PCT))
                entry_fee = self.get_entry_fee(staked_amount)
            else:
                entry_fee = self.get_entry_fee(staked_amount)
        else:
            staked_amount = math.floor(staked_amount / (1 + self.TAKER_FEE_PCT))
            entry_fee = self.get_entry_fee(staked_amount)
        return staked_amount, entry_fee

    @staticmethod
    def get_exit_type(side, _open, high, low, tp, sl):
        """
            When stop_loss and take_profit are both happening in the same candle, if sl is closer to open price it is
            executed first. If open is closer to high the tp is executed first.
        """
        if side == TradeType.Long:
            if high >= tp and low <= sl:
                if abs(_open - low) < abs(high - _open):
                    return ExitType.StopLoss
                return ExitType.TakeProfit
            elif high >= tp:
                return ExitType.TakeProfit
            elif low <= sl:
                return ExitType.StopLoss
        elif side == TradeType.Short:
            if high >= sl and low <= tp:
                if abs(_open - low) > abs(high - _open):
                    return ExitType.StopLoss
                return ExitType.TakeProfit
            elif high >= sl:
                return ExitType.StopLoss
            elif low <= tp:
                return ExitType.TakeProfit
        return None

    # Step 0: Get candle data used to backtest the strategy
    def get_candle_data(self):
        if self.config['database']['historical_data_stored_in_db']:
            self.df = self.db_reader.get_candle_data(
                self.params['Pair'],
                self.params['From_Time'],
                self.params['To_Time'],
                self.params['Interval'],
                include_prior=self.MIN_DATA_SIZE,
                verbose=True)
            if self.df is None:
                raise Exception(f"No data returned by the database. Unable to backtest strategy.")
            elif len(self.df) <= self.MIN_DATA_SIZE:
                print(
                    f'\nData rows = {len(self.df)}, less than MIN_DATA_SIZE={self.MIN_DATA_SIZE}. Unable to backtest strategy.')
                raise Exception("Unable to Run Strategy on Data Set")
        else:
            self.df = self.exchange.get_candle_data(
                self.params['Pair'],
                self.params['From_Time'],
                self.params['To_Time'],
                self.params['Interval'],
                include_prior=self.MIN_DATA_SIZE,
                write_to_file=True,
                verbose=True)
            if self.df is None:
                raise Exception(f"No data returned by {self.exchange.NAME}. Unable to backtest strategy.")
            elif len(self.df) <= self.MIN_DATA_SIZE:
                print(
                    f'\nData rows = {len(self.df)}, less than MIN_DATA_SIZE={self.MIN_DATA_SIZE}. Unable to backtest strategy.')
                raise Exception("Unable to Run Strategy on Data Set")

        # Set proper data types
        self.df['open'] = self.df['open'].astype(float)
        self.df['high'] = self.df['high'].astype(float)
        self.df['low'] = self.df['low'].astype(float)
        self.df['close'] = self.df['close'].astype(float)
        self.df['volume'] = self.df['volume'].astype(float)

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
    def get_all_trade_details_fixed_pct(self, curr_row, prev_row):
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

        # Enter Long, not a reverse
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

        # Enter Short, not a reverse
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
            exit_type = self.get_exit_type(TradeType.Long, curr_row['open'], curr_row['high'], curr_row['low'], prev_row['take_profit'], prev_row['stop_loss'])
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
            # Continue long, no event
            else:
                return TradeStatuses.Short, prev_row['entry_price'], prev_row['take_profit'], prev_row['stop_loss'], \
                       prev_row['wallet'], prev_row['staked_amount'], 0, 0, 0, 0
        else:
            print(f' *** unhandled case ***')
            raise Exception

    @get_all_trade_details_decorator
    def get_all_trade_details_exit_on_next_entry(self, curr_row, prev_row):
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

        # Enter Long, not a reverse
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

        # Enter Short, not a reverse
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
            # Reverse. Close long and open short
            if curr_row['trade_status'] == TradeStatuses.EnterShort:
                account_balance = win = loss = exit_fee = 0.0
                # Exit with loss
                if curr_row['close'] <= prev_row['entry_price']:
                    loss = (curr_row['close'] - prev_row['entry_price']) / prev_row['entry_price'] * prev_row[
                        'staked_amount']
                    exit_fee = self.get_exit_fee(prev_row['staked_amount'] - loss)
                    account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                # Exit with profit
                elif curr_row['close'] >= prev_row['entry_price']:
                    win = (curr_row['close'] - prev_row['entry_price']) / prev_row['entry_price'] * prev_row[
                        'staked_amount']
                    exit_fee = self.get_exit_fee(prev_row['staked_amount'] + win)
                    account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                # Open short
                take_profit = curr_row['close'] - (self.TP_PCT * curr_row['close'])
                stop_loss = curr_row['close'] + (self.SL_PCT * curr_row['close'])
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance = account_balance - (staked_amount + entry_fee)
                return TradeStatuses.EnterShort, curr_row[
                    'close'], take_profit, stop_loss, account_balance, staked_amount, win, loss, entry_fee, exit_fee

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
            # Continue long, no event
            else:
                return TradeStatuses.Long, prev_row['entry_price'], prev_row['take_profit'], prev_row['stop_loss'], \
                       prev_row['wallet'], prev_row['staked_amount'], 0, 0, 0, 0

        # Previous row in a short
        elif prev_row['trade_status'] in [TradeStatuses.EnterShort, TradeStatuses.Short]:
            # Reverse. Close short and open long
            if curr_row['trade_status'] == TradeStatuses.EnterLong:
                account_balance = win = loss = exit_fee = 0.0
                # Exit with loss
                if curr_row['close'] >= prev_row['entry_price']:
                    loss = (prev_row['entry_price'] - curr_row['close']) / prev_row['entry_price'] * prev_row[
                        'staked_amount']
                    exit_fee = self.get_exit_fee(prev_row['staked_amount'] + loss)
                    account_balance = prev_row['wallet'] + prev_row['staked_amount'] + loss - exit_fee
                # Exit with profit
                elif curr_row['close'] <= prev_row['entry_price']:
                    win = (prev_row['entry_price'] - curr_row['close']) / prev_row['entry_price'] * prev_row[
                        'staked_amount']
                    exit_fee = self.get_exit_fee(prev_row['staked_amount'] + win)
                    account_balance = prev_row['wallet'] + prev_row['staked_amount'] + win - exit_fee
                # Open long
                take_profit = curr_row['close'] + (self.TP_PCT * curr_row['close'])
                stop_loss = curr_row['close'] - (self.SL_PCT * curr_row['close'])
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance = account_balance - (staked_amount + entry_fee)
                return TradeStatuses.EnterLong, curr_row[
                    'close'], take_profit, stop_loss, account_balance, staked_amount, win, loss, entry_fee, exit_fee

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
            # Continue long, no event
            else:
                return TradeStatuses.Short, prev_row['entry_price'], prev_row['take_profit'], prev_row['stop_loss'], \
                       prev_row['wallet'], prev_row['staked_amount'], 0, 0, 0, 0
        else:
            print(f' *** unhandled case ***')
            raise Exception

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

        if self.params['Exit_Strategy'] == 'FixedPCT':
            self.prev_row = {}
            self.df[['trade_status', 'entry_price', 'take_profit', 'stop_loss', 'wallet',
                     'staked_amount', 'win', 'loss', 'entry_fee', 'exit_fee']] = \
                self.df.apply(self.get_all_trade_details_fixed_pct, axis=1).apply(pd.Series)
        elif self.params['Exit_Strategy'] == 'ExitOnNextEntry':
            self.prev_row = {}
            self.df[['trade_status', 'entry_price', 'take_profit', 'stop_loss', 'wallet',
                     'staked_amount', 'win', 'loss', 'entry_fee', 'exit_fee']] = \
                self.df.apply(self.get_all_trade_details_exit_on_next_entry, axis=1).apply(pd.Series)
        else:
            print(f'Unimplemented exit strategy.')
            sys.exit(1)

        # Statistics
        self.stats.nb_wins = self.df['win'].astype(bool).sum(axis=0)
        self.stats.nb_losses = self.df['loss'].astype(bool).sum(axis=0)
        self.stats.total_wins = self.df['win'].sum()
        self.stats.total_losses = self.df['loss'].sum()
        self.stats.total_fees_paid = self.df['entry_fee'].sum() + self.df['exit_fee'].sum()

        #print()  # Jump to next line
        return self.df

    # old implementation or process_trades() using a loop (slower)
    def process_trades_old(self):
        exit_fixed_pct = self.params['Exit_Strategy'] == 'FixedPCT'
        exit_on_entry = self.params['Exit_Strategy'] == 'ExitOnNextEntry'
        account_balance = self.params['Initial_Capital']
        staked_amount = 0.0
        entry_price = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        print(f"Processing trades using the [{self.NAME}, {self.params['Exit_Strategy']}] strategy.")
        print(self.get_strategy_text_details())

        # Add and Initialize new columns
        self.df.loc[:, 'wallet'] = 0.0
        self.df.loc[:, 'take_profit'] = None
        self.df.loc[:, 'stop_loss'] = None
        self.df.loc[:, 'win'] = 0.0
        self.df.loc[:, 'loss'] = 0.0
        self.df.loc[:, 'entry_fee'] = 0.0
        self.df.loc[:, 'exit_fee'] = 0.0

        # We use numeric indexing to update values in the DataFrame
        # Find the column indexes
        account_balance_col_index = self.df.columns.get_loc("wallet")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        tp_col_index = self.df.columns.get_loc("take_profit")
        sl_col_index = self.df.columns.get_loc("stop_loss")
        wins_col_index = self.df.columns.get_loc("win")
        losses_col_index = self.df.columns.get_loc("loss")
        entry_fee_col_index = self.df.columns.get_loc("entry_fee")
        exit_fee_col_index = self.df.columns.get_loc("exit_fee")

        for i, row in enumerate(self.df.itertuples(index=True), 0):

            # ------------------------------- Longs -------------------------------
            if trade_status == '' and row.trade_status == TradeStatuses.EnterLong:

                # Progress Bar at Console
                self.update_progress_dots()

                entry_price = row.close
                # Stop Loss / Take Profit
                stop_loss = entry_price - (self.SL_PCT * entry_price)
                take_profit = entry_price + (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                self.df.iloc[i, entry_fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)

                trade_status = TradeStatuses.Long

            elif (exit_fixed_pct and trade_status == TradeStatuses.Long) or \
                    (exit_on_entry and trade_status == TradeStatuses.Long and
                     (pd.isnull(row.trade_status) or (row.trade_status == TradeStatuses.EnterLong))):
                if row.low <= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    self.stats.total_losses += loss
                    trade_status = ''
                    self.stats.nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount - loss)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0
                elif row.high >= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    self.stats.total_wins += win
                    trade_status = ''
                    self.stats.nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount + win)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    trade_status = TradeStatuses.Long

            # If we are in a long and encounter a EnterShort signal, we close the current long and open a short
            elif exit_on_entry and trade_status == TradeStatuses.Long and row.trade_status == TradeStatuses.EnterShort:
                # Close a win
                if row.close >= entry_price:
                    win = (row.close - entry_price) / entry_price * staked_amount
                    self.df.iloc[i, wins_col_index] = win
                    self.stats.total_wins += win
                    self.stats.nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_exit_fee(staked_amount + win)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                # Close a loss
                else:
                    loss = (row.close - entry_price) / entry_price * staked_amount
                    self.df.iloc[i, losses_col_index] = loss
                    self.stats.total_losses += loss
                    self.stats.nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_exit_fee(staked_amount - loss)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0

                # Progress Bar at Console
                self.update_progress_dots()

                # Enter Short
                entry_price = row.close
                # Stop Loss / Take Profit
                stop_loss = entry_price + (self.SL_PCT * entry_price)
                take_profit = entry_price - (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                self.df.iloc[i, entry_fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)
                trade_status = TradeStatuses.Short

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == TradeStatuses.EnterShort:

                # Progress Bar at Console
                self.update_progress_dots()

                entry_price = row.close
                # Stop Loss / Take Profit
                stop_loss = entry_price + (self.SL_PCT * entry_price)
                take_profit = entry_price - (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                self.df.iloc[i, entry_fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)

                trade_status = TradeStatuses.Short

            elif (exit_fixed_pct and trade_status == TradeStatuses.Short) or \
                    (exit_on_entry and trade_status == TradeStatuses.Short and
                     (pd.isnull(row.trade_status) or row.trade_status == TradeStatuses.EnterShort)):
                if row.high >= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    self.stats.total_losses += loss
                    trade_status = ''
                    self.stats.nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount + loss)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0
                elif row.low <= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    self.stats.total_wins += win
                    trade_status = ''
                    self.stats.nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount - win)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    trade_status = TradeStatuses.Short

            # If we are in a short and encounter a EnterLong signal, we close the current short and open a long
            elif exit_on_entry and trade_status == TradeStatuses.Short and row.trade_status == TradeStatuses.EnterLong:
                # Close a win
                if row.close <= entry_price:
                    win = (entry_price - row.close) / entry_price * staked_amount
                    self.df.iloc[i, wins_col_index] = win
                    self.stats.total_wins += win
                    self.stats.nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_exit_fee(staked_amount - win)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                # Close a loss
                else:
                    loss = (entry_price - row.close) / entry_price * staked_amount
                    self.df.iloc[i, losses_col_index] = loss
                    self.stats.total_losses += loss
                    self.stats.nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_exit_fee(staked_amount + loss)
                    self.df.iloc[i, exit_fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0

                # Progress Bar at Console
                self.update_progress_dots()

                # Enter Long
                entry_price = row.close
                # Stop Loss / Take Profit
                stop_loss = entry_price - (self.SL_PCT * entry_price)
                take_profit = entry_price + (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount, entry_fee = self.get_stake_and_entry_fee(account_balance)
                self.df.iloc[i, entry_fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)
                trade_status = TradeStatuses.Long

            # Update account_balance running balance
            self.df.iloc[i, account_balance_col_index] = account_balance

            if account_balance < 0:
                print(f"\nWARNING: ********* Account balance is below zero. balance = {account_balance} *********")

        print()  # Jump to next line
        return self.df

    # Step 4: Validate Trades, TP and SL Exits
    def validate_trades(self):
        # Validate TP/SL Exits
        conditions = [
            ((self.df['high'] >= self.df['take_profit'].fillna(0)) & (self.df['trade_status'] == TradeStatuses.Long)),
            ((self.df['low'] <= self.df['stop_loss'].fillna(0)) & (self.df['trade_status'] == TradeStatuses.Long)),
            ((self.df['low'] <= self.df['take_profit'].fillna(0)) & (self.df['trade_status'] == TradeStatuses.Short)),
            ((self.df['high'] >= self.df['stop_loss'].fillna(0)) & (self.df['trade_status'] == TradeStatuses.Short))
        ]
        choices = ['TP Exit Missed', 'SL Exit Missed', 'TP Exit Missed', 'SL Exit Missed']

        self.df.loc[:, 'Errors'] = np.select(conditions, choices, default=None)

        errors_count = self.df['Errors'].notnull().sum()
        if errors_count > 0:
            print(f'\n*** {errors_count} Errors where found related to TP/SL exits. '
                  f'Check the "Errors" column in the Trades file. ***\n')
        else:
            self.df.drop(['Errors'], axis=1, inplace=True)

    # Step 5: Save trade data to file
    def save_trades_to_file(self):
        # Save trade details to file
        self.clean_df_prior_to_saving()
        utils.save_trades_to_file(self.params['Test_Num'],
                                  self.exchange.NAME,
                                  self.params['Pair'],
                                  self.params['From_Time'],
                                  self.params['To_Time'],
                                  self.params['Interval'],
                                  self.df, False, True)

    # Step 6: Write Statistics to Statistics Result DataFrame
    def finalize_stats(self):
        # self.stats.max_conseq_wins, self.stats.max_conseq_losses = stats_utils.get_consecutives(self.df)
        self.stats.min_win_loose_index, self.stats.max_win_loose_index = stats_utils.get_win_loss_indexes(self.df)
        results = {
                'Test #': self.params['Test_Num'],
                'Exchange': self.exchange.NAME,
                'Pair': self.params['Pair'],
                'From': self.params['From_Time'].strftime("%Y-%m-%d"),
                'To': self.params['To_Time'].strftime("%Y-%m-%d"),
                'Interval': self.params['Interval'],
                'Init Capital': f'{self.params["Initial_Capital"]:,.2f}',
                'TP %': self.params['Take_Profit_PCT'],
                'SL %': self.params['Stop_Loss_PCT'],
                'Maker Fee %': self.MAKER_FEE_PCT * 100,
                'Taker Fee %': self.TAKER_FEE_PCT * 100,
                'Strategy': self.NAME,

                'Wins': int(self.stats.nb_wins),
                'Losses': int(self.stats.nb_losses),
                'Trades': int(self.stats.total_trades),
                'Win Rate': f'{self.stats.win_rate:.1f}%',
                'Loss Idx': self.stats.min_win_loose_index,
                'Win Idx': self.stats.max_win_loose_index,
                'Wins $': f'{self.stats.total_wins:,.2f}',
                'Losses $': f'{self.stats.total_losses:,.2f}',
                'Fees $': f'{self.stats.total_fees_paid:,.2f}',
                'Total P/L': f'{self.stats.total_pl:,.2f}',
                'Details': self.get_strategy_text_details()
            }

        # Store results in Results DataFrame
        self.params['Statistics'] = self.params['Statistics'].append(results, ignore_index=True)

        df = pd.DataFrame().append([results], ignore_index=True)
        del df['Init Capital']
        del df['Details']
        print('\n'+df.to_string(index=False)+'\n')

        if self.config['database']['historical_data_stored_in_db']:
            self.save_stats_to_db()

    def save_stats_to_db(self):
        table_name = 'Test_Results_Statistics'
        stats_df = self.params['Statistics'].iloc[[-1]]
        now = dt.datetime.now().strftime(constants.DATETIME_FMT)  # Get current no milliseconds
        now = datetime.strptime(now, constants.DATETIME_FMT)  # convert str back to datetime

        # Use: (df.loc[:,'New_Column']='value') or (df = df.assign(New_Column='value'))
        # instead of: df['New_Column']='value' <-- Generates warnings
        stats_df = stats_df.assign(Timestamp=now)
        stats_df.set_index('Timestamp', inplace=True)
        stats_df.to_sql(table_name, self.db_engine, index=True, if_exists='append')

    # Call this method each time a processed to update progress on console
    def update_progress_dots(self):
        if self.config['output']['progress_dots']:
            print('.', end='')
            self.progress_counter += 1
            if self.progress_counter > self.PROGRESS_COUNTER_MAX:
                self.progress_counter = 0
                print()

    # Used to validate if we should bypass the current trade entry
    # returns true by default and should be redefined in subclasses if needed
    # For example, we might encounter a trade entry, but the signal has been
    # generated during a prior trade therefore we might want to bypass this trade
    # (Used for ScalpEmaRsiAdx which used a signal and an entry that are separate).
    def entry_is_valid(self, current_index):
        return True
