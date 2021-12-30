"""
    Base Abstract Strategy Class.
    All strategies inherit from this class and must implement the abstract methods of this class
"""
import math
from abc import ABC, abstractmethod
import datetime as dt
from datetime import datetime
import pandas as pd

import config
from database.DbDataReader import DbDataReader
from exchanges.ExchangeCCXT import ExchangeCCXT
from stats import stats_utils
import utils
from enums.TradeStatus import TradeStatuses
from stats.Statistics import Statistics

# Do not remove these imports even if PyCharm says they're unused
from exchanges.Binance import Binance
from exchanges.Bybit import Bybit


class BaseStrategy(ABC):
    NAME = 'abstract'

    # Ratio of the total account balance the bot is allowed to trade.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 0.0

    # Used to output on console a dot for each trade processed.
    # Used as limited output progress bar
    USE_DOT_PROGRESS_OUTPUT = True
    PROGRESS_COUNTER_MAX = 90

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = 0

    def __init__(self, params):
        self.df = None
        self.params = params
        self.progress_counter = 0
        self.TP_PCT = self.params['Take_Profit_PCT'] / 100
        self.SL_PCT = self.params['Stop_Loss_PCT'] / 100
        # self.exchange = globals()[params['Exchange']]()
        self.exchange = ExchangeCCXT(params['Exchange'].lower())
        self.MAKER_FEE_PCT = self.exchange.get_maker_fee(params['Pair'])
        self.TAKER_FEE_PCT = self.exchange.get_taker_fee(params['Pair'])
        self.stats = Statistics()
        if config.HISTORICAL_DATA_STORED_IN_DB:
            self.db_reader = DbDataReader(self.exchange.NAME)
            self.db_engine = self.db_reader.engine

    def run(self):
        self.get_candle_data()  # Step 0
        self.add_indicators_and_signals()  # Step1
        self.add_trade_entry_points()  # Step2
        self.process_trades()  # Step3
        self.save_trades_to_file()  # Step 4
        self.finalize_stats()  # Step 5

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
        # Uncomment/Comment one of the lines below to change
        # if trades are entered in maker/taker order
        
        # return float(trade_amount) * self.TAKER_FEE_PCT
        return float(trade_amount) * self.MAKER_FEE_PCT

    def get_take_profit_fee(self, trade_amount):
        return float(trade_amount) * self.MAKER_FEE_PCT

    def get_stop_loss_fee(self, trade_amount):
        return float(trade_amount) * self.TAKER_FEE_PCT

    def get_stake_and_entry_fee(self, amount):
        staked_amount = amount * self.TRADABLE_BALANCE_RATIO
        if self.MAKER_FEE_PCT > 0:
            staked_amount = math.floor(staked_amount / (1 + self.MAKER_FEE_PCT))
            entry_fee = self.get_entry_fee(staked_amount)
        else:
            entry_fee = self.get_entry_fee(staked_amount)
        return staked_amount, entry_fee

    # Step 0: Get candle data used to backtest the strategy
    def get_candle_data(self):
        if config.HISTORICAL_DATA_STORED_IN_DB:
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

    # Step 3: Mark start, ongoing and end of trades, as well as calculate statistics
    def process_trades(self):

        account_balance = self.params['Initial_Capital']
        staked_amount = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        print(f'Processing trades using the [{self.NAME}] strategy')

        # Add and Initialize new columns
        self.df['wallet'] = 0.0
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        # We use numeric indexing to update values in the DataFrame
        # Find the column indexes
        account_balance_col_index = self.df.columns.get_loc("wallet")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        tp_col_index = self.df.columns.get_loc("take_profit")
        sl_col_index = self.df.columns.get_loc("stop_loss")
        wins_col_index = self.df.columns.get_loc("win")
        losses_col_index = self.df.columns.get_loc("loss")
        fee_col_index = self.df.columns.get_loc("fee")

        for i, row in enumerate(self.df.itertuples(index=True), 0):

            # ------------------------------- Longs -------------------------------
            if trade_status == '' and row.trade_status == TradeStatuses.EnterLong and self.entry_is_valid(i):

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
                self.df.iloc[i, fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)

                # # We exit in the same candle we entered, hit stop loss
                # if row.low <= stop_loss:
                #     loss = staked_amount * self.SL_PCT * -1
                #     self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                #     self.df.iloc[i, losses_col_index] = loss
                #     self.stats.total_losses += loss
                #     trade_status = ''
                #     self.stats.nb_losses += 1
                #     # Exit Fee 'loss'
                #     exit_fee = self.get_stop_loss_fee(staked_amount - loss)
                #     self.df.iloc[i, fee_col_index] += exit_fee
                #     self.stats.total_fees_paid += exit_fee
                #     # Update staked and account_balance
                #     account_balance += staked_amount + loss - exit_fee
                #     staked_amount = 0.0
                #
                # # We exit in the same candle we entered, take profit
                # elif row.high >= take_profit:
                #     win = staked_amount * self.TP_PCT
                #     self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                #     self.df.iloc[i, wins_col_index] = win
                #     self.stats.total_wins += win
                #     trade_status = ''
                #     self.stats.nb_wins += 1
                #     # Exit Fee 'win'
                #     exit_fee = self.get_take_profit_fee(staked_amount + win)
                #     self.df.iloc[i, fee_col_index] += exit_fee
                #     self.stats.total_fees_paid += exit_fee
                #     # Update staked and account_balance
                #     account_balance += staked_amount + win - exit_fee
                #     staked_amount = 0.0
                #
                #
                # else:
                # We just entered TradeStatuses.EnterLong in this candle so set the status to TradeStatuses.Long
                trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and pd.isnull(row.trade_status):
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
                    self.df.iloc[i, fee_col_index] += exit_fee
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
                    self.df.iloc[i, fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and row.trade_status in [TradeStatuses.EnterLong,
                                                                               TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # ignore the signal and override the value with TradeStatuses.Long, we are already in a long
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == TradeStatuses.EnterShort and self.entry_is_valid(i):

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
                self.df.iloc[i, fee_col_index] += entry_fee
                self.stats.total_fees_paid += entry_fee
                # Update staked and account_balance
                if entry_fee < 0:  # Negative fee = credit/refund
                    # remove staked amount from balance and add fee credit/refund
                    account_balance = account_balance - staked_amount - entry_fee
                else:
                    account_balance -= (staked_amount + entry_fee)

                # # We exit in the same candle we entered, hit stop loss
                # if row.high >= stop_loss:
                #     loss = staked_amount * self.SL_PCT * -1
                #     self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                #     self.df.iloc[i, losses_col_index] = loss
                #     self.stats.total_losses += loss
                #     trade_status = ''
                #     self.stats.nb_losses += 1
                #     # Exit Fee 'loss'
                #     exit_fee = self.get_stop_loss_fee(staked_amount + loss)
                #     self.df.iloc[i, fee_col_index] += exit_fee
                #     self.stats.total_fees_paid += exit_fee
                #     # Update staked and account_balance
                #     account_balance += staked_amount + loss - exit_fee
                #     staked_amount = 0.0
                #
                # # We exit in the same candle we entered, hit take profit
                # elif row.low <= take_profit:
                #     win = staked_amount * self.TP_PCT
                #     self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                #     self.df.iloc[i, wins_col_index] = win
                #     self.stats.total_wins += win
                #     trade_status = ''
                #     self.stats.nb_wins += 1
                #     # Exit Fee 'win'
                #     exit_fee = self.get_take_profit_fee(staked_amount - win)
                #     self.df.iloc[i, fee_col_index] += exit_fee
                #     self.stats.total_fees_paid += exit_fee
                #     # Update staked and account_balance
                #     account_balance += staked_amount + win - exit_fee
                #     staked_amount = 0.0
                #
                #
                # else:
                # We just entered TradeStatuses.EnterShort in this candle, so set the status to TradeStatuses.Short
                trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and pd.isnull(row.trade_status):
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
                    self.df.iloc[i, fee_col_index] += exit_fee
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
                    self.df.iloc[i, fee_col_index] += exit_fee
                    self.stats.total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and row.trade_status in [TradeStatuses.EnterLong,
                                                                                TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # ignore the signal and override the value with TradeStatuses.Long, we are already in a short
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss

            # Update account_balance running balance
            self.df.iloc[i, account_balance_col_index] = account_balance

            if account_balance < 0:
                print(f"WARNING: ********* Account balance is below zero. balance = {account_balance} *********")

        print()  # Jump to next line
        return self.df

    # Step 4: Save trade data to file
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

    def finalize_stats(self):
        # self.stats.max_conseq_wins, self.stats.max_conseq_losses = stats_utils.get_consecutives(self.df)
        self.stats.min_win_loose_index, self.stats.max_win_loose_index = stats_utils.get_win_loss_indexes(self.df)
        # Store results in Results DataFrame
        self.params['Statistics'] = self.params['Statistics'].append(
            {
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

                'Wins': self.stats.nb_wins,
                'Losses': self.stats.nb_losses,
                'Trades': self.stats.total_trades,
                'Success Rate': f'{self.stats.success_rate:.1f}%',
                'Loss Idx': self.stats.min_win_loose_index,
                'Win Idx': self.stats.max_win_loose_index,
                'Wins $': f'{self.stats.total_wins:,.2f}',
                'Losses $': f'{self.stats.total_losses:,.2f}',
                'Fees $': f'{self.stats.total_fees_paid:,.2f}',
                'Total P/L': f'{self.stats.total_pl:,.2f}',
                'Details': self.get_strategy_text_details()
            },
            ignore_index=True,
        )
        if config.HISTORICAL_DATA_STORED_IN_DB:
            self.save_stats_to_db()

    def save_stats_to_db(self):
        table_name = 'Test_Results_Statistics'
        stats_df = self.params['Statistics'].iloc[[-1]]
        now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current no milliseconds
        now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S') # convert str back to datetime

        # Use: (df.loc[:,'New_Column']='value') or (df = df.assign(New_Column='value'))
        # instead of: df['New_Column']='value' <-- Generates warnings
        stats_df = stats_df.assign(Timestamp=now)
        stats_df.set_index('Timestamp', inplace=True)
        stats_df.to_sql(table_name, self.db_engine, index=True, if_exists='append')

    # Call this method each time a processed to update progress on console
    def update_progress_dots(self):
        if self.USE_DOT_PROGRESS_OUTPUT:
            print('.', end='')
            self.progress_counter += 1
            if self.progress_counter > self.PROGRESS_COUNTER_MAX:
                self.progress_counter = 0
                print()

    # Used to validate if we should bypass the current trade entry
    # returns true by default and should be redefined in subclasses if needed
    # For example, we might encounter a trade entry, but the signal has been
    # generated during a prior trade therefore we might want to bypass this trade
    def entry_is_valid(self, current_index):
        return True
