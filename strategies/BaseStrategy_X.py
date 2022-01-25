"""
    This class implements the Step 3: process_trades() method for the "Early"
    version of strategies
"""
import datetime as dt
from abc import abstractmethod

import pandas as pd

import utils
from enums.TradeStatus import TradeStatuses
from enums.TradeTypes import TradeTypes
from strategies.BaseStrategy import BaseStrategy


class BaseStrategy_X(BaseStrategy):

    def __init__(self, params):
        super().__init__(params)

    # The early strategy that will inherit this class needs to implement this
    # function to find the exact point in time when the strategy criteria are met
    @abstractmethod
    def find_exact_trade_entry(self, df, from_time, to_time, trade_type):
        pass

    # Step 3: Mark start, ongoing and end of trades, as well as calculate statistics
    # We overwrite the process_trades() method from the IStrategy class for minute precision crossing
    def process_trades(self):

        account_balance = self.params['Initial_Capital']
        staked_amount = 0.0
        entry_price = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        print(f'Processing trades using the [{self.NAME}] strategy.')
        print(self.get_strategy_text_details())

        # Add and Initialize new columns
        self.df['wallet'] = 0.0
        self.df['entry_time'] = None
        self.df['entry_price'] = None
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        # Download locally all data for all minutes during time range (very slow)
        # self.cache_minutes_data()

        # We use numeric indexing to update values in the DataFrame
        # Find the column indexes
        account_balance_col_index = self.df.columns.get_loc("wallet")
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        tp_col_index = self.df.columns.get_loc("take_profit")
        sl_col_index = self.df.columns.get_loc("stop_loss")
        wins_col_index = self.df.columns.get_loc("win")
        losses_col_index = self.df.columns.get_loc("loss")
        fee_col_index = self.df.columns.get_loc("fee")
        entry_time_col_index = self.df.columns.get_loc("entry_time")
        entry_price_col_index = self.df.columns.get_loc("entry_price")

        interval = utils.convert_interval_to_min(self.params['Interval'])

        for i, row in enumerate(self.df.itertuples(index=True), 0):

            # ------------------------------- Longs -------------------------------
            if trade_status == '' and row.trade_status == TradeStatuses.EnterLong:

                # Progress Bar at Console
                self.update_progress_dots()

                # print(f'\nEntering Long: {row.Index}')
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(self.df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=interval)
                entry_time, entry_price = self.find_exact_trade_entry(
                    self.df[['high', 'low', 'close']].iloc[0:i],
                    start_time,
                    end_time,
                    TradeTypes.Long
                )
                self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')

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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and row.trade_status in [TradeStatuses.EnterLong,
                                                                               TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Long trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == TradeStatuses.EnterShort:

                # Progress Bar at Console
                self.update_progress_dots()

                # print(f'\nEntering Short: {row.Index}')
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(self.df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=interval)
                entry_time, entry_price = self.find_exact_trade_entry(
                    self.df[['high', 'low', 'close']].iloc[0:i],
                    start_time,
                    end_time,
                    TradeTypes.Short
                )
                self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')
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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
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
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and row.trade_status in [TradeStatuses.EnterLong,
                                                                                TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Short trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price

            # Update account_balance running balance
            self.df.iloc[i, account_balance_col_index] = account_balance

            if account_balance < 0:
                print(f"\nWARNING: ********* Account balance is below zero. balance = {account_balance} *********")
                exit(1)

        print()  # Jump to next line
        return self.df
