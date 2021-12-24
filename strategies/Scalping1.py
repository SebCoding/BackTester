import numpy as np
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

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = 50

    def __init__(self, exchange, params, df):
        super().__init__(exchange, params, df)
        self.TP_PCT = self.params['Take_Profit_PCT'] / 100
        self.SL_PCT = self.params['Stop_Loss_PCT'] / 100

    def mark_entries(self):
        # Mark long entries
        self.df.loc[
            (
                    (self.df['close'] > self.df['EMA50']) &  # price > EMA-50
                    (self.df['RSI3'] < 20) &  # RSI < 20
                    (self.df['ADX5'] > 30)  # ADX > 30
            ),
            'signal'] = 1

        # Mark short entries
        # trend == 'Down' and macdsignal > 0 and cross == 1:
        self.df.loc[
            (
                    (self.df['close'] < self.df['EMA50']) &  # price < EMA-50
                    (self.df['RSI3'] > 80) &  # RSI > 80
                    (self.df['ADX5'] > 30)  # ADX > 30
            ),
            'signal'] = -1

    # When we get a 'Enter Long' signal we only enter the trade when the RSI exists the oversold area
    # When we get a 'Enter Short' signal we only enter the trade when the RSI exists the overbought area
    def adjust_entry_points(self):

        self.df['trade_status'] = None
        received_long_signal = False
        received_short_signal = False
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        rsi3_col_index = self.df.columns.get_loc("RSI3")

        for i, row in enumerate(self.df.itertuples(index=True), 0):
            # if we receive another signal while we are not done processing the prior one,
            # we ignore the new ones until the old one is processed
            if row.signal == 1 and not received_long_signal and not received_short_signal:
                received_long_signal = True
            elif row.signal == -1 and not received_long_signal and not received_short_signal:
                received_short_signal = True

            # RSI exiting oversold area
            if received_long_signal and self.df.iloc[i, rsi3_col_index] > 20:
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterLong
                received_long_signal = False
            # RSI exiting overbought area
            elif received_short_signal and self.df.iloc[i, rsi3_col_index] < 80:
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
        self.df['EMA50'] = talib.EMA(self.df['close'], timeperiod=50)

        # Momentum Indicator. RSI-3
        self.df['RSI3'] = talib.RSI(self.df['close'], timeperiod=3)

        # Volatility Indicator. ADX-5
        self.df['ADX5'] = talib.ADX(self.df['high'], self.df['low'], self.df['close'], timeperiod=5)

        # Identify the trend
        # self.df.loc[self.df['close'] > self.df['EMA-50'], 'trend'] = 'Up'
        # self.df.loc[self.df['close'] < self.df['EMA-50'], 'trend'] = 'Down'

        # Mark long/short entries
        self.mark_entries()

        # When we get a 'Enter Long' signal we only enter the trade when the RSI exists the oversold area
        # When we get a 'Enter Short' signal we only enter the trade when the RSI exists the overbought area
        self.adjust_entry_points()

        # Add and Initialize new columns
        self.df['wallet'] = 0.0
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        return self.df

    def get_entry_fee(self, trade_amount):
        return float(trade_amount) * self.exchange.MAKER_FEE_PCT / 100

    def get_take_profit_fee(self, trade_amount):
        return float(trade_amount) * self.exchange.MAKER_FEE_PCT / 100

    def get_stop_loss_fee(self, trade_amount):
        return float(trade_amount) * self.exchange.TAKER_FEE_PCT / 100

    def get_stake_amount(self, amount):
        return amount * self.TRADABLE_BALANCE_RATIO

    # Mark start, ongoing and end of trades, as well as calculate statistics
    def process_trades(self):

        account_balance = self.params['Initial_Capital']
        staked_amount = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        # Stats
        nb_wins = 0
        nb_losses = 0
        total_wins = 0.0
        total_losses = 0.0
        total_fees_paid = 0.0

        print(f'Processing trades using the [{self.NAME}] strategy')

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
            if trade_status == '' and row.trade_status == TradeStatuses.EnterLong:

                # Progress Bar at Console
                self.update_progress_dots()

                entry_price = row.open
                # Stop Loss / Take Profit
                stop_loss = entry_price - (self.SL_PCT * entry_price)
                take_profit = entry_price + (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount = self.get_stake_amount(account_balance)
                entry_fee = self.get_entry_fee(staked_amount)
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee
                # Update staked and account_balance
                account_balance -= staked_amount
                staked_amount -= entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.low <= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount - loss)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0

                # We exit in the same candle we entered, take profit
                elif row.high >= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount + win)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0

                # We just entered TradeStatuses.EnterLong in this candle so set the status to TradeStatuses.Long
                else:
                    trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and pd.isnull(row.trade_status):
                if row.low <= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount - loss)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0
                elif row.high >= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount + win)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
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
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Long trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == TradeStatuses.EnterShort:

                # Progress Bar at Console
                self.update_progress_dots()

                entry_price = row.open
                # Stop Loss / Take Profit
                stop_loss = entry_price + (self.SL_PCT * entry_price)
                take_profit = entry_price - (self.TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                staked_amount = self.get_stake_amount(account_balance)
                entry_fee = self.get_entry_fee(staked_amount)
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee
                # Update staked and account_balance
                account_balance -= staked_amount
                staked_amount -= entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.high >= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount + loss)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0

                # We exit in the same candle we entered, hit take profit
                elif row.low <= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount - win)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + win - exit_fee
                    staked_amount = 0.0

                # We just entered TradeStatuses.EnterShort in this candle, so set the status to TradeStatuses.Short
                else:
                    trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and pd.isnull(row.trade_status):
                if row.high >= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = self.get_stop_loss_fee(staked_amount + loss)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                    # Update staked and account_balance
                    account_balance += staked_amount + loss - exit_fee
                    staked_amount = 0.0
                elif row.low <= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = self.get_take_profit_fee(staked_amount - win)
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
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
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Short trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss

            # Update account_balance running balance
            self.df.iloc[i, account_balance_col_index] = account_balance

            if account_balance < 0:
                print(f"WARNING: ********* Account balance is below zero. balance = {account_balance} *********")

        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for EMA50
        self.df = self.df.dropna(subset=['EMA50'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))

        print()  # Jump to next line

        # Save trade details to file
        utils.save_trades_to_file(self.params['Test_Num'], self.params['Exchange'], self.params['Pair'],
                                  self.params['From_Time'],
                                  self.params['To_Time'], self.params['Interval'], self.df, False, True)

        max_conseq_wins, max_conseq_losses, min_win_loose_index, max_win_loose_index = stats.analyze_win_lose(self.df)

        # Store results in Results DataFrame
        total_trades = nb_wins + nb_losses
        success_rate = (nb_wins / total_trades * 100) if total_trades != 0 else 0
        self.params['Statistics'] = self.params['Statistics'].append(
            {
                'Test #': self.params['Test_Num'],
                'Exchange': self.params['Exchange'],
                'Pair': self.params['Pair'],
                'From': self.params['From_Time'].strftime("%Y-%m-%d"),
                'To': self.params['To_Time'].strftime("%Y-%m-%d"),
                'Interval': self.params['Interval'],
                'Init Capital': self.params['Initial_Capital'],
                'TP %': self.params['Take_Profit_PCT'],
                'SL %': self.params['Stop_Loss_PCT'],
                'Maker Fee %': self.exchange.MAKER_FEE_PCT,
                'Taker Fee %': self.exchange.TAKER_FEE_PCT,
                'Strategy': self.params['Strategy'],

                'Wins': nb_wins,
                'Losses': nb_losses,
                'Trades': total_trades,
                'Success Rate': f'{success_rate:.1f}%',
                'Loss Idx': min_win_loose_index,
                'Win Idx': max_win_loose_index,
                'Wins $': round(total_wins, 2),
                'Losses $': round(total_losses, 2),
                'Fees $': round(total_fees_paid, 2),
                'Total P/L': round(total_wins + total_losses - total_fees_paid, 2)
            },
            ignore_index=True,
        )

        return self.df
