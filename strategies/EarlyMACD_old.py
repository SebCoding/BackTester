import datetime as dt

import numpy as np
import pandas as pd
import talib

import utils
from enums.TradeStatus import TradeStatuses
from strategies.IStrategy import IStrategy


class EarlyMACD(IStrategy):
    NAME = 'Early MACD'

    # Ratio of the total account balance allowed to be traded.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 1.0

    # Trend indicator: EMA - Exponential Moving Average
    EMA_PERIODS = 200

    # Trend following momentum indicator:
    # MACD - Moving Average Convergence Divergence
    MACD_FAST_PERIOD = 12
    MACD_SLOW_PERIOD = 26
    MACD_SIGNAL_PERIOD = 9

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = EMA_PERIODS

    # Indicator column names
    ema_col_name = 'EMA' + str(EMA_PERIODS)

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

        # Keep only this list of columns, delete all other columns
        # final_table_columns = ['pair', 'interval', 'open', 'high', 'low', 'close']
        # self.df = self.df[self.df.columns.intersection(final_table_columns)]

        # MACD - Moving Average Convergence/Divergence
        macd, macdsignal, macdhist = talib.MACD(self.df['close'],
                                                fastperiod=self.MACD_FAST_PERIOD,
                                                slowperiod=self.MACD_SLOW_PERIOD,
                                                signalperiod=self.MACD_SIGNAL_PERIOD)
        self.df['MACD'] = macd
        self.df['MACDSIG'] = macdsignal

        # EMA - Exponential Moving Average 200
        self.df[self.ema_col_name] = talib.EMA(self.df['close'], timeperiod=self.EMA_PERIODS)

        # Identify the trend
        # self.df.loc[self.df['close'] > self.df[self.ema_col_name], 'trend'] = 'Up'
        # self.df.loc[self.df['close'] < self.df[self.ema_col_name], 'trend'] = 'Down'

        # macdsignal over macd then 1, under 0
        self.df['O/U'] = np.where(self.df['MACDSIG'] >= self.df['MACD'], 1, 0)

        # macdsignal crosses macd
        self.df['cross'] = self.df['O/U'].diff()

    # Step 2: Identify the trade entries
    def add_trade_entry_points(self):
        print('Adding entry points for all trades.')
        # Enter long trade
        self.df.loc[
            (
                    (self.df['close'] > self.df[self.ema_col_name]) &  # price > ema200
                    (self.df['MACDSIG'] < 0) &  # macdsignal < 0
                    (self.df['cross'] == -1)  # macdsignal crossed and is now under macd
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Enter short trade
        self.df.loc[
            (
                    (self.df['close'] < self.df[self.ema_col_name]) &  # price < ema200
                    (self.df['MACDSIG'] > 0) &  # macdsignal > 0
                    (self.df['cross'] == 1)  # macdsignal crossed and is now over macd
            ),
            'trade_status'] = TradeStatuses.EnterShort

    # Find with a minute precision the first point where macd crossed macdsignal
    # and return the time and closing price for that point in time + delta minutes
    def find_crossing(self, df, pair, from_time, to_time, delta=0):
        # We need to get an extra row to see the value at -1min in case the cross is on the first row
        to_time = to_time - dt.timedelta(minutes=1)

        minutes_df = self.exchange.get_candle_data(0, pair, from_time, to_time, "1m", include_prior=0,
                                                   write_to_file=False, verbose=False)
        # Used cached data (very slow)
        # minutes_df = self.get_minutes_from_cached_file(from_time, to_time)

        # Only keep the close column
        minutes_df = minutes_df[['close']]

        # Convert column type to float
        minutes_df['close'] = minutes_df['close'].astype(float)

        tmp_list = []
        for index, row in minutes_df.iterrows():
            # print(f'Row >>> [{index}]')
            df2 = df.copy()
            df2 = df2.append(row)

            macd, macdsignal, macdhist = talib.MACD(df2['close'],
                                                    fastperiod=self.MACD_FAST_PERIOD,
                                                    slowperiod=self.MACD_SLOW_PERIOD,
                                                    signalperiod=self.MACD_SIGNAL_PERIOD)
            df2['MACD'] = macd
            df2['MACDSIG'] = macdsignal

            # macdsignal over macd then 1, under 0
            df2['O/U'] = np.where(df2['MACDSIG'] >= df2['MACD'], 1, 0)

            # macdsignal crosses macd
            df2['cross'] = df2['O/U'].diff()

            #         print(df2.tail(20).to_string())
            #         print('\n')

            # Remove nulls
            # df2.dropna(inplace=True)

            # Just keep last row
            tmp_list.append(df2.iloc[[-1]])

        result_df = pd.concat(tmp_list)
        # print(f'result_df_len: {len(result_df)}')

        # print(result_df.to_string())
        # print('\n')

        # Find first occurrence of crossing. Delta optional (add delta minutes)
        price_on_crossing = 0.0  # Force float
        time_on_crossing = dt.datetime(1, 1, 1)
        close_col_index = result_df.columns.get_loc("close")
        for i, row in enumerate(result_df.itertuples(index=True), 0):
            if row.cross in [-1, 1]:
                # print(f'Found 1st Crossing at [{i}] + delta[{delta}]')
                price_on_crossing = result_df.iloc[i + delta, close_col_index]
                time_on_crossing = utils.idx2datetime(result_df.index.values[i + delta])
                break

        return time_on_crossing, price_on_crossing

    # Step 3: Mark start, ongoing and end of trades, as well as calculate statistics
    # We overwrite the process_trades() method from the IStrategy class for minute precision crossing
    def process_trades(self):

        account_balance = self.params['Initial_Capital']
        staked_amount = 0.0
        entry_price = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        print(f'Processing trades using the [{self.NAME}] strategy')

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
                entry_time, entry_price = self.find_crossing(self.df[['close']].iloc[0:i], self.params['Pair'],
                                                             start_time, end_time)
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

                # We exit in the same candle we entered, hit stop loss
                if row.low <= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, losses_col_index] = loss
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

                # We exit in the same candle we entered, take profit
                elif row.high >= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, wins_col_index] = win
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
                entry_time, entry_price = self.find_crossing(self.df[['close']].iloc[0:i], self.params['Pair'],
                                                             start_time, end_time)
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

                # We exit in the same candle we entered, hit stop loss
                if row.high >= stop_loss:
                    loss = staked_amount * self.SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, losses_col_index] = loss
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

                # We exit in the same candle we entered, hit take profit
                elif row.low <= take_profit:
                    win = staked_amount * self.TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, wins_col_index] = win
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

        print()  # Jump to next line
        return self.df

    def clean_df_prior_to_saving(self):
        # Round all values to 2 decimals
        self.df['take_profit'] = self.df['take_profit'].astype(float).round(2)
        self.df['stop_loss'] = self.df['stop_loss'].astype(float).round(2)
        self.df = self.df.round(decimals=2)

        # Remove rows with nulls entries for ema200
        self.df = self.df.dropna(subset=[self.ema_col_name])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))
