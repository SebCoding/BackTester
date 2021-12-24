import time

import numpy as np
import pandas as pd
import talib
import datetime as dt

from enums.TradeStatus import TradeStatuses
from strategies.IStrategy import IStrategy

# Abstract Exchange Class
import stats
import utils


class MACD(IStrategy):

    NAME = 'MACD'
    STAKE_AMOUNT_PCT = 1.0

    def __init__(self, exchange, params, df):
        super().__init__(exchange, params, df)

    # ----------------------------------------------------------------------
    # Function used determine trade entries (long/short)
    # ----------------------------------------------------------------------
    # def trade_entries(self, trend, macdsignal, cross):
    #     if trend == 'Up' and macdsignal < 0 and cross == -1:
    #         return "Enter Long"
    #     elif trend == 'Down' and macdsignal > 0 and cross == 1:
    #         return "Enter Short"
    #     return None

    def mark_entries(self):
        # Mark long entries
        self.df.loc[
            (
                (self.df['close'] > self.df['ema200']) &  # price > ema200
                (self.df['macdsignal'] < 0) &  # macdsignal < 0
                (self.df['cross'] == -1)  # macdsignal crossed and is now under macd
            ),
            'trade_status'] = TradeStatuses.EnterLong

        # Mark short entries
        # trend == 'Down' and macdsignal > 0 and cross == 1:
        self.df.loc[
            (
                (self.df['close'] < self.df['ema200']) &  # price < ema200
                (self.df['macdsignal'] > 0) &  # macdsignal > 0
                (self.df['cross'] == 1)  # macdsignal crossed and is now over macd
            ),
            'trade_status'] = TradeStatuses.EnterShort

        # We enter the trade on the next candle after the signal candle has completed
        self.df['trade_status'] = self.df['trade_status'].shift(1)

    # Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and signals to data.')

        # Set proper data types
        self.df['open'] = self.df['open'].astype(float)
        self.df['high'] = self.df['high'].astype(float)
        self.df['low'] = self.df['low'].astype(float)
        self.df['close'] = self.df['close'].astype(float)
        self.df['volume'] = self.df['volume'].astype(float)

        # Keep only this list of columns, delete all other columns
        final_table_columns = ['pair', 'interval', 'open', 'high', 'low', 'close']
        self.df = self.df[self.df.columns.intersection(final_table_columns)]

        ## MACD - Moving Average Convergence/Divergence
        macd, macdsignal, macdhist = talib.MACD(self.df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        self.df['macd'] = macd
        self.df['macdsignal'] = macdsignal

        # EMA - Exponential Moving Average 200
        self.df['ema200'] = talib.EMA(self.df['close'], timeperiod=200)

        # Identify the trend
        self.df.loc[self.df['close'] > self.df['ema200'], 'trend'] = 'Up'
        self.df.loc[self.df['close'] < self.df['ema200'], 'trend'] = 'Down'

        # macdsignal over macd then 1, under 0
        self.df['O/U'] = np.where(self.df['macdsignal'] >= self.df['macd'], 1, 0)

        # macdsignal crosses macd
        self.df['cross'] = self.df['O/U'].diff()

        # Enter trade in the candle after the crossing
        # self.df['trade_status'] = self.df.apply(
        #     lambda x: self.trade_entries(x['trend'], x['macdsignal'], x['cross']),
        #     axis=1).shift(1)

        # Mark long/short entries
        self.mark_entries()

        # Add and Initialize new columns
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        return self.df

    # Mark start, ongoing and end of trades, as well as calculate statistics
    def process_trades(self):

        wallet = self.params['Initial_Capital']
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
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        tp_col_index = self.df.columns.get_loc("take_profit")
        sl_col_index = self.df.columns.get_loc("stop_loss")
        wins_col_index = self.df.columns.get_loc("win")
        losses_col_index = self.df.columns.get_loc("loss")
        fee_col_index = self.df.columns.get_loc("fee")

        # interval = utils.convert_interval_to_min(self.params['Interval'])
        TP_PCT = self.params['Take_Profit_PCT'] / 100
        SL_PCT = self.params['Stop_Loss_PCT'] / 100
        MAKER_FEE_PCT = self.exchange.MAKER_FEE_PCT / 100
        TAKER_FEE_PCT = self.exchange.TAKER_FEE_PCT / 100

        for i, row in enumerate(self.df.itertuples(index=True), 0):

            # ------------------------------- Longs -------------------------------
            if trade_status == '' and row.trade_status == TradeStatuses.EnterLong:

                self.update_progress_dots()

                entry_price = row.open
                stop_loss = entry_price - (SL_PCT * entry_price)
                take_profit = entry_price + (TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                entry_fee = self.params['Initial_Capital'] * MAKER_FEE_PCT
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.low <= stop_loss:
                    loss = self.params['Initial_Capital'] * SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Initial_Capital'] - loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee

                # We exit in the same candle we entered, take profit
                elif row.high >= take_profit:
                    win = self.params['Initial_Capital'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitLong
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Initial_Capital'] + win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee

                # We just entered TradeStatuses.EnterLong in this candle so set the status to TradeStatuses.Long
                else:
                    trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and pd.isnull(row.trade_status):
                if row.low <= stop_loss:
                    loss = self.params['Initial_Capital'] * SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Initial_Capital'] - loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                elif row.high >= take_profit:
                    win = self.params['Initial_Capital'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitLong
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Initial_Capital'] + win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = TradeStatuses.Long

            elif trade_status in [TradeStatuses.Long] and row.trade_status in [TradeStatuses.EnterLong, TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Long trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Long
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                # self.df.iloc[i, entry_price_col_index] = entry_price

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == TradeStatuses.EnterShort:

                self.update_progress_dots()

                entry_price = row.open
                stop_loss = entry_price + (SL_PCT * entry_price)
                take_profit = entry_price - (TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                entry_fee = self.params['Initial_Capital'] * MAKER_FEE_PCT
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.high >= stop_loss:
                    loss = SL_PCT * self.params['Initial_Capital'] * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Initial_Capital'] + loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                # We exit in the same candle we entered, hit take profit
                elif row.low <= take_profit:
                    win = self.params['Initial_Capital'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.EnterExitShort
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Initial_Capital'] - win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                # We just entered TradeStatuses.EnterShort in this candle, so set the status to TradeStatuses.Short
                else:
                    trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and pd.isnull(row.trade_status):
                if row.high >= stop_loss:
                    loss = SL_PCT * self.params['Initial_Capital'] * -1
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Initial_Capital'] + loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                elif row.low <= take_profit:
                    win = self.params['Initial_Capital'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.ExitShort
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Initial_Capital'] - win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                else:
                    self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    # self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = TradeStatuses.Short

            elif trade_status in [TradeStatuses.Short] and row.trade_status in [TradeStatuses.EnterLong, TradeStatuses.EnterShort]:
                # If we are in a long and encounter another TradeStatuses.EnterLong or a TradeStatuses.EnterShort
                # signal, ignore the signal and override the value with TradeStatuses.Long, we are already in a
                # TradeStatuses.Short trade
                self.df.iloc[i, trade_status_col_index] = TradeStatuses.Short
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                # self.df.iloc[i, entry_price_col_index] = entry_price

        # Remove rows with nulls entries for macd, macdsignal or ema200
        self.df = self.df.dropna(subset=['ema200'])

        # Remove underscores from column names
        self.df = self.df.rename(columns=lambda name: name.replace('_', ' '))

        print() # Jump to next line

        # Save trade details to file
        utils.save_trades_to_file(self.params['Test_Num'], self.params['Exchange'], self.params['Pair'],
                                  self.params['From_Time'],
                                  self.params['To_Time'], self.params['Interval'], self.df, False, True)

        max_conseq_wins, max_conseq_losses, min_win_loose_index, max_win_loose_index = stats.analyze_win_lose(self.df)

        # print_trade_stats(
        #     total_wins,
        #     total_losses,
        #     nb_wins,
        #     nb_losses,
        #     total_fees_paid,
        #     max_conseq_wins,
        #     max_conseq_losses,
        #     min_win_loose_index,
        #     max_win_loose_index
        # )

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
                'Total Trades': total_trades,
                'Success Rate': f'{success_rate:.1f}%',
                'Loss Idx': min_win_loose_index,
                'Win Idx': max_win_loose_index,
                'Wins $': total_wins,
                'Losses $': total_losses,
                'Fees $': round(total_fees_paid, 2),
                'Total P/L': round(total_wins + total_losses - total_fees_paid, 2)
            },
            ignore_index=True,
        )

        return self.df
