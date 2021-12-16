import numpy as np
import pandas as pd
import talib
import datetime as dt

from Strategy import Strategy

# Abstract Exchange Class
import stats
import utils

class StrategyEarlyMACD(Strategy):

    NAME = 'Early MACD'

    def __init__(self, params, df, my_exchange):
        super().__init__(params, df)
        self.my_exchange = my_exchange

    # ----------------------------------------------------------------------
    # Function used determine trade entries (long/short)
    # ----------------------------------------------------------------------
    def trade_entries(self, open, ema200, macdsignal, cross):
        if open >= ema200 and macdsignal < 0 and cross == -1:
            return "Enter Long"
        elif open < ema200 and macdsignal > 0 and cross == 1:
            return "Enter Short"
        return None

    # Calculate indicator values required to determine long/short signals
    def add_indicators_and_signals(self):
        print('Adding indicators and Signals to Data.')

        # Set proper data types
        self.df['open'] = self.df['open'].astype(float)
        self.df['high'] = self.df['high'].astype(float)
        self.df['low'] = self.df['low'].astype(float)
        self.df['close'] = self.df['close'].astype(float)
        self.df['volume'] = self.df['volume'].astype(float)

        # Keep only this list of columns, delete all other columns
        final_table_columns = ['symbol', 'interval', 'open', 'high', 'low', 'close']
        self.df = self.df[self.df.columns.intersection(final_table_columns)]

        ## MACD - Moving Average Convergence/Divergence
        tmp = pd.DataFrame()
        tmp['macd'], tmp['macdsignal'], tmp['macdhist'] = talib.MACD(self.df['close'], fastperiod=12, slowperiod=26,
                                                                     signalperiod=9)
        tmp.drop(['macdhist'], axis=1, inplace=True)
        self.df = self.df.join(tmp, rsuffix='_right')

        ## EMA - Exponential Moving Average
        self.df['ema200'] = talib.EMA(self.df['close'], timeperiod=200)

        # # Remove nulls
        # self.df.dropna(inplace=True)

        # # Check if price is greater than ema200
        self.df['GT_ema200'] = np.where(self.df['open'] > self.df['ema200'], 'Bull', 'Bear')

        # macdsignal over macd then 1, under 0
        self.df['O/U'] = np.where(self.df['macdsignal'] >= self.df['macd'], 1, 0)

        # macdsignal crosses macd
        self.df['cross'] = self.df['O/U'].diff()

        # Drop now useless 'signal_over_under' column
        # self.df.drop(['signal_over_under'], inplace=True, axis = 1)

        # Remove nulls
        # self.df.dropna(inplace=True)

        # Enter trade on the same candle as the crossing
        self.df['trade_status'] = self.df.apply(lambda x: self.trade_entries(x['open'], x['ema200'], x['macdsignal'], x['cross']), axis=1)

        # Add and Initialize new columns
        self.df['entry_time'] = None
        self.df['entry_price'] = None
        self.df['take_profit'] = None
        self.df['stop_loss'] = None
        self.df['win'] = 0.0
        self.df['loss'] = 0.0
        self.df['fee'] = 0.0

        return self.df

    # Find with a minute precision the first point where macd crossed macdsignal
    # and return the time and closing price for that point
    def find_crossing(self, df, symbol, from_time, to_time, delta=0):
        # We need to get an extra row to see the value at -1min in case the cross is on the first row
        to_time = to_time - dt.timedelta(minutes=1)

        minutes_df = self.my_exchange.get_candle_data(0, symbol, from_time, to_time, "1", include_prior=0,
                                                 write_to_file=False, verbose=True)

        # Only keep the close column
        minutes_df = minutes_df[['close']]

        # Convert column type to float
        minutes_df['close'] = minutes_df['close'].astype(float)

        tmp_list = []
        for index, row in minutes_df.iterrows():
            # print(f'Row >>> [{index}]')
            df2 = df.copy()
            df2 = df2.append(row)

            df2['macd'], df2['macdsignal'], df2['macdhist'] = talib.MACD(df2['close'], fastperiod=12, slowperiod=26,
                                                                         signalperiod=9)
            # del df2['macdhist']

            # macdsignal over macd then 1, under 0
            df2['O/U'] = np.where(df2['macdsignal'] >= df2['macd'], 1, 0)

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
        price_on_crossing = 0.0 # Force float
        time_on_crossing = dt.datetime(1, 1, 1)
        close_col_index = result_df.columns.get_loc("close")
        for i, row in enumerate(result_df.itertuples(index=True), 0):
            if row.cross in [-1, 1]:
                # print(f'Found 1st Crossing at [{i}] + delta[{delta}]')
                price_on_crossing = result_df.iloc[i + delta, close_col_index]
                time_on_crossing = utils.idx2datetime(result_df.index.values[i + delta])
                break

        return time_on_crossing, price_on_crossing

    # Mark start, ongoing and end of trades, as well as calculate statistics
    def process_trades(self):
        # entry_time = None
        entry_price = 0.0
        stop_loss = 0.0
        take_profit = 0.0
        trade_status = ''

        # Stats
        nb_wins = 0
        nb_losses = 0
        total_wins = 0.0
        total_losses = 0.0
        total_fees_paid = 0.0

        print(f'Processing Trades using the [{self.NAME}] strategy')

        # We use numeric indexing to update values in the DataFrame
        # Find the column indexes
        trade_status_col_index = self.df.columns.get_loc("trade_status")
        tp_col_index = self.df.columns.get_loc("take_profit")
        sl_col_index = self.df.columns.get_loc("stop_loss")
        wins_col_index = self.df.columns.get_loc("win")
        losses_col_index = self.df.columns.get_loc("loss")
        fee_col_index = self.df.columns.get_loc("fee")
        entry_time_col_index = self.df.columns.get_loc("entry_time")
        entry_price_col_index = self.df.columns.get_loc("entry_price")

        interval = utils.convert_interval_to_min(self.params['Interval'])
        TP_PCT = self.params['Take_Profit_PCT'] / 100
        SL_PCT = self.params['Stop_Loss_PCT'] / 100
        MAKER_FEE_PCT = self.params['Maker_Fee_PCT'] / 100
        TAKER_FEE_PCT = self.params['Taker_Fee_PCT'] / 100

        for i, row in enumerate(self.df.itertuples(index=True), 0):

            # ------------------------------- Longs -------------------------------
            if trade_status == '' and row.trade_status == 'Enter Long':

                # print(f'\nEntering Long: {row.Index}')
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(self.df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=interval)
                entry_time, entry_price = self.find_crossing(self.df[['close']].iloc[0:i], self.params['Symbol'], start_time, end_time)
                self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')


                stop_loss = entry_price - (SL_PCT * entry_price)
                take_profit = entry_price + (TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                entry_fee = self.params['Trade_Amount'] * TAKER_FEE_PCT
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.low <= stop_loss:
                    loss = self.params['Trade_Amount'] * SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = 'Enter/Exit Long'
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Trade_Amount'] - loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee

                # We exit in the same candle we entered, take profit
                elif row.high >= take_profit:
                    win = self.params['Trade_Amount'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = 'Enter/Exit Long'
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Trade_Amount'] + win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee

                # We just entered 'Enter long' in this candle so set the status to 'Long'
                else:
                    trade_status = 'Long'

            elif trade_status in ['Long'] and pd.isnull(row.trade_status):
                if row.low <= stop_loss:
                    loss = self.params['Trade_Amount'] * SL_PCT * -1
                    self.df.iloc[i, trade_status_col_index] = 'Exit Long'
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Trade_Amount'] - loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                elif row.high >= take_profit:
                    win = self.params['Trade_Amount'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = 'Exit Long'
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Trade_Amount'] + win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                else:
                    self.df.iloc[i, trade_status_col_index] = 'Long'
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = 'Long'

            elif trade_status in ['Long'] and row.trade_status in ['Enter Long', 'Enter Short']:
                # If we are in a long and encounter another 'Enter Long' or a 'Enter Short' signal,
                # ignore the signal and override the value with 'Long', we are already in a 'Long' trade
                self.df.iloc[i, trade_status_col_index] = 'Long'
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price

            # ------------------------------- Shorts -------------------------------
            elif trade_status == '' and row.trade_status == 'Enter Short':

                # print(f'\nEntering Short: {row.Index}')
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(self.df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=interval)
                entry_time, entry_price = self.find_crossing(self.df[['close']].iloc[0:i], self.params['Symbol'], start_time, end_time)
                self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')

                stop_loss = entry_price + (SL_PCT * entry_price)
                take_profit = entry_price - (TP_PCT * entry_price)
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # Entry Fee
                entry_fee = self.params['Trade_Amount'] * TAKER_FEE_PCT
                self.df.iloc[i, fee_col_index] += entry_fee
                total_fees_paid += entry_fee

                # We exit in the same candle we entered, hit stop loss
                if row.high >= stop_loss:
                    loss = SL_PCT * self.params['Trade_Amount'] * -1
                    self.df.iloc[i, trade_status_col_index] = 'Enter/Exit Short'
                    self.df.iloc[i, losses_col_index] = loss
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Trade_Amount'] + loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                # We exit in the same candle we entered, hit take profit
                elif row.low <= take_profit:
                    win = self.params['Trade_Amount'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = 'Enter/Exit Short'
                    self.df.iloc[i, wins_col_index] = win
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Trade_Amount'] - win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                # We just entered 'Enter Short' in this candle, so set the status to 'Short'
                else:
                    trade_status = 'Short'

            elif trade_status in ['Short'] and pd.isnull(row.trade_status):
                if row.high >= stop_loss:
                    loss = SL_PCT * self.params['Trade_Amount'] * -1
                    self.df.iloc[i, trade_status_col_index] = 'Exit Short'
                    self.df.iloc[i, losses_col_index] = loss
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    total_losses += loss
                    trade_status = ''
                    nb_losses += 1
                    # Exit Fee 'loss'
                    exit_fee = (self.params['Trade_Amount'] + loss) * TAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                elif row.low <= take_profit:
                    win = self.params['Trade_Amount'] * TP_PCT
                    self.df.iloc[i, trade_status_col_index] = 'Exit Short'
                    self.df.iloc[i, wins_col_index] = win
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    total_wins += win
                    trade_status = ''
                    nb_wins += 1
                    # Exit Fee 'win'
                    exit_fee = (self.params['Trade_Amount'] - win) * MAKER_FEE_PCT
                    self.df.iloc[i, fee_col_index] += exit_fee
                    total_fees_paid += exit_fee
                else:
                    self.df.iloc[i, trade_status_col_index] = 'Short'
                    self.df.iloc[i, tp_col_index] = take_profit
                    self.df.iloc[i, sl_col_index] = stop_loss
                    # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                    self.df.iloc[i, entry_price_col_index] = entry_price
                    trade_status = 'Short'

            elif trade_status in ['Short'] and row.trade_status in ['Enter Long', 'Enter Short']:
                # If we are in a long and encounter another 'Enter Long' or a 'Enter Short' signal,
                # ignore the signal and override the value with 'Long', we are already in a 'Short' trade
                self.df.iloc[i, trade_status_col_index] = 'Short'
                self.df.iloc[i, tp_col_index] = take_profit
                self.df.iloc[i, sl_col_index] = stop_loss
                # self.df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                self.df.iloc[i, entry_price_col_index] = entry_price

        # Remove nulls
        # self.df.dropna(inplace=True)
        # self.df = self.df.loc[self.df['macd'] != None]
        self.df = self.df.loc[self.df['macd'].apply(lambda x: x is not None)]

        # Save trade details to file
        utils.save_dataframe2file(self.params['Test_Num'], self.params['Exchange'], self.params['Symbol'], self.params['From_Time'],
                                  self.params['To_Time'], self.params['Interval'], self.df, False, False, True)

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
        self.params['Results'] = self.params['Results'].append(
            {
                'Test #': self.params['Test_Num'],
                'Exchange': self.params['Exchange'],
                'Symbol': self.params['Symbol'],
                'From': self.params['From_Time'].strftime("%Y-%m-%d"),
                'To': self.params['To_Time'].strftime("%Y-%m-%d"),
                'Interval': self.params['Interval'],
                'Amount': self.params['Trade_Amount'],
                'TP %': self.params['Take_Profit_PCT'],
                'SL %': self.params['Stop_Loss_PCT'],
                'Maker Fee %': self.params['Maker_Fee_PCT'],
                'Taker Fee %': self.params['Taker_Fee_PCT'],
                'Strategy': self.params['Strategy'],

                'Wins': nb_wins,
                'Losses': nb_losses,
                'Total Trades': total_trades,
                'Success Rate': f'{success_rate:.1f}%',
                'Loss Idx': min_win_loose_index,
                'Win Idx': max_win_loose_index,
                'Wins $': total_wins,
                'Losses $': total_losses,
                'Fees $': total_fees_paid,
                'Total P/L': total_wins + total_losses - total_fees_paid
            },
            ignore_index=True,
        )

        return self.df

