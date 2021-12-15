import datetime as dt


import numpy as np
import pandas as pd
import talib
from pybit import HTTP

import config
import stats
import utils
from params import validate_params, load_test_cases_from_file

# Ignore warnings when reading xlsx file containing list of values for dropdown
import warnings
warnings.filterwarnings('ignore')

##################################################################################
### Get Data from the ByBit API
##################################################################################
def get_bybit_kline_data(test_num, symbol, from_time, to_time, interval, include_prior_200=True, write_to_file=True, verbose=True):
    # Unauthenticated
    # session_unauth = HTTP(endpoint=api_endpoint)

    # Authenticated
    session_auth = HTTP(
        endpoint = config.api_endpoint,
        api_key = config.my_api_key,
        api_secret = config.my_api_secret
    )

    # The issue with ByBit API is that you can get a maximum of 200 bars from it.
    # So if you need to get data for a large portion of the time you have to call it multiple times.

    if verbose:
        # print(f'Fetching {symbol} data from ByBit. Interval [{interval}], From[{from_time.strftime("%Y-%m-%d")}], To[{to_time.strftime("%Y-%m-%d")}].')
        print(f'Fetching {symbol} data from ByBit. Interval [{interval}], From[{from_time}], To[{to_time}]')

    df_list = []
    start_time = from_time

    # Adjust from_time to add 200 additional prior entries for ema200
    if include_prior_200:
        start_time = utils.adjust_from_time(from_time, interval)

    last_datetime_stamp = start_time.timestamp()
    to_time_stamp = to_time.timestamp()

    while last_datetime_stamp < to_time_stamp:
        # print(f'Fetching next 200 lines fromTime: {last_datetime_stamp} < to_time: {to_time}')
        # print(f'Fetching next 200 lines fromTime: {dt.datetime.fromtimestamp(last_datetime_stamp)} < to_time: {dt.datetime.fromtimestamp(to_time)}')
        result = session_auth.query_kline(symbol=symbol, interval=interval, **{'from': last_datetime_stamp})['result']
        tmp_df = pd.DataFrame(result)

        if tmp_df is None or (len(tmp_df.index) == 0):
            break

        tmp_df.index = [dt.datetime.fromtimestamp(x) for x in tmp_df.open_time]
        # tmp_df.index = [dt.datetime.utcfromtimestamp(x) for x in tmp_df.open_time]
        df_list.append(tmp_df)
        last_datetime_stamp = float(max(tmp_df.open_time) + 1)  # Add 1 sec to last data received

        # time.sleep(2) # Sleep for x seconds, to avoid being locked out

    if df_list is None or len(df_list) == 0:
        return None

    df = pd.concat(df_list)

    # Drop rows that have a timestamp greater than to_time
    df = df[df.open_time <= int(to_time.timestamp())]

    # Write to file
    if write_to_file:
        utils.save_dataframe2file(test_num, symbol, from_time, to_time, interval, df, True, True)

    return df

##################################################################################
### Calculate the Indicators and Signals
##################################################################################
# ----------------------------------------------------------------------
# Function used determine trade entries (long/short)
# ----------------------------------------------------------------------
def trade_entries(open, ema200, macdsignal, cross):
    if open >= ema200 and macdsignal < 0 and cross == -1:
        return "Enter Long"
    elif open < ema200 and macdsignal > 0 and cross == 1:
        return "Enter Short"
    return None


# ----------------------------------------------------------------------
# Calculate and add indicators and signals to the DataFrame
# ----------------------------------------------------------------------
def add_indicators_and_signals(params, df):
    print('Adding indicators and Signals to Data.')

    # Set proper data types
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    # df['volume'] = df['volume'].astype(np.int64)

    # Keep only this list of columns, delete all other columns
    final_table_columns = ['symbol', 'interval', 'open', 'high', 'low', 'close']
    df = df[df.columns.intersection(final_table_columns)]

    ## MACD - Moving Average Convergence/Divergence
    tmp = pd.DataFrame()
    tmp['macd'], tmp['macdsignal'], tmp['macdhist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26,
                                                                 signalperiod=9)
    tmp.drop(['macdhist'], axis=1, inplace=True)
    df = df.join(tmp, rsuffix='_right')

    ## EMA - Exponential Moving Average
    df['ema200'] = talib.EMA(df['close'], timeperiod=200)

    # # Remove nulls
    # df.dropna(inplace=True)

    # # Check if price is greater than ema200
    df['GT_ema200'] = np.where(df['open'] > df['ema200'], 'Bull', 'Bear')

    # macdsignal over macd then 1, under 0
    df['O/U'] = np.where(df['macdsignal'] >= df['macd'], 1, 0)

    # macdsignal crosses macd
    df['cross'] = df['O/U'].diff()

    # Drop now useless 'signal_over_under' column
    # df.drop(['signal_over_under'], inplace=True, axis = 1)

    # Remove nulls
    # df.dropna(inplace=True)

    if params['Precision_Crossing']:
        # Enter trade on the same candle as the crossing
        df['trade_status'] = df.apply(lambda x: trade_entries(x['open'], x['ema200'], x['macdsignal'], x['cross']),
                                      axis=1)
    else:
        # Enter trade in the candle after the crossing
        df['trade_status'] = df.apply(lambda x: trade_entries(x['open'], x['ema200'], x['macdsignal'], x['cross']),
                                      axis=1).shift(1)

    # Add and Initialize new columns
    df['entry_time'] = None
    df['entry_price'] = None
    df['take_profit'] = None
    df['stop_loss'] = None
    df['win'] = 0.0
    df['loss'] = 0.0
    df['fee'] = 0.0

    return df



##################################################################################
### Process Trades
##################################################################################
# Find with a minute precision the first point where macd crossed macdsignal
# and return the time and closing price for that point
def find_crossing(df, symbol, from_time, to_time, delta=0):
    # We need to get an extra row to see the value at -1min in case the cross is on the first row
    to_time = to_time - dt.timedelta(minutes=1)

    minutes_df = get_bybit_kline_data(0, symbol, from_time, to_time, 1, include_prior_200=False, write_to_file=False, verbose=True)

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
    price_on_crossing = 0
    time_on_crossing = dt.datetime(1, 1, 1)
    close_col_index = result_df.columns.get_loc("close")
    for i, row in enumerate(result_df.itertuples(index=True), 0):
        if row.cross in [-1, 1]:
            # print(f'Found 1st Crossing at [{i}] + delta[{delta}]')
            price_on_crossing = result_df.iloc[i + delta, close_col_index]
            time_on_crossing = utils.idx2datetime(result_df.index.values[i + delta])
            break

    return time_on_crossing, price_on_crossing


# ----------------------------------------------------------------------------
# Process Trades: Add Trades to the Dataframe, write results to Excel files
# ----------------------------------------------------------------------------
def process_trades(params, df):
    #entry_time = None
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

    print(f'Processing Trades. Precision Crossing[{params["Precision_Crossing"]}]')

    # We use numeric indexing to update values in the DataFrame
    # Find the column indexes
    trade_status_col_index = df.columns.get_loc("trade_status")
    tp_col_index = df.columns.get_loc("take_profit")
    sl_col_index = df.columns.get_loc("stop_loss")
    wins_col_index = df.columns.get_loc("win")
    losses_col_index = df.columns.get_loc("loss")
    fee_col_index = df.columns.get_loc("fee")
    entry_time_col_index = df.columns.get_loc("entry_time")
    entry_price_col_index = df.columns.get_loc("entry_price")

    TP_PCT = params['Take_Profit_PCT'] / 100
    SL_PCT = params['Stop_Loss_PCT'] / 100
    MAKER_FEE_PCT = params['Maker_Fee_PCT'] / 100
    TAKER_FEE_PCT = params['Taker_Fee_PCT'] / 100

    for i, row in enumerate(df.itertuples(index=True), 0):

        # ------------------------------- Longs -------------------------------
        if trade_status == '' and row.trade_status == 'Enter Long':

            # print(f'\nEntering Long: {row.Index}')
            if params['Precision_Crossing']:
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=(utils.convert_interval_to_min(params['Interval'])))
                entry_time, entry_price = find_crossing(df[['close']].iloc[0:i], params['Symbol'], start_time, end_time)
                df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')
            else:
                #start_time = None
                entry_price = row.open

            stop_loss = entry_price - (SL_PCT * entry_price)
            take_profit = entry_price + (TP_PCT * entry_price)
            df.iloc[i, tp_col_index] = take_profit
            df.iloc[i, sl_col_index] = stop_loss
            # Entry Fee
            entry_fee = params['Trade_Amount'] * TAKER_FEE_PCT
            df.iloc[i, fee_col_index] += entry_fee
            total_fees_paid += entry_fee

            # We exit in the same candle we entered, hit stop loss
            if row.low <= stop_loss:
                loss = params['Trade_Amount'] * SL_PCT * -1
                df.iloc[i, trade_status_col_index] = 'Enter/Exit Long'
                df.iloc[i, losses_col_index] = loss
                total_losses += loss
                trade_status = ''
                nb_losses += 1
                # Exit Fee 'loss'
                exit_fee = (params['Trade_Amount'] - loss) * TAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee

            # We exit in the same candle we entered, take profit
            elif row.high >= take_profit:
                win = params['Trade_Amount'] * TP_PCT
                df.iloc[i, trade_status_col_index] = 'Enter/Exit Long'
                df.iloc[i, wins_col_index] = win
                total_wins += win
                trade_status = ''
                nb_wins += 1
                # Exit Fee 'win'
                exit_fee = (params['Trade_Amount'] + win) * MAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee

            # We just entered 'Enter long' in this candle so set the status to 'Long'
            else:
                trade_status = 'Long'

        elif trade_status in ['Long'] and pd.isnull(row.trade_status):
            if row.low <= stop_loss:
                loss = params['Trade_Amount'] * SL_PCT * -1
                df.iloc[i, trade_status_col_index] = 'Exit Long'
                df.iloc[i, losses_col_index] = loss
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                total_losses += loss
                trade_status = ''
                nb_losses += 1
                # Exit Fee 'loss'
                exit_fee = (params['Trade_Amount'] - loss) * TAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            elif row.high >= take_profit:
                win = params['Trade_Amount'] * TP_PCT
                df.iloc[i, trade_status_col_index] = 'Exit Long'
                df.iloc[i, wins_col_index] = win
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                total_wins += win
                trade_status = ''
                nb_wins += 1
                # Exit Fee 'win'
                exit_fee = (params['Trade_Amount'] + win) * MAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            else:
                df.iloc[i, trade_status_col_index] = 'Long'
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                trade_status = 'Long'

        elif trade_status in ['Long'] and row.trade_status in ['Enter Long', 'Enter Short']:
            # If we are in a long and encounter another 'Enter Long' or a 'Enter Short' signal,
            # ignore the signal and override the value with 'Long', we are already in a 'Long' trade
            df.iloc[i, trade_status_col_index] = 'Long'
            df.iloc[i, tp_col_index] = take_profit
            df.iloc[i, sl_col_index] = stop_loss
            # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
            df.iloc[i, entry_price_col_index] = entry_price

        # ------------------------------- Shorts -------------------------------
        elif trade_status == '' and row.trade_status == 'Enter Short':

            # print(f'\nEntering Short: {row.Index}')
            if params['Precision_Crossing']:
                # Find exact crossing and price to the minute
                start_time = utils.idx2datetime(df.index.values[i])
                end_time = start_time + dt.timedelta(minutes=(utils.convert_interval_to_min(params['Interval'])))
                entry_time, entry_price = find_crossing(df[['close']].iloc[0:i], params['Symbol'], start_time, end_time)
                df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                # print(f'entry_time[{entry_time}], entry_price[{entry_price}]')
            else:
                #start_time = None
                entry_price = row.open

            stop_loss = entry_price + (SL_PCT * entry_price)
            take_profit = entry_price - (TP_PCT * entry_price)
            df.iloc[i, tp_col_index] = take_profit
            df.iloc[i, sl_col_index] = stop_loss
            # Entry Fee
            entry_fee = params['Trade_Amount'] * TAKER_FEE_PCT
            df.iloc[i, fee_col_index] += entry_fee
            total_fees_paid += entry_fee

            # We exit in the same candle we entered, hit stop loss
            if row.high >= stop_loss:
                loss = SL_PCT * params['Trade_Amount'] * -1
                df.iloc[i, trade_status_col_index] = 'Enter/Exit Short'
                df.iloc[i, losses_col_index] = loss
                total_losses += loss
                trade_status = ''
                nb_losses += 1
                # Exit Fee 'loss'
                exit_fee = (params['Trade_Amount'] + loss) * TAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            # We exit in the same candle we entered, hit take profit
            elif row.low <= take_profit:
                win = params['Trade_Amount'] * TP_PCT
                df.iloc[i, trade_status_col_index] = 'Enter/Exit Short'
                df.iloc[i, wins_col_index] = win
                total_wins += win
                trade_status = ''
                nb_wins += 1
                # Exit Fee 'loss'
                exit_fee = (params['Trade_Amount'] - win) * MAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            # We just entered 'Enter Short' in this candle, so set the status to 'Short'
            else:
                trade_status = 'Short'

        elif trade_status in ['Short'] and pd.isnull(row.trade_status):
            if row.high >= stop_loss:
                loss = SL_PCT * params['Trade_Amount'] * -1
                df.iloc[i, trade_status_col_index] = 'Exit Short'
                df.iloc[i, losses_col_index] = loss
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                total_losses += loss
                trade_status = ''
                nb_losses += 1
                # Exit Fee 'loss'
                exit_fee = (params['Trade_Amount'] + loss) * TAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            elif row.low <= take_profit:
                win = params['Trade_Amount'] * TP_PCT
                df.iloc[i, trade_status_col_index] = 'Exit Short'
                df.iloc[i, wins_col_index] = win
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                total_wins += win
                trade_status = ''
                nb_wins += 1
                # Exit Fee 'win'
                exit_fee = (params['Trade_Amount'] - win) * MAKER_FEE_PCT
                df.iloc[i, fee_col_index] += exit_fee
                total_fees_paid += exit_fee
            else:
                df.iloc[i, trade_status_col_index] = 'Short'
                df.iloc[i, tp_col_index] = take_profit
                df.iloc[i, sl_col_index] = stop_loss
                # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
                df.iloc[i, entry_price_col_index] = entry_price
                trade_status = 'Short'

        elif trade_status in ['Short'] and row.trade_status in ['Enter Long', 'Enter Short']:
            # If we are in a long and encounter another 'Enter Long' or a 'Enter Short' signal,
            # ignore the signal and override the value with 'Long', we are already in a 'Short' trade
            df.iloc[i, trade_status_col_index] = 'Short'
            df.iloc[i, tp_col_index] = take_profit
            df.iloc[i, sl_col_index] = stop_loss
            # df.iloc[i, entry_time_col_index] = entry_time.strftime('%H:%M')
            df.iloc[i, entry_price_col_index] = entry_price

    # Remove nulls
    # df.dropna(inplace=True)
    #df = df.loc[df['macd'] != None]
    df = df.loc[df['macd'].apply(lambda x: x is not None)]

    # Save trade details to file
    utils.save_dataframe2file(params['Test_Num'], params['Symbol'], params['From_Time'], params['To_Time'], params['Interval'], df, False, True)

    max_conseq_wins, max_conseq_losses, min_win_loose_index, max_win_loose_index = stats.analyze_win_lose(df)

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
    params['Results'] = params['Results'].append(
        {
            'Test #': params['Test_Num'],
            'Symbol': params['Symbol'],
            'From': params['From_Time'].strftime("%Y-%m-%d"),
            'To': params['To_Time'].strftime("%Y-%m-%d"),
            'Interval': params['Interval'],
            'Amount': params['Trade_Amount'],
            'TP %': params['Take_Profit_PCT'],
            'SL %': params['Stop_Loss_PCT'],
            'Maker Fee %': params['Maker_Fee_PCT'],
            'Taker Fee %': params['Taker_Fee_PCT'],
            'Precision Crossing': params['Precision_Crossing'],

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

    return df

##################################################################################
###
##################################################################################

# def create_empty_results_df():
#     df = pd.DataFrame(
#         columns=['Test #', 'Symbol', 'From', 'To', 'Interval', 'Amount', 'TP %', 'SL %', 'Maker Fee %', 'Taker Fee %',
#                  'Precision Crossing', 'Wins', 'Losses', 'Total Trades', 'Success Rate', 'Loss Idx', 'Win Idx',
#                  'Wins $', 'Losses $', 'Fees $', 'Total P/L'])
#     return df

# Run the backtesting
def backtest(params):
    print(f'----------------------- TEST #{params["Test_Num"]} -----------------------')
    # print_parameters(params, True)
    validate_params(params)

    # Method 1 (slow): Get historical data directly from the ByBit API
    # --------------------------------------------------------------------
    df = get_bybit_kline_data(params['Test_Num'], params['Symbol'], params['From_Time'], params['To_Time'], params['Interval'])
    if df is None:
        print(f'\nNo data was returned from ByBit. Unable to backtest strategy.')
        raise Exception("No data returned by ByBit")
    elif len(df) <= config.MIN_DATA_SIZE:
        print(f'\nData rows = {len(df)}, less than MIN_DATA_SIZE={config.MIN_DATA_SIZE}. Unable to backtest strategy.')
        raise Exception("Unable to Run Strategy on Data Set")

    # Method 2 (fast): Get historical data from previously saved files
    # --------------------------------------------------------------------
    # filename = 'ByBitData\\BTCUSDT_2021-01-01_to_2021-11-27_30.xlsx'
    # print(f'Reading data from file => [{filename}]')
    # df = convertExcelToDataFrame(filename)

    df = add_indicators_and_signals(params, df)
    df = process_trades(params, df)

    # for index, row in df.iterrows():
    #     print(index, row['close'], row['macd'])
    return df

##################################################################################
### Running the BackTesting
##################################################################################

def main():
    # Load test cases from Excel file
    test_cases_df = load_test_cases_from_file(config.TEST_CASES_FILE_PATH)
    # print(test_cases_df.to_string())

    # Create DataFrame to store results
    results_df = pd.DataFrame(
        columns=['Test #', 'Symbol', 'From', 'To', 'Interval', 'Amount', 'TP %', 'SL %', 'Maker Fee %', 'Taker Fee %',
                 'Precision Crossing', 'Wins', 'Losses', 'Total Trades', 'Success Rate', 'Loss Idx', 'Win Idx',
                 'Wins $', 'Losses $', 'Fees $', 'Total P/L'])
    # print(results_df.to_string())

    # Run back test each test case
    for index, row in test_cases_df.iterrows():
        params = {
            'Test_Num': index
            , 'Symbol': row.Symbol
            , 'From_Time': row.From
            , 'To_Time': row.To
            , 'Interval': row.Interval
            , 'Trade_Amount': row['Trade Amount']
            , 'Take_Profit_PCT': row['TP %']
            , 'Stop_Loss_PCT': row['SL %']
            , 'Maker_Fee_PCT': row['Maker Fee %']
            , 'Taker_Fee_PCT': row['Taker Fee %']
            , 'Precision_Crossing': row['Precision Crossing']

            , 'Results': results_df
        }

        df = backtest(params)
        results_df = params['Results']

    # Save results to file
    results_df = results_df.set_index('Test #')
    if 'csv' in config.OUTPUT_FILE_FORMAT:
        fname = config.RESULTS_PATH + '\\' + 'Statistics.csv'
        results_df.to_csv(fname, index=True, header=True)
        print(f'Stats file created => [{fname}]')

    if 'xlsx' in config.OUTPUT_FILE_FORMAT:
        fname = config.RESULTS_PATH + '\\' + 'Statistics.xlsx'
        results_df.to_excel(fname, index=True, header=True)
        print(f'Stats file created => [{fname}]')

    # Display Results DataFrame to Console
    print(results_df.to_markdown())


if __name__ == "__main__":
    main()
