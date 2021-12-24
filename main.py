from datetime import datetime
import time
import warnings

import pandas as pd

from exchanges import ByBit
import config
from exchanges.Binance import Binance
from exchanges.ByBit import ByBit
from strategies.MACD import MACD
from strategies.EarlyMACD import EarlyMACD
from params import validate_params, load_test_cases_from_file

# Ignore warnings when reading xlsx file containing list of values for dropdown
warnings.filterwarnings('ignore')


# Run the backtesting for a specific test case (set of parameters)
def backtest(params):
    print(
        f'---------------------------------------- TEST #{params["Test_Num"]} ----------------------------------------')
    execution_start = time.time()
    validate_params(params)

    exchange = globals()[params['Exchange']]()
    df = exchange.get_candle_data(params['Test_Num'], params['Pair'],
                                  params['From_Time'], params['To_Time'], params['Interval'],
                                  include_prior=200, write_to_file=True, verbose=True)
    if df is None:
        print(f'\nNo data was returned from {exchange.NAME}. Unable to backtest strategy.')
        raise Exception(f"No data returned by {exchange.NAME}")
    elif len(df) <= config.MIN_DATA_SIZE:
        print(f'\nData rows = {len(df)}, less than MIN_DATA_SIZE={config.MIN_DATA_SIZE}. Unable to backtest strategy.')
        raise Exception("Unable to Run Strategy on Data Set")

    strategy = globals()[params['Strategy']](exchange, params, df)
    strategy.add_indicators_and_signals()
    strategy.process_trades()

    exec_time = time.time() - execution_start
    print(f'Test #{params["Test_Num"]} Execution Time: {exec_time:.1f}s\n')


def main():
    # Load test cases from Excel file
    test_cases_df = load_test_cases_from_file(config.TEST_CASES_FILE_PATH)
    # print(test_cases_df.to_string())

    # Create DataFrame to store results
    statistics_df = pd.DataFrame(
        columns=['Test #', 'Exchange', 'Pair', 'From', 'To', 'Interval', 'Init Capital', 'TP %', 'SL %', 'Maker Fee %',
                 'Taker Fee %',
                 'Strategy', 'Wins', 'Losses', 'Total Trades', 'Success Rate', 'Loss Idx', 'Win Idx',
                 'Wins $', 'Losses $', 'Fees $', 'Total P/L'])
    # print(results_df.to_string())

    # Run back test each test case
    for index, row in test_cases_df.iterrows():
        params = {
            'Test_Num': index
            , 'Exchange': row.Exchange
            , 'Pair': row.Pair
            , 'From_Time': row.From
            , 'To_Time': row.To
            , 'Interval': row.Interval
            , 'Initial_Capital': row['Initial Capital']
            , 'Take_Profit_PCT': row['TP %']
            , 'Stop_Loss_PCT': row['SL %']
            , 'Strategy': row['Strategy']
            , 'Statistics': statistics_df
        }

        backtest(params)
        statistics_df = params['Statistics']

    # Save results to file
    now = datetime.now().strftime('[%Y-%m-%d] [%H.%M.%S]')
    statistics_df = statistics_df.set_index('Test #')
    if 'csv' in config.OUTPUT_FILE_FORMAT:
        filename = config.RESULTS_PATH + '\\' + f'Statistics - {now}.csv'
        statistics_df.to_csv(filename, index=True, header=True)
        print(f'Stats file created => [{filename}]')

    if 'xlsx' in config.OUTPUT_FILE_FORMAT:
        filename = config.RESULTS_PATH + '\\' + f'Statistics - {now}.xlsx'
        statistics_df.to_excel(filename, index=True, header=True)
        print(f'Stats file created => [{filename}]')

    # Display Results DataFrame to Console
    print(statistics_df.to_markdown())


if __name__ == "__main__":
    main()
