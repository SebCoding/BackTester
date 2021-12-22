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


# def init_exchange(params):
#     match params['Exchange']:
#         case 'Binance':
#             return Binance()
#         case 'ByBit':
#             return ByBit()
#         case _:
#             raise Exception(f"Invalid Exchange: {params['Exchange']}.")
#
#
# def init_strategy(params, df, my_exchange):
#     match params['Strategy']:
#         case 'MACD':
#             return MACD(params, df)
#         case 'EarlyMACD':
#             return EarlyMACD(params, df, my_exchange)
#         case _:
#             raise Exception(f"Invalid Strategy: {params['Strategy']}.")


# Run the backtesting for a specific test case (set of parameters)
def backtest(params):
    print(f'---------------------------------------- TEST #{params["Test_Num"]} ----------------------------------------')
    execution_start = time.time()
    # print_parameters(params, True)
    validate_params(params)

    # my_exchange = init_exchange(params)
    my_exchange = globals()[params['Exchange']]()
    df = my_exchange.get_candle_data(params['Test_Num'], params['Symbol'],
                                     params['From_Time'], params['To_Time'], params['Interval'],
                                     include_prior=200, write_to_file=True, verbose=True)
    if df is None:
        print(f'\nNo data was returned from {my_exchange.NAME}. Unable to backtest strategy.')
        raise Exception(f"No data returned by {my_exchange.NAME}")
    elif len(df) <= config.MIN_DATA_SIZE:
        print(f'\nData rows = {len(df)}, less than MIN_DATA_SIZE={config.MIN_DATA_SIZE}. Unable to backtest strategy.')
        raise Exception("Unable to Run Strategy on Data Set")

    # strategy = init_strategy(params, df, my_exchange)
    strategy = globals()[params['Strategy']](params, df, my_exchange)
    strategy.add_indicators_and_signals()
    strategy.process_trades()

    exec_time = time.time() - execution_start
    print(f'Test #{params["Test_Num"]} Execution Time: {exec_time:.1f}s\n')


##################################################################################
### Running the BackTesting
##################################################################################

def main():
    # Load test cases from Excel file
    test_cases_df = load_test_cases_from_file(config.TEST_CASES_FILE_PATH)
    # print(test_cases_df.to_string())

    # Create DataFrame to store results
    results_df = pd.DataFrame(
        columns=['Test #', 'Exchange', 'Symbol', 'From', 'To', 'Interval', 'Amount', 'TP %', 'SL %', 'Maker Fee %',
                 'Taker Fee %',
                 'Strategy', 'Wins', 'Losses', 'Total Trades', 'Success Rate', 'Loss Idx', 'Win Idx',
                 'Wins $', 'Losses $', 'Fees $', 'Total P/L'])
    # print(results_df.to_string())

    # Run back test each test case
    for index, row in test_cases_df.iterrows():
        params = {
            'Test_Num': index
            , 'Exchange': row.Exchange
            , 'Symbol': row.Symbol
            , 'From_Time': row.From
            , 'To_Time': row.To
            , 'Interval': row.Interval
            , 'Trade_Amount': row['Trade Amount']
            , 'Take_Profit_PCT': row['TP %']
            , 'Stop_Loss_PCT': row['SL %']
            , 'Maker_Fee_PCT': row['Maker Fee %']
            , 'Taker_Fee_PCT': row['Taker Fee %']
            , 'Strategy': row['Strategy']

            , 'Results': results_df
        }

        backtest(params)
        results_df = params['Results']

    # Save results to file
    results_df = results_df.set_index('Test #')
    if 'csv' in config.OUTPUT_FILE_FORMAT:
        filename = config.RESULTS_PATH + '\\' + 'Statistics.csv'
        results_df.to_csv(filename, index=True, header=True)
        print(f'Stats file created => [{filename}]')

    if 'xlsx' in config.OUTPUT_FILE_FORMAT:
        filename = config.RESULTS_PATH + '\\' + 'Statistics.xlsx'
        results_df.to_excel(filename, index=True, header=True)
        print(f'Stats file created => [{filename}]')

    # Display Results DataFrame to Console
    print(results_df.to_markdown())


if __name__ == "__main__":
    main()
