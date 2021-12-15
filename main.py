# Ignore warnings when reading xlsx file containing list of values for dropdown
import warnings

import pandas as pd

import ExchangeByBit
import config
from StrategyMACD import StrategyMACD
from StrategyMACDPrecise import StrategyMACDPrecise
from params import validate_params, load_test_cases_from_file

warnings.filterwarnings('ignore')

def get_strategy(params, df, my_exchange):
    match params['Strategy']:
        case 'MACD':
            return StrategyMACD(params, df)
        case 'MACD Precise':
            return StrategyMACDPrecise(params, df, my_exchange)
        case _:
            raise Exception(f"Invalid Strategy: {params['Strategy']}.")

# Run the backtesting for a specific test case (set of parameters)
def backtest(params):
    print(f'----------------------- TEST #{params["Test_Num"]} -----------------------')
    # print_parameters(params, True)
    validate_params(params)

    # Method 1 (slow): Get historical data directly from the ByBit API
    # --------------------------------------------------------------------
    my_exchange = ExchangeByBit.ExchangeByBit()
    df = my_exchange.get_candle_data(params['Test_Num'], params['Symbol'],
                                     params['From_Time'], params['To_Time'], params['Interval'],
                                     include_prior=200, write_to_file=True, verbose=True)
    if df is None:
        print(f'\nNo data was returned from {my_exchange.name}. Unable to backtest strategy.')
        raise Exception(f"No data returned by {my_exchange.name}")
    elif len(df) <= config.MIN_DATA_SIZE:
        print(f'\nData rows = {len(df)}, less than MIN_DATA_SIZE={config.MIN_DATA_SIZE}. Unable to backtest strategy.')
        raise Exception("Unable to Run Strategy on Data Set")

    # Method 2 (fast): Get historical data from previously saved files
    # --------------------------------------------------------------------
    # filename = config.HISTORICAL_FILES_PATH + '\\BTCUSDT_2021-01-01_to_2021-11-27_30.xlsx'
    # print(f'Reading data from file => [{filename}]')
    # df = utils.convert_excel_to_dataframe(filename)

    strategy = get_strategy(params, df, my_exchange)
    strategy.add_indicators_and_signals()
    strategy.process_trades()

##################################################################################
### Running the BackTesting
##################################################################################

def main():
    # Load test cases from Excel file
    test_cases_df = load_test_cases_from_file(config.TEST_CASES_FILE_PATH)
    # print(test_cases_df.to_string())

    # Create DataFrame to store results
    results_df = pd.DataFrame(
        columns=['Test #', 'Exchange', 'Symbol', 'From', 'To', 'Interval', 'Amount', 'TP %', 'SL %', 'Maker Fee %', 'Taker Fee %',
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
