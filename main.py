import time
import warnings

from datetime import datetime

import config
import utils
from params import validate_params, load_test_cases_from_file

# Do not remove these imports even if PyCharm says they're unused
from strategies.MACD import MACD
from strategies.MACD_X import MACD_X
from strategies.ScalpEmaRsiAdx import ScalpEmaRsiAdx
from strategies.ScalpEmaRsiAdx_X import ScalpEmaRsiAdx_X

# Ignore warnings when reading xlsx file containing list of values for dropdown
from stats import stats_utils


# Do not delete
# from strategies.Scalping1 import Scalping1
# from strategies.MACD import MACD
# from strategies.EarlyMACD import EarlyMACD


# Run the backtesting for a specific test case (set of parameters)
def backtest(params):
    print(f'=============================================',
          f'TEST #{params["Test_Num"]}',
          '=============================================')
    execution_start = time.time()
    validate_params(params)
    strategy = globals()[params['Strategy']](params)
    strategy.run()

    # print(f'{strategy.exchange.NAME} API request count: {strategy.exchange.CURRENT_REQUESTS_COUNT}')
    exec_time = utils.format_execution_time(time.time() - execution_start)
    print(f'Test #{params["Test_Num"]} Execution Time: {exec_time}\n')


def main():
    # Load test cases from Excel file
    test_cases_df = load_test_cases_from_file(config.TEST_CASES_FILE_PATH)
    # print(test_cases_df.to_string())

    # Create an empty DataFrame with only headers to store Statistics
    statistics_df = stats_utils.get_initial_statistics_df()

    # Disable ResourceWarning, pybit library seems to not be closing its ssl.SSLSocket properly
    warnings.simplefilter("ignore", ResourceWarning)

    # Run back test each test case
    for index, row in test_cases_df.iterrows():
        params = {
            'Test_Num': int(index)
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

    warnings.simplefilter("default", ResourceWarning)

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
