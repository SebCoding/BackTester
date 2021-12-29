import ccxt

# Paths and Folder
TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
HISTORICAL_FILES_PATH = 'exchange_data'  # Folder location where to store the exchange's original raw data
RESULTS_PATH = 'output_files'  # Folder location where to store the back testing output

# True: Using local PostgreSQL database to store historical candle data
# False: Getting data directly from exchange's API with limited caching in local files
HISTORICAL_DATA_STORED_IN_DB = True

# File Formats
SUPPORTED_FILE_FORMATS = ['csv', 'xlsx']
OUTPUT_FILE_FORMAT = ['xlsx']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']

# Exchanges
SUPPORTED_EXCHANGES = ['Binance', 'Bybit']
# SUPPORTED_EXCHANGES = ccxt.exchanges

# Valid Intervals. Some intervals are not supported by some exchanges
VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M']

# Bybit does not support 8h interval
# VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w', '1M']

# Implemented Strategies
IMPLEMENTED_STRATEGIES = ['MACD', 'MACD_X', 'ScalpEmaRsiAdx', 'ScalpEmaRsiAdx_X']

