import api_keys

# Global Application Settings
TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
HISTORICAL_FILES_PATH = 'exchange_data'  # Folder location where to store the exchange's original raw data
RESULTS_PATH = 'output_files'  # Folder location where to store the back testing output
OUTPUT_FILE_FORMAT = ['csv']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']

# Supported File Formats
SUPPORTED_FILE_FORMATS = ['csv', 'xlsx']

# Supported Exchange List
SUPPORTED_EXCHANGES = ['Binance', 'ByBit', 'ByBit2']

# Valid Intervals. Some intervals are not supported by some exchanges
VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w']

# Implemented Strategies
IMPLEMENTED_STRATEGIES = ['MACD', 'EarlyMACD', 'ScalpingEmaRsiAdx']

