import api_keys

# Global Application Settings
TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
HISTORICAL_FILES_PATH = 'exchange_data'  # Folder location where to store the exchange's original raw data
RESULTS_PATH = 'output_files'  # Folder location where to store the back testing output
OUTPUT_FILE_FORMAT = ['xlsx']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']
MIN_DATA_SIZE = 201  # Cannot run Strategy on data set less than this value

# Supported File Formats
SUPPORTED_FILE_FORMATS = ['csv', 'xlsx']

# Supported Exchange List
SUPPORTED_EXCHANGES = ['Binance', 'ByBit']

# Implemented Strategies
IMPLEMENTED_STRATEGIES = ['MACD', 'EarlyMACD']

