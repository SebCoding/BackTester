import api_keys

# Global Application Settings
TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
HISTORICAL_FILES_PATH = 'ExchangeData'  # Folder location where to store the exchange's original raw data
RESULTS_PATH = 'Output'  # Folder location where to store the back testing output
OUTPUT_FILE_FORMAT = ['xlsx']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']
MIN_DATA_SIZE = 201  # Cannot run Strategy on data set less than this value

# Supported File Formats
SUPPORTED_FILE_FORMATS = ['csv', 'xlsx']

# Supported Exchange List
SUPPORTED_EXCHANGES = ['ByBit']

# Implemented Strategies
IMPLEMENTED_STRATEGIES = ['MACD', 'MACD Precise']

