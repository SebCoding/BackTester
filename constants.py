# Location of the config file
CONFIG_FILE = 'config.json'

# Paths and Folder
# TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
# HISTORICAL_FILES_PATH = 'exchange_data'  # Folder location where to store the exchange's original raw data
# RESULTS_PATH = 'output_files'  # Folder location where to store the back testing output

# True: Using local PostgreSQL database to store historical candle data
# False: Getting data directly from exchange's API with limited caching in local files
# HISTORICAL_DATA_STORED_IN_DB = False

# File Formats
SUPPORTED_FILE_FORMATS = ['csv', 'xlsx']
# OUTPUT_FILE_FORMAT = ['xlsx']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']

# Exchanges
SUPPORTED_EXCHANGES = ['Binance', 'Bybit']
# SUPPORTED_EXCHANGES = ccxt.exchanges

# Valid Intervals. Some intervals are not supported by some exchanges
VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M']

# Bybit does not support 8h interval
# VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w', '1M']

# Implemented Strategies
IMPLEMENTED_STRATEGIES = ['MACD', 'MACD_X', 'ScalpEmaRsiAdx', 'ScalpEmaRsiAdx_X']

# JSON configuration schema to validate the config.json file
CONFIG_SCHEMA = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    '$id': 'https://example.com/product.schema.json',
    'title': 'ConfigFileSchema',
    'description': 'json schema to validate config.json file',
    'type': 'object',
    'properties': {
        'output': {
            'type': 'object',
            'properties': {
                'progress_dots': {
                    'type': 'boolean',
                    'default': True
                },
                'test_cases_file_path': {
                    'description': 'File containing test cases',
                    'type': 'string'
                },
                'historical_files_path': {
                    'description': 'Folder location where to store the exchange\'s original raw data',
                    'type': 'string'
                },
                'results_path': {
                    'description': 'Folder location where to store the back testing output',
                    'type': 'string'
                },
                'output_file_format': {
                    'description': 'Preferred format(s) for the output: csv, xlsx or both. Ex: [\'csv\', \'xlsx\']',
                    'type': 'array',
                    'items': {'type': 'string',  'enum': SUPPORTED_FILE_FORMATS},
                    'minItems': 1,
                    'uniqueItems': True
                 }
            },
            'required': [
                'progress_dots',
                'test_cases_file_path',
                'historical_files_path',
                'results_path',
                'output_file_format'
            ]
        },
        'exchange': {
            'type': 'object',
            'properties': {
                'use_testnet': {'type': 'boolean', 'default': False}
            },
            'required': ['use_testnet']
        },
        'database': {
            'type': 'object',
            'properties': {
                'historical_data_stored_in_db': {
                    'description': 'Use local db to store historical data or get it directly from exchange API',
                    'type': 'boolean',
                    'default': True
                },
                'address': {'type': 'string', 'default': 'localhost'},
                'port': {'type': 'integer', 'default': 5432},
                'username': {'type': 'string'},
                'password': {'type': 'string'}
            },
            'required': ['historical_data_stored_in_db', 'address', 'port', 'username', 'password']
        },
    },
    'required': ['output', 'exchange', 'database']
}