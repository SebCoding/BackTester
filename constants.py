# Location of the config file
CONFIG_FILE = 'config.json'

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
DATETIME_FMT_No_S = '%Y-%m-%d %H:%M'
# Use some_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] to get only 3 digits for ms
DATETIME_FMT_MS = '%Y-%m-%d %H:%M:%S.%f'

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
IMPLEMENTED_STRATEGIES = ['MACD_BB_Freeman', 'MACD', 'MACD_X', 'ScalpEmaRsiAdx', 'ScalpEmaRsiAdx_X', 'UltimateScalper']

# Implemented Exit Strategies
IMPLEMENTED_EXIT_STRATEGIES = ['FixedPCT', 'ExitOnNextEntry']

# JSON configuration schema to validate the config.json file
CONFIG_SCHEMA = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    '$id': 'https://example.com/product.schema.json',
    'title': 'ConfigFileSchema',
    'description': 'json schema to validate config.json file',
    'type': 'object',
    'properties': {
        'trades': {
            'type': 'object',
            'properties': {
                'tradable_ratio': {'type': 'number', 'exclusiveMinimum': 0, 'maximum': 1.0},
                'entry_as_maker': {'type': 'boolean', 'default': False},
                'initial_capital': {'type': 'number', 'exclusiveMinimum': 0},
            },
            'required': ['tradable_ratio', 'entry_as_maker', 'initial_capital']
        },
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
                    'items': {'type': 'string', 'enum': SUPPORTED_FILE_FORMATS},
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
    'required': ['trades', 'output', 'exchange', 'database']
}
