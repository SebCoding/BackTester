import api_keys

# Global Application Settings
TEST_CASES_FILE_PATH = 'TestCases.xlsx'  # File containing test cases
HISTORICAL_FILES_PATH = 'ByBitData'  # Folder location where to store ByBit original raw data
RESULTS_PATH = 'Output'  # Folder location where to store the back testing output
OUTPUT_FILE_FORMAT = ['xlsx']  # Preferred format(s) for the output: csv, xlsx or both. Ex: ['csv', 'xlsx']
MIN_DATA_SIZE = 201  # Cannot run Strategy on data set less than this value

##################################################################################
### Get keys from the config file
##################################################################################
my_api_key = api_keys.API_KEY
my_api_secret = api_keys.API_SECRET
api_endpoint = 'https://api.bybit.com'
# print(my_api_key)
# print(my_api_secret)