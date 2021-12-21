from abc import ABC, abstractmethod
import api_keys

# Abstract Exchange Class
class IExchange(ABC):

    NAME = 'abstract'

    # Dictionary of symbols used by exchange to define intervals for candle data
    interval_map = None

    def __init__(self):
        self.my_api_key = api_keys.BYBIT_API_KEY
        self.my_api_secret = api_keys.BYBIT_API_SECRET
        self.my_api_endpoint = None

    # from_time and to_time are being passed as pandas._libs.tslibs.timestamps.Timestamp
    @abstractmethod
    def get_candle_data(self, test_num, symbol, from_time, to_time, interval, include_prior=0, write_to_file=True, verbose=False):
        pass

