from abc import ABC, abstractmethod


# Abstract Exchange Class
class Exchange(ABC):

    name = 'abstract'
    my_api_key = None
    my_api_secret = None
    api_endpoint = None

    # Dictionary of symbols used by exchange to define intervals for candle data
    interval_map = { }

    @abstractmethod
    def get_candle_data(self, test_num, symbol, from_time, to_time, interval, include_prior=0, write_to_file=True, verbose=False):
        pass

