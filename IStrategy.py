from abc import ABC, abstractmethod

# Abstract Exchange Class
class IStrategy(ABC):

    NAME = 'abstract'

    def __init__(self, params, df):
        self.params = params
        self.df = df

    # Calculate indicator values required to determine long/short signals
    @abstractmethod
    def add_indicators_and_signals(self):
        pass

    # Mark start, ongoing and end of trades, as well as calculate statistics
    @abstractmethod
    def process_trades(self):
        pass

