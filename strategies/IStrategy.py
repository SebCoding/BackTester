from abc import ABC, abstractmethod

# Abstract Exchange Class
class IStrategy(ABC):

    NAME = 'abstract'

    # Used to output on console a dot for each trade processed.
    # Used as limited output progress bar
    PROGRESS_COUNTER_MAX = 100
    USE_DOT_PROGRESS_OUTPUT = True

    def __init__(self, params, df):
        self.params = params
        self.df = df
        self.progress_counter = 0

    # Calculate indicator values required to determine long/short signals
    @abstractmethod
    def add_indicators_and_signals(self):
        pass

    # Mark start, ongoing and end of trades, as well as calculate statistics
    @abstractmethod
    def process_trades(self):
        pass


    # Call this method each time a is processed to update progress on console
    def update_progress_dots(self):
        if self.USE_DOT_PROGRESS_OUTPUT:
            print('.', end='')
            self.progress_counter += 1
            if self.progress_counter > self.PROGRESS_COUNTER_MAX:
                self.progress_counter = 0
                print()

