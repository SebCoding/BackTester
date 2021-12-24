from abc import ABC, abstractmethod

# Abstract Exchange Class
class IStrategy(ABC):

    NAME = 'abstract'

    # Percentage of the wallet that will be used for each trade
    # Amount must be between 0 and 1.
    STAKE_AMOUNT_PCT = 0.0

    # Used to output on console a dot for each trade processed.
    # Used as limited output progress bar
    USE_DOT_PROGRESS_OUTPUT = True
    PROGRESS_COUNTER_MAX = 100

    def __init__(self, exchange, params, df):
        self.exchange = exchange
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

