from abc import ABC, abstractmethod

# Base Abstract Strategy Class
class IStrategy(ABC):

    NAME = 'abstract'

    # Ratio of the total account balance the bot is allowed to trade.
    # Positive float between 0.0 and 1.0
    TRADABLE_BALANCE_RATIO = 0.0

    # Used to output on console a dot for each trade processed.
    # Used as limited output progress bar
    USE_DOT_PROGRESS_OUTPUT = True
    PROGRESS_COUNTER_MAX = 90

    # Cannot run Strategy on data set less than this value
    MIN_DATA_SIZE = 0

    def __init__(self, exchange, params, df):
        self.exchange = exchange
        self.params = params
        self.df = df
        self.progress_counter = 0
        self.TP_PCT = self.params['Take_Profit_PCT'] / 100
        self.SL_PCT = self.params['Stop_Loss_PCT'] / 100
        self.MAKER_FEE_PCT = self.exchange.get_maker_fee(params['Pair'])
        self.TAKER_FEE_PCT = self.exchange.get_taker_fee(params['Pair'])

    # Calculate indicator values required to determine long/short signals
    @abstractmethod
    def add_indicators_and_signals(self):
        pass

    # Mark start, ongoing and end of trades, as well as calculate statistics
    @abstractmethod
    def process_trades(self):
        pass

    def get_entry_fee(self, trade_amount):
        return float(trade_amount) * self.MAKER_FEE_PCT

    def get_take_profit_fee(self, trade_amount):
        return float(trade_amount) * self.MAKER_FEE_PCT

    def get_stop_loss_fee(self, trade_amount):
        return float(trade_amount) * self.TAKER_FEE_PCT

    def get_stake_amount(self, amount):
        return amount * self.TRADABLE_BALANCE_RATIO

    # Call this method each time a is processed to update progress on console
    def update_progress_dots(self):
        if self.USE_DOT_PROGRESS_OUTPUT:
            print('.', end='')
            self.progress_counter += 1
            if self.progress_counter > self.PROGRESS_COUNTER_MAX:
                self.progress_counter = 0
                print()

