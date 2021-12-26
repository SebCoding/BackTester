"""
    Statistics class.
    An instance of this can be used to store and handle statistics results
"""
import locale


class Statistics:
    def __init__(self):
        self.nb_wins = 0
        self.nb_losses = 0

        self.max_conseq_wins = 0
        self.max_conseq_losses = 0

        self.min_win_loose_index = 0
        self.max_win_loose_index = 0

        # Currency Values
        self.total_wins = float(0)
        self.total_losses = float(0)
        self.total_fees_paid = float(0)

    def get_total_trades(self):
        return self.nb_wins + self.nb_losses

    total_trades = property(get_total_trades)

    def get_success_rate(self):
        if self.total_trades != 0:
            rate = (float(self.nb_wins) / float(self.total_trades)) * 100
            return round(rate, 1)
        else:
            return 0

    success_rate = property(get_success_rate)

    def get_total_pl(self):
        return round(self.total_wins + self.total_losses - self.total_fees_paid, 2)

    total_pl = property(get_total_pl)

    def to_string(self):
        total_trades = self.nb_wins + self.nb_losses
        success_rate = (self.nb_wins / total_trades * 100) if total_trades != 0 else 0
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        print(f'\n-------------------- Statistics --------------------')
        print(f'Winning Trades: {self.nb_wins}')
        print(f'Max # Consecutive Wins: {self.max_conseq_wins}')
        print('---')
        print(f'Losing Trades: {self.nb_losses}')
        print(f'Max # Consecutive Losses: {self.max_conseq_losses}')
        print('---')
        print(f'Total Trades: {total_trades}')
        print(f'Success Rate: {success_rate:.1f}%')
        print(f'Win/Loose Index: Min[{self.min_win_loose_index}] Max[{self.max_win_loose_index}]')
        print()
        print(f'Total Wins: {locale.currency(self.total_wins, grouping=True)}')
        print(f'Total Losses: {locale.currency(self.total_losses, grouping=True)}')
        print(f'Total Fees Paid: {locale.currency(self.total_fees_paid, grouping=True)}')
        print(f'Total P/L: {locale.currency(self.total_pl, grouping=True)}\n')
        print('-------------------------------------------------------')
