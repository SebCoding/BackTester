##################################################################################
### Statistics
##################################################################################
import locale

def print_trade_stats(total_wins, total_losses, nb_wins, nb_losses, total_fees_paid,
                      max_conseq_wins, max_conseq_losses, min_win_loose_index, max_win_loose_index):
    total_trades = nb_wins + nb_losses
    success_rate = (nb_wins / total_trades * 100) if total_trades != 0 else 0
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    print(f'\n-------------------- Statistics --------------------')
    print(f'Winning Trades: {nb_wins}')
    print(f'Max # Consecutive Wins: {max_conseq_wins}')
    print('---')
    print(f'Losing Trades: {nb_losses}')
    print(f'Max # Consecutive Losses: {max_conseq_losses}')
    print('---')
    print(f'Total Trades: {total_trades}')
    print(f'Success Rate: {success_rate:.1f}%')
    print(f'Win/Loose Index: Min[{min_win_loose_index}] Max[{max_win_loose_index}]')
    print()
    print(f'Total Wins: {locale.currency(total_wins, grouping=True)}')
    print(f'Total Losses: {locale.currency(total_losses, grouping=True)}')
    print(f'Total Fees Paid: {locale.currency(total_fees_paid, grouping=True)}')
    print(f'Total P/L: {locale.currency(total_wins + total_losses - total_fees_paid, grouping=True)}\n')
    # print('-------------------------------------------------------')

def determine_win_or_loose(row):
    if row['win'] != 0:
        return 'W'
    elif row['loss'] != 0:
        return 'L'
    else:
        return None

# Returns 4 values.
# 1) Maximum number of consecutive win trades within the date range
# 2) Maximum number of consecutive loss trades within the date range
# 3) Minimum loosing index
# 4) Maximum loosing index
def analyze_win_lose(df):
    # Part 1: Max consecutive wins or losses
    df['W/L'] = df.apply(determine_win_or_loose, axis=1)
    values = df['W/L'].values.tolist()
    values = list(filter(None, values))  # Remove nulls

    last_index = len(values) - 1
    count_W = 0
    count_L = 0
    max_W = 0
    max_L = 0

    for i, val in enumerate(values):
        if val == 'W':
            count_W += 1
            if i == last_index and count_W > max_W:
                max_W = count_W
            elif i < last_index and values[i + 1] != val:
                if count_W > max_W:
                    max_W = count_W
                count_W = 0
        elif val == 'L':
            count_L += 1
            if i == last_index and count_L > max_L:
                max_L = count_L
            elif i < last_index and values[i + 1] != val:
                if count_L > max_L:
                    max_L = count_L
                count_L = 0
        # print(f'Index[{i}] Value[{val}] - count_W: {count_W}, count_L: {count_L}, max_W: {max_W}, max_L: {max_L}')

    # Part 2: Win/Loose Index Metric
    win_loose_index = 0
    min_win_loose_index = 0
    max_win_loose_index = 0

    for i, val in enumerate(values):
        if val == 'W':
            win_loose_index += 1
        elif val == 'L':
            win_loose_index -= 1

        if win_loose_index > max_win_loose_index:
            max_win_loose_index = win_loose_index
        elif win_loose_index < min_win_loose_index:
            min_win_loose_index = win_loose_index

        # print(f'[{i}][{val}]: current[{win_loose_index}] min[{min_win_loose_index}] max[{max_win_loose_index}]')

    return max_W, max_L, min_win_loose_index, max_win_loose_index