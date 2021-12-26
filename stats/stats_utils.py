import pandas as pd


def determine_win_or_loose(row):
    if row['win'] != 0:
        return 'W'
    elif row['loss'] != 0:
        return 'L'
    else:
        return None


# Returns 2 values.
# 1) max_wins: Maximum number of consecutive win trades within the date range
# 2) max_losses: Maximum number of consecutive loss trades within the date range
def get_consecutives(df):
    df_tmp = pd.DataFrame()
    df_tmp['W/L'] = df.apply(determine_win_or_loose, axis=1)
    values = df_tmp['W/L'].values.tolist()
    values = list(filter(None, values))  # Remove nulls

    last_index = len(values) - 1
    count_wins = 0
    count_losses = 0
    max_wins = 0
    max_losses = 0

    for i, val in enumerate(values):
        if val == 'W':
            count_wins += 1
            if i == last_index and count_wins > max_wins:
                max_wins = count_wins
            elif i < last_index and values[i + 1] != val:
                if count_wins > max_wins:
                    max_wins = count_wins
                count_wins = 0
        elif val == 'L':
            count_losses += 1
            if i == last_index and count_losses > max_losses:
                max_losses = count_losses
            elif i < last_index and values[i + 1] != val:
                if count_losses > max_losses:
                    max_losses = count_losses
                count_losses = 0
        # print(f'Index[{i}] Value[{val}] - count_W: {count_W}, count_L: {count_L}, max_W: {max_W}, max_L: {max_L}')
    return max_wins, max_losses


# Returns 2 values.
# 3) min_win_loose_index: Minimum loosing index
# 4) max_win_loose_index: Maximum loosing index
def get_win_loss_indexes(df):
    df_tmp = pd.DataFrame()
    # Part 1: Max consecutive wins or losses
    df_tmp['W/L'] = df.apply(determine_win_or_loose, axis=1)
    values = df_tmp['W/L'].values.tolist()
    values = list(filter(None, values))  # Remove nulls

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

    return min_win_loose_index, max_win_loose_index


def get_initial_statistics_df():
    statistics_df = pd.DataFrame(columns=[
        'Test #',
        'Exchange',
        'Pair',
        'From',
        'To',
        'Interval',
        'Init Capital',
        'TP %',
        'SL %',
        'Maker Fee %',
        'Taker Fee %',
        'Strategy',
        'Wins',
        'Losses',
        'Trades',
        'Success Rate',
        'Loss Idx',
        'Win Idx',
        'Wins $',
        'Losses $',
        'Fees $',
        'Total P/L'
    ])
    # print(results_df.to_string())
    return statistics_df
