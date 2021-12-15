import numpy as np
import datetime as dt
from utils import convert_excel_to_dataframe

# Print parameter values.
# 'all'=True prints all values
# 'all'=False prints only relevant ones (default)
import config
def print_parameters(params, all=False):
    if all:
        print('------------------------ Params ---------------------------')
        for key, value in params.items():
            print(f'{key}: {value}')
        print('-------------------------------------------------------')
    else:
        print('-------------------------------------------------------')
        print(f'SYMBOL: {params["Symbol"]}')
        print(f'FROM_TIME: {params["From_Time"]}')
        print(f'TO_TIME: {params["To_Time"]}')
        print(f'INTERVAL: {params["Interval"]}')
        print(f'TRADE_AMOUNT: {params["Trade_Amount"]}')
        print(f'TAKE_PROFIT_PCT: {params["Take_Profit_PCT"]}%')
        print(f'STOP_LOSS_PCT: {params["Stop_Loss_PCT"]}%')
        print(f'MAKER_FEE_PCT: {params["Maker_Fee_PCT"]}%')
        print(f'TAKER_FEE_PCT: {params["Taker_Fee_PCT"]}%')
        print('-------------------------------------------------------')


def validate_params(params):
    if not isinstance(params['From_Time'], dt.datetime):
        raise Exception(f'Invalid Parameter [From_Time] = {params["From_Time"]}')

    if not isinstance(params['To_Time'], dt.datetime):
        raise Exception(f'Invalid Parameter [To_Time] = {params["To_Time"]}')

    if params["Interval"] not in ["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W"]:
        raise Exception(f'Invalid Parameter [Interval] = {params["Interval"]}')

    trade_amount = params["Trade_Amount"]
    if not isinstance(trade_amount, float) or trade_amount <= 0:
        raise Exception(f'Invalid Parameter [Trade_Amount] = {trade_amount}. Must be a positive value of type float.')

    take_profit_pct = params["Take_Profit_PCT"]
    if not isinstance(take_profit_pct, float) or take_profit_pct <= 0:
        raise Exception(
            f'Invalid Parameter [Take_Profit_PCT] = {take_profit_pct}. Must be a positive value of type float.')

    stop_loss_pct = params["Stop_Loss_PCT"]
    if not isinstance(stop_loss_pct, float) or stop_loss_pct <= 0:
        raise Exception(f'Invalid Parameter [Stop_Loss_PCT] = {stop_loss_pct}. Must be a positive value of type float.')

    maker_fee_pct = params["Maker_Fee_PCT"]
    if not isinstance(maker_fee_pct, float):
        raise Exception(f'Invalid Parameter [Maker_Fee_PCT] = {maker_fee_pct}. Must be a positive value of type float.')

    taker_fee_pct = params["Taker_Fee_PCT"]
    if not isinstance(taker_fee_pct, float):
        raise Exception(f'Invalid Parameter [Taker_Fee_PCT] = {taker_fee_pct}. Must be a positive value of type float.')

    if not isinstance(params['Precision_Crossing'], bool):
        raise Exception(f'Invalid Parameter [Precision_Crossing] = {params["Precision_Crossing"]}')

    # Convert all items to lower case
    formats_list = config.OUTPUT_FILE_FORMAT
    if not isinstance(formats_list, list) or len(config.OUTPUT_FILE_FORMAT) == 0:
        raise Exception(f'Invalid Global Setting [OUTPUT_FILE_FORMAT] = {formats_list}.')
    config.OUTPUT_FILE_FORMAT = [x.lower() for x in config.OUTPUT_FILE_FORMAT]


def load_test_cases_from_file(filename):
    print(f'Loading test cases from file => [{filename}]')
    df = convert_excel_to_dataframe(filename)
    df['Interval'] = df['Interval'].astype(str)
    df['Trade Amount'] = df['Trade Amount'].astype(float)
    df['TP %'] = df['TP %'].astype(float)
    df['SL %'] = df['SL %'].astype(float)
    df['Maker Fee %'] = df['Maker Fee %'].astype(float)
    df['Taker Fee %'] = df['Taker Fee %'].astype(float)
    # Convert str to bool. astype(bool) does not work
    df['Precision Crossing'] = np.where(df['Precision Crossing'] == 'True', True, False)
    return df