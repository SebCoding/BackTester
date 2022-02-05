import warnings
import json
import numpy as np
import datetime as dt

import pandas as pd
import rapidjson

import constants
from Configuration import Configuration
from utils import read_excel_to_dataframe

config = Configuration.get_config()


def print_parameters(params, all=False):
    """
        Print parameter values.
        'all'=True prints all values
        'all'=False prints only relevant ones (default)
    """
    if all:
        print('------------------------ Params ---------------------------')
        for key, value in params.items():
            print(f'{key}: {value}')
        print('-------------------------------------------------------')
    else:
        print('-------------------------------------------------------')
        print(f'PAIR: {params["Pair"]}')
        print(f'FROM_TIME: {params["From_Time"]}')
        print(f'TO_TIME: {params["To_Time"]}')
        print(f'INTERVAL: {params["Interval"]}')
        print(f'INITIAL_AMOUNT: {params["Initial_Capital"]}')
        print(f'TAKE_PROFIT_PCT: {params["Take_Profit_PCT"]}%')
        print(f'STOP_LOSS_PCT: {params["Stop_Loss_PCT"]}%')
        print('-------------------------------------------------------')


def validate_params(params):
    if params['Exchange'].lower() not in [x.lower() for x in constants.SUPPORTED_EXCHANGES]:
        raise Exception(f'Unsupported Exchange = [{params["Exchange"]}].')

    if not isinstance(params['From_Time'], dt.datetime):
        raise Exception(f'Invalid Parameter: From_Time = [{params["From_Time"]}].')

    if not isinstance(params['To_Time'], dt.datetime):
        raise Exception(f'Invalid Parameter: To_Time = [{params["To_Time"]}].')

    if params['From_Time'] > params['To_Time']:
        raise Exception(f'Invalid date range. {params["From_Time"]} must be <= {params["To_Time"]}.')

    if params["Interval"] not in constants.VALID_INTERVALS:
        raise Exception(f'Invalid Parameter: Interval = [{params["Interval"]}].')

    initial_capital = params["Initial_Capital"]
    if not isinstance(initial_capital, float) or initial_capital <= 0:
        raise Exception(
            f'Invalid Parameter: Initial_Capital = [{initial_capital}]. Must be a positive value of type float.')

    take_profit_pct = params["Take_Profit_PCT"]
    if not isinstance(take_profit_pct, float) or take_profit_pct <= 0:
        raise Exception(
            f'Invalid Parameter: Take_Profit_PCT = [{take_profit_pct}]. Must be a positive value of type float.')

    stop_loss_pct = params["Stop_Loss_PCT"]
    if not isinstance(stop_loss_pct, float) or stop_loss_pct <= 0:
        raise Exception(
            f'Invalid Parameter: Stop_Loss_PCT = [{stop_loss_pct}]. Must be a positive value of type float.')

    if params['Strategy'] not in constants.IMPLEMENTED_STRATEGIES:
        raise Exception(f'Invalid Parameter: Unsupported Strategy = [{params["Strategy"]}]')

    if params['Exit_Strategy'] not in constants.IMPLEMENTED_EXIT_STRATEGIES:
        raise Exception(f'Invalid Parameter: Unsupported Exit Strategy = [{params["Exit_Strategy"]}]')

    if params['StrategySettings'] and not isinstance(params['StrategySettings'], dict):
        raise Exception(f'Invalid Optional Strategy Settings.\n '
                        f"Incorrect dictionary format: {params['StrategySettings']}")

    # Convert all items to lower case
    formats_list = config['output']['output_file_format']
    if not isinstance(formats_list, list) or \
            len(formats_list) == 0 or \
            not set(formats_list).issubset(set(constants.SUPPORTED_FILE_FORMATS)):
        raise Exception(f'Invalid Output file format(s): {formats_list}.')
    config['output']['output_file_format'] = [x.lower() for x in config['output']['output_file_format']]


def load_test_cases_from_file(filename):
    print(f'\nLoading test cases from file => [{filename}]')

    # Disable warning because openpyxl issues warnings because the TestCases.xlsx
    # file uses dropdown to enforce integrity of values passed
    warnings.simplefilter("ignore", UserWarning)
    df = read_excel_to_dataframe(filename)
    warnings.simplefilter("default", UserWarning)

    df.dropna(subset=['Exchange'], inplace=True)

    # Adjust column types
    df.index = df.index.astype(int)
    df['From'] = df['From'].astype('datetime64[ns]')
    df['To'] = df['To'].astype('datetime64[ns]')
    df['Interval'] = df['Interval'].astype(str)
    df['TP %'] = df['TP %'].astype(float)
    df['SL %'] = df['SL %'].astype(float)

    # Convert Optional Strategy Settings column to dictionary
    try:
        if df['Optional Strategy Settings'].notnull().sum() != 0:
            df['Optional Strategy Settings'] = df['Optional Strategy Settings'].apply(lambda x: json.loads(x))
    except json.decoder.JSONDecodeError:
        raise Exception(f'Invalid Optional Strategy Settings.\n '
                        f"Incorrect dictionary format: {df['Optional Strategy Settings']}")

    print_df = df.copy()
    print_df['From'] = print_df['From'].apply(lambda x: dt.datetime.strftime(x, constants.DATE_FMT))
    print_df['To'] = print_df['To'].apply(lambda x: dt.datetime.strftime(x, constants.DATE_FMT))

    # Do not print options columns if they are empty
    if print_df['Optional Strategy Settings'].notnull().sum() == 0:
        del print_df["Optional Strategy Settings"]
    print('\n'+print_df.to_string(col_space={'Interval': 9, 'Exit_Strategy': 15})+'\n')
    # print('\n'+df.to_markdown()+'\n')

    return df
