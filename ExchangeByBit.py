import datetime as dt

import pandas as pd
from pybit import HTTP

import utils
from IExchange import IExchange


class ExchangeByBit(IExchange):
    NAME = 'ByBit'

    interval_map = {
        "1": "1",
        "3": "3",
        "5": "5",
        "15": "15",
        "30": "30",
        "60": "60",   # 1 Hour
        "120": "120", # 2 Hours
        "240": "240", # 4 Hours
        "360": "360", # 6 Hours
        "720": '720', # 12 Hours
        "D": "D",
        "W": "W"
    }

    def __init__(self):
        super().__init__()
        self.my_api_endpoint = 'https://api.bybit.com'
        # Unauthenticated
        self.session_unauth = HTTP(endpoint=self.my_api_endpoint)
        # Authenticated
        self.session_auth = HTTP(endpoint=self.my_api_endpoint, api_key=self.my_api_key, api_secret=self.my_api_secret)

    def get_candle_data(self, test_num, symbol, from_time, to_time, interval, include_prior=0, write_to_file=True, verbose=False):
        # The issue with ByBit API is that you can get a maximum of 200 bars from it.
        # So if you need to get data for a large portion of the time you have to call it multiple times.

        if verbose:
            # print(f'Fetching {symbol} data from ByBit. Interval [{interval}], From[{from_time.strftime("%Y-%m-%d")}], To[{to_time.strftime("%Y-%m-%d")}].')
            print(f'Fetching {symbol} data from {self.NAME}. Interval [{interval}], From[{from_time}], To[{to_time}]')

        df_list = []
        start_time = from_time

        # Adjust from_time for example to add 200 additional prior entries for ema200
        if include_prior > 0:
            start_time = utils.adjust_from_time(from_time, interval, include_prior)

        last_datetime_stamp = start_time.timestamp()
        to_time_stamp = to_time.timestamp()

        while last_datetime_stamp < to_time_stamp:
            # print(f'Fetching next 200 lines fromTime: {last_datetime_stamp} < to_time: {to_time}')
            # print(f'Fetching next 200 lines fromTime: {dt.datetime.fromtimestamp(last_datetime_stamp)} < to_time: {dt.datetime.fromtimestamp(to_time)}')
            result = self.session_auth.query_kline(symbol=symbol, interval=interval, **{'from': last_datetime_stamp})[
                'result']
            tmp_df = pd.DataFrame(result)

            if tmp_df is None or (len(tmp_df.index) == 0):
                break

            tmp_df.index = [dt.datetime.fromtimestamp(x) for x in tmp_df.open_time]
            # tmp_df.index = [dt.datetime.utcfromtimestamp(x) for x in tmp_df.open_time]
            df_list.append(tmp_df)
            last_datetime_stamp = float(max(tmp_df.open_time) + 1)  # Add 1 sec to last data received

            # time.sleep(2) # Sleep for x seconds, to avoid being locked out

        if df_list is None or len(df_list) == 0:
            return None

        df = pd.concat(df_list)

        # Drop rows that have a timestamp greater than to_time
        df = df[df.open_time <= int(to_time.timestamp())]

        # Only keep relevant columns OHLC(V)
        df = df[['symbol', 'open', 'close', 'high', 'low', 'volume']]

        # Set proper data types
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # Write to file
        if write_to_file:
            utils.save_dataframe2file(test_num, self.NAME, symbol, from_time, to_time, self.interval_map[interval], df,
                                      exchange_data_file=True,
                                      include_time=True if interval == '1' else False,
                                      verbose=False)
        return df

# Testing Class
# ex = ExchangeByBit()
# from_time = pd.Timestamp(year=2021, month=10, day=1)
# to_time = pd.Timestamp(year=2021, month=12, day=31)
# ex.get_candle_data(0, 'BTCUSDT', from_time, to_time, "60", include_prior=0, write_to_file=True, verbose=True)