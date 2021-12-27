import time

import pandas as pd
import datetime as dt

import api_keys
import utils
from exchanges.IExchange import IExchange
from binance.client import Client
from binance.enums import HistoricalKlinesType


# Using: python-binance
# https://python-binance.readthedocs.io/en/latest/index.html

class Binance(IExchange):
    NAME = 'Binance'

    # Dictionary of pairs used by exchange to define intervals for candle data
    # Binance valid intervals - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    interval_map = {
        "1m": Client.KLINE_INTERVAL_1MINUTE,
        "3m": Client.KLINE_INTERVAL_3MINUTE,
        "5m": Client.KLINE_INTERVAL_5MINUTE,
        "15m": Client.KLINE_INTERVAL_15MINUTE,
        "30m": Client.KLINE_INTERVAL_30MINUTE,
        "1h": Client.KLINE_INTERVAL_1HOUR,
        "2h": Client.KLINE_INTERVAL_2HOUR,
        "4h": Client.KLINE_INTERVAL_4HOUR,
        "6h": Client.KLINE_INTERVAL_6HOUR,
        "8h": Client.KLINE_INTERVAL_8HOUR,
        "12h": Client.KLINE_INTERVAL_12HOUR,
        "D": Client.KLINE_INTERVAL_1DAY,
        "W": Client.KLINE_INTERVAL_1WEEK
    }

    def __init__(self):
        super().__init__()
        self.my_api_key = api_keys.BINANCE_API_KEY
        self.my_api_secret = api_keys.BINANCE_API_SECRET_KEY
        self.client = Client(api_keys.BINANCE_API_KEY, api_keys.BINANCE_API_SECRET_KEY)

    def get_maker_fee(self, pair):
        return 0.0002 * 0.9  # 10% rebate for paying fees in BNB

    def get_taker_fee(self, pair):
        return 0.0004 * 0.9  # 10% rebate for paying fees in BNB

    # from_time and to_time are being passed as pandas._libs.tslibs.timestamps.Timestamp
    # Note: Binance uses 13 digit timestamps as opposed to 10 in our code.
    #       We need to multiply and divide by 1000 to adjust for it
    def get_candle_data(self, test_num, pair, from_time, to_time, interval, include_prior=0, write_to_file=True,
                        verbose=False):
        self.validate_interval(interval)

        # Use locally saved data if it exists
        include_time = False if interval != "1" else True
        cached_df = self.get_cached_exchange_data(pair, from_time, to_time, interval, prior=include_prior,
                                                  include_time=include_time)

        from_time_str = from_time.strftime('%Y-%m-%d')
        to_time_str = to_time.strftime('%Y-%m-%d')
        if cached_df is not None:
            if verbose:
                print(
                    f'Using locally cached data for {pair} from {self.NAME}. Interval [{interval}], From[{from_time_str}], To[{to_time_str}]')
            return cached_df

        if verbose:
            print(
                f'Fetching {pair} data from {self.NAME}. Interval [{interval}], From[{from_time_str}], To[{to_time_str}]')

        start_time = from_time

        # Adjust from_time for example to add 200 additional prior entries for ema200
        if include_prior > 0:
            start_time = utils.adjust_from_time(from_time, interval, include_prior)

        start_datetime_stamp = start_time.timestamp() * 1000
        to_time_stamp = to_time.timestamp() * 1000

        result = self._get_historical_klines(pair, self.interval_map[interval],
                                             int(start_datetime_stamp), int(to_time_stamp))

        # delete unwanted data - just keep date, open, high, low, close, volume
        for line in result:
            del line[6:]

        tmp_df = pd.DataFrame(result, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        # tmp_df.set_index('date', inplace=True)

        tmp_df.index = [dt.datetime.fromtimestamp(x / 1000) for x in tmp_df.date]

        # Drop rows that have a timestamp greater than to_time
        # df = df[df.open_time <= int(to_time.timestamp())]

        # Add pair column
        tmp_df['pair'] = pair

        # Only keep relevant columns OHLC(V)
        tmp_df = tmp_df[['pair', 'open', 'close', 'high', 'low', 'volume']]

        # Set proper data types
        tmp_df['open'] = tmp_df['open'].astype(float)
        tmp_df['high'] = tmp_df['high'].astype(float)
        tmp_df['low'] = tmp_df['low'].astype(float)
        tmp_df['close'] = tmp_df['close'].astype(float)
        tmp_df['volume'] = tmp_df['volume'].astype(float)

        # print(tmp_df.head().to_string())
        # print(len(tmp_df))
        # print(tmp_df.tail().to_string())

        # Write to file
        if write_to_file:
            self.save_candle_data(pair, from_time, to_time, interval, tmp_df, prior=include_prior,
                                  include_time=True if interval == '1' else False, verbose=False)
        return tmp_df

    def _get_historical_klines(self, pair, interval, from_time, to_time):
        attempt_count = 1
        while attempt_count <= self.MAX_RETRIES:
            try:
                # self.random_timeout()
                result = self.client.get_historical_klines(pair, interval, from_time, to_time, limit=1000,
                                                           klines_type=HistoricalKlinesType.FUTURES)
                return result
            except TimeoutError:
                print(
                    f'TimeoutError on {self.NAME}. Attempt {attempt_count}, waiting {self.RETRY_WAIT_TIME}s before retry.')
                time.sleep(self.RETRY_WAIT_TIME)
                attempt_count += 1
        raise TimeoutError(
            f'Unable to fetch data from {self.NAME}. MAX_RETRIES={self.MAX_RETRIES} has been reached.')


# Testing Class
# ex = ExchangeBinance()
# from_time = pd.Timestamp(year=2021, month=6, day=1)
# to_time = pd.Timestamp(year=2021, month=12, day=5)
# ex.get_candle_data(0, 'BTCUSDT', from_time, to_time, "60", include_prior=0, write_to_file=True, verbose=True)
