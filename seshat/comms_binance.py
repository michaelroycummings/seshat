## Internal Modules
from seshat.utils import try5times
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Union, Tuple
import requests
import json
import re
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import hashlib  # for signing
import hmac     # for signing


class CommsBinance:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]
        ## Binance API Specifics
        self.url_spot = 'https://api.binance.com'
        self.url_c2c = 'https://fapi.binance.com/fapi'  # C2C means coin / coin (USDT) pairs
        self.url_c2f = 'https://dapi.binance.com/dapi'  # C2F means coin / fiat (USD) pairs

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_BINANCE.value)
        with open(self.DataLoc.File.API_CONFIG.value) as json_file:
            keys = json.load(json_file)['binance']
        self.api_key = keys['api_key']
        self.private_key = keys['private_key']

        ## Constants
        self.kline_intervals = {
            60: '1m',
            180: '3m',
            300: '5m',
            900: '15m',
            1800: '30m',
            3600: '1h',
            7200: '2h',
            14400: '4h',
            21600: '6h',
            28800: '8h',
            43200: '12h',
            86400: '1d',
            259200: '3d',
            604800: '1w',
            2628000: '1M'
        }
        self.quote_assets_c2f = ['USD']


    @staticmethod
    def time_to_unix(time: Union[datetime, pd.Timestamp]) -> int:
        '''
        Takes a datetime.datetime OR a pd.Timestamp and returns
        Binance's time format: unix time in seconds.
        '''
        # Both datetime.datetime and pd.Timestamp have a `.timestamp()` function.
        # If this changes in the future, just do a try, except.
        return int(time.timestamp()) * 1000


    @staticmethod
    def unix_to_dt(unix_milliseconds: int) -> datetime:
        '''
        Takes Binance's time format (unix time in milliseconds) and
        returns a datetime.datetime.
        '''
        return datetime.utcfromtimestamp(unix_milliseconds/1000)


    def _get_signature(self, params: dict) -> str:
        return hmac.new(
            self.private_key.encode("utf-8"),
            urlencode(params).encode("utf-8"),
            hashlib.sha256
        ).hexdigest()


    def interval_to_offset(self, interval: str, multiplier: int) -> Tuple[int, pd.DateOffset]:
        '''
        Takes an interval str, and a multiplier, converts the interval into the
        nearest Binance-accepted interval times (in seconds), multiplies this by the multiplier,
        and returns a pd.DateOffset of that value, as well as the Binance-accepted interval in seconds.
            - If the multiplier is the number of records to return per request,
              then the returned Offset value is the period of time for that API request,
              and can be used in a pd.date_range(freq=offset) to get start/end dates to make
              multiple requests over a large start/end date range.

        Inputs
        ------
        interval : str
            Any float + [s,m,h,d] is allowed. Will convert this to the
            nearest Binance-accepted interval.
            Example: '63s'
        multiplier : int
            Value to multiply the interval by (see output example).
            Example: 1000

        Output
        ------
        ('1m', pd.DateOffset(seconds=60))
        '''
        mapping = {
            's': 'seconds',
            'm': 'minutes',
            'h': 'hours',
            'd': 'days'
        }
        number, interval_str = re.match('(\d*)(\w+)', interval).groups()
        try:
            time_units = mapping[interval_str]
        except KeyError:
            raise Exception(f'Unrecognised time units given. Interval: {interval}.')
        requested_seconds = timedelta(**{time_units: float(number)}).total_seconds()
        accepted_seconds = int(min(self.kline_intervals.keys(), key=lambda x:abs(x - requested_seconds)))
        accepted_interval_str = self.kline_intervals[accepted_seconds]
        offset = pd.DateOffset(seconds=accepted_seconds * multiplier)
        return accepted_interval_str, offset


    def send_request(self, method: str, url: str, headers: dict = {}, params: dict = {}, sign: bool = False):
        '''
        Send a request to the url and handles errors.

        Common Binance API Errors
        -------------------------
        1100: 'Illegal characters found in a parameter.'
            - startTime may be earlier than the earliest requestable period.
        429: Rate limit is hit.
            - Stop sending queries for response.header['Retry-After'] seconds.
        418: Multiple queries were sent after hitting the rate limit.
            - Ban can last from 2 minutes to 3 days

        Output
        ------
        If a problem occurs, returns None.
        Otherwise, returns the API's response.
        '''
        ## Clean Input
        if method  not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise Exception(f'Incorrect "method " given: {method}')

        ## All Requests
        kwargs = {
            'method': method,
            'url': url
        }
        if headers:
            kwargs.update({'headers': headers})
        if params:
            kwargs.update({'params': params})

        ## Signature-specific requests
        if sign:
            headers.update({"Content-Type": "application/json;charset=utf-8", "X-MBX-APIKEY": self.api_key})
            params.update({'signature': self._get_signature(params)})
            kwargs.update({
                'headers': headers,
                'params': params
            })

        ## Send Request and Handle Rate Limits
        while True:
            response = requests.request(**kwargs)

            ## Handle Rate Limits
            if response.status_code == 429:
                time.sleep(float(response.headers['Retry-After']))
                continue
            elif response.status_code == 418:
                message = 'Binance API has RATE-LIMIT BANNED US with status code 418. \
                           This occurs after ignoring their 429 rate-limit error message.'
                self.logger.critical(message)
                time.sleep(float(response.headers['Retry-After']))
                continue
            else:
                break

        ## Parse non-rate limit exceptions
        try:
            data = response.json()
        except Exception:
            self.logger.exception('Error occured from decoding (.json) response from API.')
            data = None
        try:  # check for no data returned
            error_code, error_message = data['code'], data['msg']
            self.logger.error(
                f'API returned error code "{error_code}" with error message "{error_message}". \n\
                url: {url}. \n\
                params: {params}.'
            )
            data = None
        except (TypeError, KeyError):  # this means that request was successful
            pass
        return data


    def get_exchange_timestamp(self) -> int:
        ''' Returns the current server time in unix time, in milliseconds. '''
        data = self.send_request(
            method='GET', url=f'{self.url_spot}/api/v3/time',
            headers={}, params={}
        )
        try:
            timestamp = data['serverTime']
        except Exception:
            self.logger.exception(f'Could not parse server time. Data received from Binance API: {data}.')
            timestamp = None
        if timestamp is None:  # get machine's server time as exchange request failed
            return self.time_to_unix(datetime.now(timezone.utc))
        else:
            return timestamp


    #########################
    #     Market Prices     #
    #########################


    def get_spot_pairs(self):
        '''
        Gets all the spot pairs traded at Binance.

        Endpoint
        --------
        https://binance-docs.github.io/apidocs/spot/en/#exchange-information

        Output
        ------
        pd.DataFrame
            underlying | quote
            -------------------
              'BTC'    | 'USDT'
              'ETH'    | 'USDT'
        '''
        ## Query Trading Pairs
        url = self.url_spot + '/api/v3/exchangeInfo'
        data = self.send_request(method='GET', url=url)

        ## Format Pairs
        pairs = [[d.get('baseAsset'), d.get('quoteAsset')] for d in data['symbols'] if d.get('status') == 'TRADING']
        df = pd.DataFrame(pairs, columns=['underlying', 'quote'])
        df = df.dropna()  # in case they have incomplete data
        return df


    def get_historical_price(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime, interval: str, instrument: str
    ) -> pd.DataFrame:
        '''
        Returns a pd.DataFrame of prices for the requested pairs, interval, and historical time frame.

        Inputs
        ------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')
        start : datetime
        end : datetime
        interval : str
            Format {number}{time unit}
            Where time unit is one of s (second), m (minute), h (hour), d (day)

        Endpoints
        ---------
        https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
        https://binance-docs.github.io/apidocs/delivery/en/#kline-candlestick-data

        Output
        ------
        pd.DataFrame
            Columns = 'open_time', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_asset_volume', 'number_of_trades',
                      'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                      'ignore'
        '''
        ## Clean Inputs
        if instrument not in ['spot', 'perp']:
            raise Exception(f'Value for "instrument" parameter should be one of [spot, perp]. Received "{instrument}.')
        if not isinstance(interval, str):
            raise Exception(f'Value for "interval" parameter should be str. Received "{interval} of type {type(interval)}.')
        if not isinstance(pairs, list):
            pairs = [pairs]
        try:
            pairs = [pair for pair in pairs if len(pair) == 2]
        except Exception:
            raise Exception(f'Pairs argument passed an invalid value. Should contain a list of ("underlying", "quote") tuples. Value passed: {pairs}')

        ## Make date iterables to request the whole start-to-end range
        data_per_request = 1000
        accepted_interval, freq = self.interval_to_offset(interval=interval, multiplier=data_per_request)
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )
        periods = list(periods)
        periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps

        ## Query Data
        prices = []
        prices_pair_index = []
        for pair in pairs:

            ## Select URL
            if instrument == 'spot':
                url = self.url_spot + '/api/v3/klines'
                symbol = pair[0] + pair[1]
            elif pair[1].upper() in self.quote_assets_c2f:
                url = self.url_c2f + '/v1/klines'
                symbol = pair[0] + pair[1] + '_PERP'
            else:
                url = self.url_c2c + '/v1/klines'
                symbol = pair[0] + pair[1]

            for i in range(len(periods))[:-1]:
                payload = {
                    "symbol": symbol,
                    "interval": accepted_interval,
                    "startTime": self.time_to_unix(periods[i]),
                    "endTime": self.time_to_unix(periods[i + 1]),
                    "limit": data_per_request  # max is 1000
                }
                data = self.send_request(method='GET', url=url, params=payload)
                if data is None:
                    continue
                data = [[float(v) for v in l] for l in data]
                prices.extend(data)
                prices_pair_index.extend([list(pair)] * len(data))  # faster way to add underlying and quote to each price data point

        ## Convert data into a DataFrame
        columns=[
            'open_time',  # time at start of kline
            'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
            'ignore'
        ]
        if columns is None:
            df = pd.DataFrame()
        else:
            ## Make DataFrame
            df = pd.DataFrame(data=prices, columns=columns)
            df[['underlying', 'quote']] = pd.DataFrame(data=prices_pair_index, columns=['underlying', 'quote'])

            ## Clean DataFrame
            df['open_time'] = df['open_time'].apply(self.unix_to_dt)
            df['close_time'] = df['close_time'].apply(self.unix_to_dt)
            time_mask = (start <= df.open_time) & (df.open_time < end)
            df = df[time_mask]
            df = df.drop_duplicates()
        return


    def get_historical_spot_price(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime, interval: str
    ) -> pd.DataFrame:
        ''' Proxy function for `get_historical_price` method. '''
        return self.get_historical_price(pairs, start, end, interval, instrument='spot')


    def get_historical_perp_price(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime, interval: str
    ) -> pd.DataFrame:
        ''' Proxy function for `get_historical_price` method. '''
        return self.get_historical_price(pairs, start, end, interval, instrument='perp')


    #########################
    #     Funding Rates     #
    #########################


    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Binance.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        underlying | quote
        -------------------
          'BTC'    | 'USDT'
          'ETH'    | 'USDT'
        '''
        perp_pairs = []
        for url in [
            f'{self.url_c2c}/v1/exchangeInfo',
            f'{self.url_c2f}/v1/exchangeInfo',
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('symbols'):
                if data_dict.get('contractType') == 'PERPETUAL':  # do not consider expiring futures
                    perp_pairs.append([
                        data_dict['baseAsset'].upper(),
                        data_dict['quoteAsset'].upper(),
                    ])
        df = pd.DataFrame(columns=['underlying', 'quote'], data=perp_pairs)
        return df


    def get_next_funding_rate(self, pairs: Union[List[Tuple[str, str]], None]) -> pd.DataFrame:
        '''
        Returns a dict, for requested pairs (or all pairs if NoneType given) listed at Binance,
        of the upcoming funding rates.
            - NOTE that Binance rates are "Estimated Until Paid", so the final rate is not
              known until the begining of its period, when the rate is charged.

        Method Inputs
        -------------
        pairs: a list of tuples (str, str) or NoneType
            Format is ('underlying_asset', 'quote_asset')
            If NoneType, returns all funding rates that Binance has.

        Endpoint Inputs
        ----------------
        C2C: fapi/v1/premiumIndex format:
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSDT
                - NOTE: for this endpoint, expiring futures have a symbol format: ETHUSDT_210625
        C2F: dapi/v1/premiumIndex provides data with this:
            - "symbol": <underlying_asset><quote_asset>_PERP
                - E.g. BTCUSD_PERP
            - "pair": <underlying_asset><quote_asset>
                - E.g. BTCUSD

        Endpoint Output
        --------------
        C2C: [
            {'symbol': 'SUSHIUSDT', 'markPrice': '16.84710000', 'indexPrice': '16.82849111', 'estimatedSettlePrice': '16.82813723', 'lastFundingRate': '0.00044160', 'interestRate': '0.00010000', 'nextFundingTime': 1620403200000, 'time': 1620399721000},
            {'symbol': 'CVCUSDT', 'markPrice': '0.63221192', 'indexPrice': '0.63072313', 'estimatedSettlePrice': '0.63270632', 'lastFundingRate': '0.00073579', 'interestRate': '0.00010000', 'nextFundingTime': 1620403200000, 'time': 1620399721000}
        ]
        C2F: [
            {'symbol': 'XRPUSD_210625', 'pair': 'XRPUSD', 'markPrice': '1.67816237', 'indexPrice': '1.62303728', 'estimatedSettlePrice': '1.62475630', 'lastFundingRate': '', 'interestRate': '', 'nextFundingTime': 0, 'time': 1620399663000},
            {'symbol': 'LTCUSD_PERP', 'pair': 'LTCUSD', 'markPrice': '370.12016832', 'indexPrice': '356.87782325', 'estimatedSettlePrice': '356.72048049', 'lastFundingRate': '', 'interestRate': '', 'nextFundingTime': 0, 'time': 1620399663000}
        ]

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        pairs_lookup = {f'{underlying}{quote}': (underlying, quote) for (underlying, quote) in pairs}
        funding_rates = []

        ## Get C2C (USDT quote) Perpetual Funding Rates
        url = f'{self.url_c2c}/v1/premiumIndex'
        response = requests.request("GET", url).json()
        for data_dict in response:
            pair = data_dict['symbol']
            ## Ignore non-perpetual futures
            if re.match('(\w+?)(_)(\d+)', pair):
                continue  # some expiring futures are also given
            ## Ignore Pairs not requested
            try:
                (underlying, quote) = pairs_lookup[pair]
            except KeyError:
                continue
            ## Parse Funding Rate
            funding_rates.append([
                datetime.utcfromtimestamp(data_dict['nextFundingTime']/1000),
                underlying.upper(),
                quote.upper(),
                float(data_dict['lastFundingRate']),
            ])

        ## Get C2F (USD quote) Perpetual Funding Rates
        url = f'{self.url_c2f}/v1/premiumIndex'
        response = requests.request("GET", url).json()
        for data_dict in response:  # C2F
            contract_id = data_dict['symbol']  # format BTCUSD_PERP
            pair = data_dict['pair']
            ## Ignore non-perpetual futures
            if contract_id.endswith('_PERP') is False:
                continue
            ## Ignore Pairs not requested
            try:
                (underlying, quote) = pairs_lookup[pair]
            except KeyError:
                continue
            ## Parse Funding Rate
            funding_rates.append([
                datetime.utcfromtimestamp(data_dict['nextFundingTime']/1000),
                underlying.upper(),
                quote.upper(),
                float(data_dict['lastFundingRate']),
            ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        return df


    def get_historical_funding_rates(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime
    ) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')
        start : datetime
        end : datetime

        Endpoint Inputs
        ----------------
            C2C: fapi/v1/premiumIndex format:
                - "symbol": "<underlying_asset><quote_asset>
                    - E.g. BTCUSDT
            C2F: dapi/v1/premiumIndex format:
                - "symbol": "<underlying_asset><quote_asset>_PERP
                    - E.g. BTCUSD_PERP

        Endpoint Output
        ---------------
        C2C: [
            {'symbol': 'BTCUSDT', 'fundingTime': 1620345600000, 'fundingRate': '0.00044645'},
            {'symbol': 'BTCUSDT', 'fundingTime': 1620374400000, 'fundingRate': '0.00053313'}
        ]
        C2F: [
            {'symbol': 'BTCUSD_PERP', 'fundingTime': 1620345600004, 'fundingRate': '0.00010000'},
            {'symbol': 'BTCUSD_PERP', 'fundingTime': 1620374400002, 'fundingRate': '0.00010000'}
        ]

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.02 |   'BTC'    |'USDT' |  0.001
        2022.01.02 |   'ETH'    |'USDT' |  0.002
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        ## Make date iterables to request the whole start-to-end range
        data_per_request = 1000  # 1000 is the max; there is no pagination
        freq = pd.DateOffset(hours=(8 * data_per_request))  # Binance funding rate occurs every eight hours
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )
        periods = list(periods)
        periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps

        funding_rates = []

        ## Query per Pair
        for (underlying, quote) in pairs:

            ## Set URL and Binance-specific Symbol
            if quote == 'USDT':
                url = f'{self.url_c2c}/v1/fundingRate'
                payload = {
                    "symbol": f'{underlying}{quote}',
                    "limit": data_per_request
                }
            elif quote == 'USD':
                url = f'{self.url_c2f}/v1/fundingRate'
                payload = {
                    "symbol": f'{underlying}{quote}_PERP',
                    "limit": data_per_request
                }
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Underlying: {underlying}. Quote: {quote}.')
                continue

            ## Send Query for each sub-date range
            for i in range(len(periods))[:-1]:
                payload.update({
                    "startTime": self.time_to_unix(periods[i]),
                    "endTime": self.time_to_unix(periods[i + 1])
                })

                ## Send Query
                data = self.send_request(method='GET', url=url, params=payload)

                ## Pass on failed response
                if data is None:
                    continue
                ## Ignore empty reponses
                if len(data) == 0:
                    continue
                elif isinstance(data, dict):
                    continue

                ## Parse Funding Rates for each time period
                for time_dict in data:
                    funding_rates.append([
                        datetime.utcfromtimestamp(time_dict['fundingTime']/1000),
                        underlying.upper(),
                        quote.upper(),
                        float(time_dict['fundingRate']),
                    ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        df = df.drop_duplicates()
        return df


    #########################
    # Borrow Interest Rates #
    #########################


    def get_borrowable_assets(self) -> List:
        ''' Returns a list of all assets that can be borrowed from Binance. '''
        url = self.url_spot + '/sapi/v1/margin/allAssets'
        data = self.send_request(method='GET', url=url, sign=True)
        assets = [d['assetName'] for d in data if d['isBorrowable'] is True]
        return assets


    def get_current_borrow_rate(self, symbols: List[str]) -> pd.DataFrame:
        '''
        Returns a DataFrame of spot borrow interest rates for today, per asset.
            NOTE: Binance sets interest rates daily.

        Inputs
        ------
        symbols : List[str]
            A list of symbols to get interest rates for.

        Output
        ------
         datetime  | symbol | rate
        ---------------------------
        2022.01.01 | 'BTC'  | 0.001
        2022.01.01 | 'ETH'  | 0.002
        '''
        interest_rates = []
        url = self.url_spot + '/sapi/v1/margin/interestRateHistory'
        for symbol in symbols:
            now = datetime.now(timezone.utc)
            server_time = self.get_exchange_timestamp()
            payload = {
                "asset": symbol,
                "startTime": self.time_to_unix(now - timedelta(days=2)),
                "endTime": self.time_to_unix(now),
                "recvWindow": 60000,
                "timestamp": server_time
            }
            data = self.send_request(method='GET', url=url, params=payload, sign=True)

            ## Ignore assets not supported by Binance
            if data is None:
                continue

            ## Parse Interest Rates
            latest_rate = data[0]
            interest_rates.append([
                datetime.utcfromtimestamp(latest_rate['timestamp']/1000),
                latest_rate['asset'].upper(),
                float(latest_rate['dailyInterestRate']),
            ])
        df = pd.DataFrame(columns=['datetime', 'symbol', 'rate'], data=interest_rates)
        return df


    def get_historical_borrow_rate(self, symbols: List[str], start: datetime, end: datetime) -> pd.DataFrame:
        '''
        Returns a DataFrame of spot borrow interest rates, per asset and per day.
            NOTE: Binance sets interest rates daily.

        Inputs
        ------
        symbols : List[str]
            A list of symbols to get interest rates for.
        start : datetime
        end : datetime

        Output
        ------
         datetime  | symbol | rate
        ----------------------------
        2022.01.02 | 'BTC'  | 0.001
        2022.01.02 | 'ETH'  | 0.002
        2022.01.01 | 'BTC'  | 0.001
        2022.01.01 | 'ETH'  | 0.002
        '''
        ## Make date iterables to request the whole start-to-end range
        data_per_request = 60000  # is the max amount, which covers more than 30 days of interest rate data
        freq = pd.DateOffset(days=30)  # max range is 30 days
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )
        periods = list(periods)
        periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps

        interest_rates = []
        url = self.url_spot + '/sapi/v1/margin/interestRateHistory'

        ## Query per Symbol
        for symbol in symbols:

            ## Query per sub-date range
            for i in range(len(periods))[:-1]:

                now = datetime.now(timezone.utc)
                server_time = self.get_exchange_timestamp()

                payload = {
                    "asset": symbol,
                    "startTime": self.time_to_unix(periods[i]),
                    "endTime": self.time_to_unix(periods[i + 1]),
                    "recvWindow": data_per_request,
                    "timestamp": server_time
                }

                data = self.send_request(method='GET', url=url, params=payload, sign=True)

                ## Ignore assets not supported by Binance
                if data is None:
                    continue

                ## Parse Interest Rates
                for time_dict in data:
                    interest_rates.append([
                        datetime.utcfromtimestamp(time_dict['timestamp']/1000),
                        time_dict['asset'].upper(),
                        float(time_dict['dailyInterestRate']),
                    ])
        df = pd.DataFrame(columns=['datetime', 'symbol', 'rate'], data=interest_rates)
        df = df.drop_duplicates()
        return df


if __name__ == '__main__':
    pass

    ## Example Usage ##

    # CommsBinance().get_next_funding_rate(pairs=[('BOOPEDOOP', 'USDT'), ('BTC', 'USDT'), ('ETH', 'USD')])

    # CommsBinance().get_historical_borrow_rate(symbols=['BTC', 'ETH', 'LHGSDFGSD', 'LUNA', 'BNB'])

    # CommsBinance(
    #     ).get_historical_price(
    #         pairs=[('BTC','USDT'), ('ETH', 'USD'), ('DOT', 'USD')],
    #         start=datetime(2022,4,1,10),
    #         end=datetime(2022,4,7,10),
    #         interval='1h',
    #         instrument='perp')
