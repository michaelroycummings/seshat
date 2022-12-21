## Internal Modules
from seshat.utils import try5times
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc

## External Libraries
from typing import List, Tuple, Union
import time
import requests
import json
import re
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode  # for signing
import hashlib  # for signing
import hmac     # for signing
import base64   # for signing

class CommsHuobi:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_HUOBI.value)
        self.url_spot = 'https://api.huobi.pro'
        self.url_fut = 'https://api.hbdm.com'  # Same base URL for C2C (USDT) and C2F (USD), but different extentions

        ## API Keys
        with open(self.DataLoc.File.API_CONFIG.value) as json_file:
            keys = json.load(json_file)['huobi']
        self.api_key = keys['api_key']
        self.private_key = keys['private_key']

        ## Constants
        self.kline_intervals = {
            60: '1min',
            300: '5min',
            900: '15min',
            1800: '30min',
            3600: '60min',
            14400: '4hour',
            86400: '1day',
            2628000: '1mon',
        }
        self.quote_assets_c2f = ['USD']


    @staticmethod
    def time_to_unix(time: Union[datetime, pd.Timestamp]) -> int:
        '''
        Takes a datetime.datetime OR a pd.Timestamp and returns
        Huobi's time format: unix time in seconds.
        '''
        # Both datetime.datetime and pd.Timestamp have a `.timestamp()` function.
        # If this changes in the future, just do a try, except.
        return int(time.timestamp())


    @staticmethod
    def unix_to_dt(unix_seconds: int) -> datetime:
        '''
        Takes Huobi's time format (unix time in seconds) and
        returns a datetime.datetime.
        '''
        return datetime.fromtimestamp(unix_seconds)


    def interval_to_offset(self, interval: str, multiplier: int) -> Tuple[int, pd.DateOffset]:
        '''
        Takes an interval str, and a multiplier, converts the interval into the
        nearest Huobi-accepted interval times (in seconds), multiplies this by the multiplier,
        and returns a pd.DateOffset of that value, as well as the Huobi-accepted interval in seconds.
            - If the multiplier is the number of records to return per request,
              then the returned Offset value is the period of time for that API request,
              and can be used in a pd.date_range(freq=offset) to get start/end dates to make
              multiple requests over a large start/end date range.

        Inputs
        ------
        interval : str
            Any float + [m,h,d] is allowed. Will convert this to the
            nearest Binance-accpted interval.
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


    def _get_signature(self, method: str, url: str, params: dict = {}) -> str:
        ''' NOTE: endpoint tail should not include the base_url. E.g., api.huobi.pro '''
        ## Create Phrase to Sign
        endpoint_tail = url.replace(self.url_spot, '').replace(self.url_fut, '')
        sig_params = {
            "AccessKeyId": self.api_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": datetime.utcfromtimestamp(
                self.get_exchange_timestamp()/1000
                ).strftime('%Y-%m-%dT%H:%M:%S')
        }
        front_string = method + "\n" + 'api.huobi.pro' + "\n" + endpoint_tail + "\n"
        phrase = front_string + urlencode(sig_params) + urlencode(params)

        ## Sign Phrase
        signature = hmac.new(
            self.private_key.encode("utf-8"),
            phrase.encode("utf-8"),
            hashlib.sha256
        )
        signature = base64.b64encode(signature.digest()).decode()

        ## Return Signature + Sig Params
        sig_params.update({'Signature': signature})
        return sig_params


    def send_request(self, method: str, url: str, headers: dict = {}, params: dict = {}, sign: bool = False):
        '''
        Send a request to the url and handles errors.

        Common Huobi API Errors
        -------------------------
        ...

        Output
        ------
        If a problem occurs, returns None.
        Otherwise, returns the API's response.
        '''
        ## Clean Input
        if method  not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise Exception(f'Incorrect "method " given: {method }')

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
            more_params = self._get_signature(method=method, url=url, params=params)
            params.update(more_params)
            kwargs.update({'params': params})

        ## Send Request and Handle Rate Limits
        while True:
            response = requests.request(**kwargs)

            ## Handle Rate Limits
            if response.status_code == 429:
                time.sleep(1)
                continue
            else:
                break

        ## Handle Response - copied from https://github.com/ftexchange/ftx/blob/master/rest/client.py#L163
        try:
            data = response.json()
        except Exception:
            self.logger.exception('Error occured from decoding (.json) response from API.')
            result = None
        else:
            if data['status'] == 'ok':
                result = data['data']
            else:
                error_code = data.get('err-code', None)
                error_message = data.get('err-msg', None)
                if None in [error_code, error_message]:
                    self.logger.error(f'API returned an error. Unable to parse error message. All returned data: \n{data}')
                else:
                    self.logger.error(f'API returned an error. Error code: {error_code}. Error message: {error_message}.')
                result = None
        return result


    def get_exchange_timestamp(self) -> int:
        ''' Returns the current server time in unix time, in milliseconds. '''
        timestamp = self.send_request(
            method='GET', url=f'{self.url_spot}/v1/common/timestamp',
            headers={}, params={}
        )

        if timestamp is None:  # get machine's server time as exchange request failed
            return int(datetime.now(timezone.utc).timestamp())
        else:
            return timestamp


    #########################
    #     Market Prices     #
    #########################


    def get_spot_pairs(self):
        '''
        Gets all the spot pairs traded at Huobi.

        Endpoint
        --------
        https://huobiapi.github.io/docs/spot/v1/en/#get-all-supported-trading-symbol-v2

        Output
        ------
        pd.DataFrame
            underlying | quote
            -------------------
              'BTC'    |  'USDT'
              'ETH'    |  'USDT'
        '''
        ## Query Trading Pairs
        url = self.url_spot + '/v2/settings/common/symbols'
        data = self.send_request(method='GET', url=url)

        ## Format Pairs
        pairs = [[d.get('bcdn'), d.get('qcdn')] for d in data if d.get('state') == 'online']
        df = pd.DataFrame(pairs, columns=['underlying', 'quote'])
        df = df.dropna()  # in case they have incomplete data
        return df


    def get_historical_spot_price(self,
        pairs: List[Tuple[str, str]], interval: str, start: Union[datetime, None] = None, end: Union[datetime, None] = None
    ) -> pd.DataFrame:
        '''
        Returns a pd.DataFrame of prices for the requested pairs and interval.
            NOTE: The "start" and "end" parameters only slice and shorten the data returned by Huobi. They cannot be
                  used to increase the range of data returned, because Huobi's spot endpoint takes a number of records
                  to return, but not a start and end date.

        Inputs
        ------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')
        start : datetime
        end : datetime
        interval : str
            Format {number}{time unit}
            Where time unit is one of s (second), m (minute), h (hour), d (day)

        Endpoint Docs
        -------------
        https://docs.huobigroup.com/docs/spot/v1/en/#get-klines-candles

        Output
        ------
        pd.DataFrame
            Columns = 'open_time', 'open', 'close', 'high', 'low', 'amount', 'volume'
        '''
        ### NOTE ###
        # Spot endpoint takes a number of records to return, but not a start and end date.
        # So, they are too different to have as one method.
        ### NOTE ###

        ## Clean Inputs
        if not isinstance(interval, str):
            raise Exception(f'Value for "interval" parameter should be str. Received "{interval} of type {type(interval)}.')
        if not isinstance(pairs, list):
            pairs = [pairs]
        try:
            pairs = [pair for pair in pairs if len(pair) == 2]
        except Exception:
            raise Exception(f'Pairs argument passed an invalid value. Should contain a list of ("underlying", "quote") tuples. Value passed: {pairs}')

        ## Convert given interval to Huobi-Accepted format
        accepted_interval, _ = self.interval_to_offset(interval=interval, multiplier=1)

        ## Query Data
        columns = None
        prices = []
        prices_pair_index = []
        for pair in pairs:

            url = self.url_spot + '/market/history/kline'
            payload = {
                "symbol": f'{pair[0]}{pair[1]}'.lower(),
                "period": accepted_interval,
                "size": 2000  # max says 2000 but USDT-margined API is a princess
            }
            data = self.send_request(method='GET', url=url, params=payload)

            if data is None:
                continue
            if columns is None and data:
                columns = data[0].keys()  # find the order that FTX is returning each candle in (e.g. high,open,close,low)

            prices.extend(data)
            prices_pair_index.extend([list(pair)] * len(data))  # faster way to add underlying and quote to each price data point

        ## Convert data into a DataFrame
        columns_rename = {
            'id': 'open_time',    # The UNIX timestamp at start of kline, in seconds
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'amount': 'amount',  # Accumulated trading volume, in base currency (buy and sell double counted for futures)
            'vol': 'volume',     # Accumulated trading value, in quote currency (buy and sell double counted for futures)
            'count': 'count'     # No. of completed trades (buy and sell double counted for futures)
        }
        if columns is None:
            df = pd.DataFrame()
        else:
            ## Make DataFrame
            df = pd.DataFrame(data=prices, columns=columns)
            df = df.rename(columns=columns_rename)
            df = df[columns_rename.values()]  # remove extra data the api may give (e.g. turnover for USDT-margined only)

            df['open_time'] = df['open_time'].apply(self.unix_to_dt)
            df[['underlying', 'quote']] = pd.DataFrame(data=prices_pair_index, columns=['underlying', 'quote'])

        ## Slice Data for dates requested
        if start is not None:
            time_mask = (start <= df['open_time'])
            df = df[time_mask]
        if end is not None:
            mask = (df['open_time'] <= end)
            df = df[time_mask]

        df = df.drop_duplicates()
        return df


    def get_historical_perp_price(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime, interval: str
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

        Endpoint Docs
        -------------
        https://docs.huobigroup.com/docs/coin_margined_swap/v1/en/#get-kline-data
        https://docs.huobigroup.com/docs/usdt_swap/v1/en/#general-get-kline-data

        Output
        ------
        pd.DataFrame
            Columns = 'open_time', 'open', 'close', 'high', 'low', 'amount', 'volume'
        '''
        ### NOTE ###
        # Spot endpoint takes a number of records to return, but not a start and end date.
        # So, they are too different to have as one method.
        ### NOTE ###

        ## Clean Inputs
        if not isinstance(interval, str):
            raise Exception(f'Value for "interval" parameter should be str. Received "{interval} of type {type(interval)}.')
        if not isinstance(pairs, list):
            pairs = [pairs]
        try:
            pairs = [pair for pair in pairs if len(pair) == 2]
        except Exception:
            raise Exception(f'Pairs argument passed an invalid value. Should contain a list of ("underlying", "quote") tuples. Value passed: {pairs}')

        ## Make date iterables to request the whole start-to-end range
        data_per_request = 1999  # max says 2000 but USDT-margined API is a princess
        accepted_interval, freq = self.interval_to_offset(interval=interval, multiplier=data_per_request)
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )
        periods = list(periods)
        periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps

        ## Query Data
        columns = None
        prices = []
        prices_pair_index = []
        for pair in pairs:

            ## Select URL
            if pair[1].upper() in self.quote_assets_c2f:
                url = self.url_fut + '/swap-ex/market/history/kline'
            else:
                url = self.url_fut + '/linear-swap-ex/market/history/kline'

            for i in range(len(periods))[:-1]:
                # NOTE: if you add a "size" parameter, then "from" and "to" will be ignored
                payload = {
                    "contract_code": f'{pair[0]}_{pair[1]}',
                    "period": accepted_interval,
                    "from": self.time_to_unix(periods[i]),
                    "to": self.time_to_unix(periods[i + 1]),
                }
                data = self.send_request(method='GET', url=url, params=payload)

                if data is None:
                    continue
                if columns is None and data:
                    columns = data[0].keys()  # find the order that FTX is returning each candle in (e.g. high,open,close,low)

                prices.extend(data)
                prices_pair_index.extend([pair] * len(data))  # faster way to add underlying and quote to each price data point

        ## Convert data into a DataFrame
        columns_rename = {
            'id': 'open_time',    # The UNIX timestamp at start of kline, in seconds
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'amount': 'amount',  # Accumulated trading volume, in base currency (buy and sell double counted for futures)
            'vol': 'volume',     # Accumulated trading value, in quote currency (buy and sell double counted for futures)
            'count': 'count'     # No. of completed trades (buy and sell double counted for futures)
        }
        if columns is None:
            df = pd.DataFrame()
        else:
            ## Make DataFrame
            df = pd.DataFrame(data=prices, columns=columns)
            df = df.rename(columns=columns_rename)
            df = df[columns_rename.values()]  # remove extra data the api may give in the future

            df[['underlying', 'quote']] = pd.DataFrame(data=prices_pair_index, columns=['underlying', 'quote'])
            df['open_time'] = df['open_time'].apply(self.unix_to_dt)
            time_mask = (start <= df.open_time) & (df.open_time < end)
            df = df[time_mask]
            df = df.drop_duplicates()

        return df


    #########################
    #     Funding Rates     #
    #########################


    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Huobi.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        underlying | quote
        -------------------
          'BTC'    | 'USDT'
          'ETH'    | 'USDT'
        '''
        perp_pairs = []
        for url, quote_asset in [
            (f'{self.url_fut}/linear-swap-api/v1/swap_batch_funding_rate', 'USDT'),
            (f'{self.url_fut}/swap-api/v1/swap_batch_funding_rate', 'USD')
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('data'):
                if data_dict['funding_rate'] is None:
                    continue  # ignore expiring futures
                perp_pairs.append([
                    data_dict['symbol'].upper(),
                    quote_asset.upper(),
                ])
        df = pd.DataFrame(columns=['underlying', 'quote'], data=perp_pairs)
        return df


    def get_next_funding_rate(self, pairs: Union[List[Tuple[str, str]], None]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of tuples (str, str) or NoneType
            Format is ('underlying_asset', 'quote_asset')
            If NoneType, returns all funding rates that Huobi has.

        Endpoint Inputs
        ----------------
        C2C: /linear-swap-api/v1/swap_batch_funding_rate
            - "contract_code": <underlying_asset>-<quote_asset>
                - E.g. "BTC-USDT"
        C2F: /swap-api/v1/swap_batch_funding_rate
            - "contract_code": <underlying_asset>-<quote_asset>
                - E.g. "BTC-USD"

        Endpoint Output
        ---------------
        C2C: {'status': 'ok', 'data': [
            {'estimated_rate': '0.000961212789852413', 'funding_rate': '0.000334083697871592', 'contract_code': 'OMG-USDT', 'symbol': 'OMG', 'fee_asset': 'USDT', 'funding_time': '1620403200000', 'next_funding_time': '1620432000000'},
            {'estimated_rate': '0.000100000000000000', 'funding_rate': '0.000100000000000000', 'contract_code': 'BAND-USDT', 'symbol': 'BAND', 'fee_asset': 'USDT', 'funding_time': '1620403200000', 'next_funding_time': '1620432000000'}
        ], 'ts': 1620400376124}

        C2F: {'status': 'ok', 'data': [
            {'estimated_rate': '0.000657262872450773', 'funding_rate': '0.000255937364669048', 'contract_code': 'TRX-USD', 'symbol': 'TRX', 'fee_asset': 'TRX', 'funding_time': '1620403200000', 'next_funding_time': '1620432000000'},
            {'estimated_rate': '0.000124803750967047', 'funding_rate': '0.000100000000000000', 'contract_code': 'XLM-USD', 'symbol': 'XLM', 'fee_asset': 'XLM', 'funding_time': '1620403200000', 'next_funding_time': '1620432000000'}
        ], 'ts': 1620400376124}

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        pairs_lookup = {f'{underlying}{quote}': (underlying, quote) for (underlying, quote) in pairs}
        funding_rates = []

        ## Perpetual Funding Rates
        for url in [
            f'{self.url_fut}/linear-swap-api/v1/swap_batch_funding_rate',
            f'{self.url_fut}/swap-api/v1/swap_batch_funding_rate'
        ]:
            for data_dict in requests.request("GET", url).json()['data']:
                pair = data_dict['contract_code'].replace('-', '')  # symbol format from the api: "BTC-USDT"
                ## Check for non-perpetual futures contract
                if re.match('(.+?)(\d\d\d\d\d\d)', pair):
                    continue  # some expiring futures are also given
                try:
                    rate = data_dict['funding_rate']
                except TypeError:
                    continue   # another way to find expiring futures, as they don't have funding rates
                ## Ignore Pairs not requested
                try:
                    (underlying, quote) = pairs_lookup[pair]
                except KeyError:
                    continue
                ## Parse Funding Rate
                funding_rates.append([
                    datetime.utcfromtimestamp(int(data_dict['next_funding_time'])/1000),
                    underlying.upper(),
                    quote.upper(),
                    float(rate),
                ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        return df


    def get_historical_funding_rates(self,
        pairs: List[Tuple[str, str]], start: datetime, end: datetime
    ) -> pd.DataFrame:
        '''
        Get historical funding rates for the requested pairs over the requested period.
            NOTE: The "start" and "end" parameters only slice and shorten the data returned by Huobi. They cannot be
                    used to increase the range of data returned, because Huobi's spot endpoint takes a number of records
                    to return, but not a start and end date.

        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')
        start : datetime
        end : datetime

        Endpoint Inputs
        ---------------
        C2C: /linear-swap-api/v1/swap_historical_funding_rate
            - "contract_code": <underlying_asset>-<quote_asset>
                - E.g. "BTC-USDT"
        C2F: /swap-api/v1/swap_historical_funding_rate
            - "contract_code": <underlying_asset>-<quote_asset>
                - E.g. "BTC-USD"

        Endpoint Output
        --------------
        C2C: {'status': 'ok', 'data': {'total_page': 296, 'current_page': 1, 'total_size': 592, 'data': [
            {'avg_premium_index': '0.000669810145611735', 'funding_rate': '0.000314505536800072', 'realized_rate': '0.000314505536800072', 'funding_time': '1620374400000', 'contract_code': 'BTC-USDT', 'symbol': 'BTC', 'fee_asset': 'USDT'},
            {'avg_premium_index': '0.000814505536800072', 'funding_rate': '0.000984936766188950', 'realized_rate': '0.000984936766188950', 'funding_time': '1620345600000', 'contract_code': 'BTC-USDT', 'symbol': 'BTC', 'fee_asset': 'USDT'}
        ]}, 'ts': 1620400035746}
        C2F: {'status': 'ok', 'data': {'total_page': 612, 'current_page': 1, 'total_size': 1223, 'data': [
            {'avg_premium_index': '0.000771168705453363', 'funding_rate': '0.000100000000000000', 'realized_rate': '0.000100000000000000', 'funding_time': '1620374400000', 'contract_code': 'BTC-USD', 'symbol': 'BTC', 'fee_asset': 'BTC'},
            {'avg_premium_index': '0.000510348612538396', 'funding_rate': '0.000634407182248312', 'realized_rate': '0.000634407182248312', 'funding_time': '1620345600000', 'contract_code': 'BTC-USD', 'symbol': 'BTC', 'fee_asset': 'BTC'}
        ]}, 'ts': 1620399996364}

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.02 |   'BTC'    |'USDT' |  0.001
        2022.01.02 |   'ETH'    |'USDT' |  0.002
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        data_per_request = 50  # max is 50
        funding_rates = []

        ## Query per Pair
        for (underlying, quote) in pairs:

            ## Assign Url
            pair = underlying + quote
            if quote in ['USDT']:
                url = f'{self.url_fut}/linear-swap-api/v1/swap_historical_funding_rate'
            elif quote in ['USD']:
                url = f'{self.url_fut}/swap-api/v1/swap_historical_funding_rate'
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Pair: {pair}.')
                continue

            ## Query Funding Rates, paginated
            page_index = 0
            while True:
                page_index += 1
                payload = {
                    'contract_code': f'{underlying}-{quote}',  # e.g. BTC-USDT
                    'page_index': page_index,
                    'page_size': data_per_request
                }
                response = requests.request("GET", url, params=payload).json()

                ## Handle Pairs that don't exist
                try:
                    data = response['data']['data']
                except KeyError:
                    if response['err_msg'] == 'The contract doesnt exist.' or response['err_code'] == 1332:
                        break
                    else:
                        self.logger.exception('')
                        continue

                ## Break Querying by pagination if no more results are returned
                if len(data) == 0:
                    break

                ## Parse Funding Rates for each time period
                for time_dict in data:
                    funding_rates.append([
                        datetime.utcfromtimestamp(int(time_dict['funding_time'])/1000),
                        underlying.upper(),
                        quote.upper(),
                        float(time_dict['funding_rate']),
                    ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)

        ## Slice Data for dates requested
        if start is not None:
            time_mask = (start <= df['datetime'])
            df = df[time_mask]
        if end is not None:
            mask = (df['datetime'] <= end)
            df = df[time_mask]

        df = df.drop_duplicates()
        return df


    #########################
    # Borrow Interest Rates #
    #########################


    def get_borrowable_assets(self) -> List:
        ''' Returns a list of all cross-margin assets that can be borrowed from Huobi. '''
        url = self.url_spot + '/v1/cross-margin/loan-info'
        data = self.send_request(method='GET', url=url, sign=True)
        assets = [d['currency'].upper() for d in data]
        return assets


    def get_current_borrow_rate(self, symbols: Union[List[str], None] = None) -> pd.DataFrame:
        '''
        Returns a DataFrame of spot borrow interest rates for today, per asset.
            NOTE: Huobi updates interest rates daily, and charges them hourly.

        Inputs
        ------
        symbols : List[str] or NoneType
            A list of symbols to get interest rates for.
            NOTE: all symbols are returned by the endpoint, so this argument will only
                  decrease the amount of data provided.

        Output
        ------
         datetime  | symbol | rate
        ---------------------------
        2022.01.01 | 'BTC'  | 0.001
        2022.01.01 | 'ETH'  | 0.002
        '''
        this_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        ## Query interest Rates
        url = self.url_spot + '/v1/cross-margin/loan-info'
        data = self.send_request(method='GET', url=url, sign=False)

        ## Parse Interest Rate Data
        interest_rates = [[this_day, d.get('currency'), d.get('actual-rate')] for d in data]
        df = pd.DataFrame(columns=['datetime', 'symbol', 'rate'], data=interest_rates)

        ## Clean Rates for Requested Symbols
        if symbols is not None:
            mask = df.symbol.isin(symbols)
            df = df[mask]
        return df




if __name__ == '__main__':
    pass

    ## Example Usage ##

    # CommsHuobi(
    #     ).get_next_funding_rate(
    #         pairs=(("BTC", "USDT"), ("LTC", "USDT"), ("ETH", "USDT")))

    # CommsHuobi().get_historical_spot_price(
    #     pairs=[('BTC','USDT'), ('eth', 'USD'), ('uvdsjka', 'USD')],
    #     start=datetime(2022,4,10,23),
    #     end=datetime(2022,4,28),
    #     interval='1m')
