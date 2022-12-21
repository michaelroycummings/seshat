## Internal Modules
from seshat.utils import try5times
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Union, Tuple
import requests
import json
import re
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
from urllib.parse import urlencode
import hashlib  # for signing
import hmac     # for signing



class CommsFTX:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## API Keys
        with open(self.DataLoc.File.API_CONFIG.value) as json_file:
            keys = json.load(json_file)['ftx']
        self.api_key = keys['api_key']
        self.private_key = keys['private_key']

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_FTX.value)
        self.url = 'https://ftx.com/api'  # Only has C2F (quoted against USD) at the moment

        ## Constants
        self.kline_intervals = [15, 60, 300, 900, 3600, 14400]
        self.kline_intervals.extend(list(range(86400, 86400*30, 86400)))  # aka, 86400, or any multiple of 86400 up to 30*86400


    @staticmethod
    def parse_ftx_datetime_string(string: str):
        parsed_string = re.match('(.+?)(\+\d\d:\d\d)', string).group(1)
        return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')


    @staticmethod
    def time_to_unix(time: Union[datetime, pd.Timestamp]) -> int:
        '''
        Takes a datetime.datetime OR a pd.Timestamp and returns
        FTX's time format: unix time in seconds.
        '''
        # Both datetime.datetime and pd.Timestamp have a `.timestamp()` function.
        # If this changes in the future, just do a try, except.
        return int(time.timestamp())


    def interval_to_offset(self, interval: str, multiplier: int) -> Tuple[int, pd.DateOffset]:
        '''
        Takes an interval str, and a multiplier, converts the interval into the
        nearest FTX-accepted interval times (in seconds), multiplies this by the multiplier,
        and returns a pd.DateOffset of that value, as well as the FTX-accepted interval in seconds.
            - If the multiplier is the number of records to return per request,
              then the returned Offset value is the period of time for that API request,
              and can be used in a pd.date_range(freq=offset) to get start/end dates to make
              multiple requests over a large start/end date range.

        Inputs
        ------
        interval : str
            Any float + [m,h,d] is allowed. Will convert this to the
            nearest FTX-accpted interval.
            Example: '63s'
        multiplier : int
            Value to multiply the interval by (see output example).
            Example: 1000

        Output
        ------
        (60, pd.DateOffset(seconds=60))
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
        accepted_seconds = int(min(self.kline_intervals, key=lambda x:abs(x - requested_seconds)))
        offset = pd.DateOffset(seconds=accepted_seconds * multiplier)
        return accepted_seconds, offset


    def _get_signature(self, ts: int, method: str, url: str, params: dict = {}) -> str:
        prepared = requests.Request(method, url).prepare()
        phrase = f'{ts}{prepared.method}{prepared.path_url}{urlencode(params)}'
        return hmac.new(
            self.private_key.encode("utf-8"),
            phrase.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()


    def send_request(self, method: str, url: str, headers: dict = {}, params: dict = {}, sign: bool = False):
        '''
        Send a request to the url and handles errors.

        Common FTX API Errors
        -------------------------
        ...

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

        # ## Signature-specific requests
        if sign:
            ts = str(self.get_exchange_timestamp())
            signature = self._get_signature(ts=ts, method=method, url=url, params=params)
            headers.update({
                "FTX-KEY": self.api_key,
                "FTX-SIGN": signature,
                "FTX-TS": ts,
            })
            kwargs.update({'headers': headers})

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
        data = {}
        try:
            data = response.json()
        except ValueError:
            self.logger.exception('API could not be json decoded.')
        if not data.get('success'):
            self.logger.error(f'API returned error: {data["error"]}.')
            result = None
        else:
            result = data['result']
        return result


    def get_exchange_timestamp(self) -> int:
        ''' Returns the current server time in unix time, in milliseconds. '''
        data = self.send_request(
            method='GET', url=f'https://otc.ftx.com/api/time',
            headers={}, params={}
        )
        try:
            dt = self.parse_ftx_datetime_string(data)
            timestamp = int(dt.timestamp() * 1000)
        except Exception:
            self.logger.exception(f'Could not parse server time. Data received from FTX API: {data}.')
            timestamp = None
        if timestamp is None:  # get machine's server time as exchange request failed
            return int(datetime.now(timezone.utc).timestamp()) * 1000
        else:
            return timestamp


    #########################
    #     Market Prices     #
    #########################


    def get_spot_pairs(self):
        '''
        Gets all the spot pairs traded at FTX.

        Endpoint
        --------
        https://docs.ftx.com/#get-markets

        Output
        ------
        pd.DataFrame
            underlying | quote
            -------------------
              'BTC'    |  'USD'
              'ETH'    |  'USD'
        '''
        ## Query Trading Pairs
        url = self.url + '/markets'
        data = self.send_request(method='GET', url=url)

        ## Format Pairs
        pairs = [[d.get('baseCurrency', None), d.get('quoteCurrency', None)] for d in data if d.get('type') == 'spot']
        df = pd.DataFrame(pairs, columns=['underlying', 'quote'])
        df = df.dropna()  # works with d.get('x', None) to remove non-spot instruments
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
        https://docs.ftx.com/#get-historical-prices

        Output
        ------
        pd.DataFrame
            Columns = 'open_time', 'open', 'close', 'high', 'low', 'volume'
        '''
        ## Clean Inputs
        if instrument not in ['spot', 'perp']:
            raise Exception(f'Value for "instrument" parameter should be one of [spot, perp]. Received "{instrument}.')
        if not isinstance(interval, str):
            raise Exception(f'Value for "interval" parameter should be str. Received "{interval} of type {type(interval)}.')

        ## Clean Inputs - Pairs
        if not isinstance(pairs, list):
            pairs = [pairs]
        try:
            pairs = [pair for pair in pairs if len(pair) == 2]
        except Exception:
            raise Exception(f'Pairs argument passed an invalid value. Should contain a list of ("underlying", "quote") tuples. Value passed: {pairs}')

        ## Clean Inputs - FTX only has COIN-USD perp's atm
        if instrument == 'perp':
            cleaned_pairs = [pair for pair in pairs if pair[1].upper() == 'USD']
            # API doesnt allow specification of quote asset for perp data, so we must filter pairs
            if len(cleaned_pairs) != len(pairs):
                pairs = cleaned_pairs
                dirty_pairs = [pair for pair in pairs if pair[1].upper() != 'USD']
                self.logger.warning(
                    f'Request received for perp futures historical price data, \
                    which included some non-USD quote pairs. FTX only has base-USD pairs, \
                    so we are ignoring these pairs. \n \
                    Dirty pairs: {dirty_pairs}.'
                )

        ## Make date iterables to request the whole start-to-end range
        data_per_request = 1500  # max that I see it's giving per request
        interval_in_seconds, freq = self.interval_to_offset(interval=interval, multiplier=data_per_request)
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

            if instrument == 'spot':
                market_name = f'{pair[0]}/{pair[1]}'
            elif instrument == 'perp':
                market_name = f'{pair[0]}-PERP'
            url = self.url + f'/markets/{market_name}/candles'

            for i in range(len(periods))[:-1]:
                payload = {
                    "resolution": interval_in_seconds,
                    "start_time": self.time_to_unix(periods[i]),
                    "end_time": self.time_to_unix(periods[i + 1]),
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
            'startTime': 'open_time',  # string time at start of kline
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'volume': 'volume',
        }
        if columns is None:
            df = pd.DataFrame()
        else:
            ## Make DataFrame
            df = pd.DataFrame(data=prices, columns=columns)
            df = df.rename(columns=columns_rename)
            df = df[columns_rename.values()]  # remove extra data the api may give in the future

            df[['underlying', 'quote']] = pd.DataFrame(data=prices_pair_index, columns=['underlying', 'quote'])
            df['open_time'] = df['open_time'].apply(self.parse_ftx_datetime_string)
            time_mask = (start <= df.open_time) & (df.open_time < end)
            df = df[time_mask]
            df = df.drop_duplicates()

        return df


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
        Gets all the perpetual pairs traded at FTX.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        underlying | quote
        ------------------
          'BTC'    | 'USD'
          'ETH'    | 'USD'
        '''
        perp_pairs = []
        url = f'{self.url}/futures'
        response_ftx= requests.request("GET", url).json()['result']
        for data_dict in response_ftx:
            if data_dict.get('perpetual') == True:
                ## Get Perp Futures Contract data
                perp_pairs.append([data_dict["underlying"].upper(), 'USD'])
        df = pd.DataFrame(columns=['underlying', 'quote'], data=perp_pairs)
        return df


    def get_next_funding_rate(self, underlying_symbols: List[str]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of str
            Format is ['BTC', 'ETH', etc...]

        Endpoint Inputs
        ----------------
        FTX: /futures/{symbol}/stats
            - "symbol": <underlying_asset>-PERP
                - E.g. "BTC-PERP"

        Endpoint Output
        ---------------
        {'success': True, 'result': {'volume': 78639.7612, 'nextFundingRate': -2e-06, 'nextFundingTime': '2021-05-04T20:00:00+00:00', 'openInterest': 30195.1692}}

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.01 |   'BTC'    | 'USD' |  0.001
        2022.01.01 |   'ETH'    | 'USD' |  0.002
        '''
        funding_rates = []
        for underlying_symbol in underlying_symbols:
            url = f"{self.url}/futures/{underlying_symbol}-PERP/stats"
            response = requests.request("GET", url).json()
            try:
                data = response['result']
            except KeyError:
                if response['error'].startswith('No such future'):
                    continue
                else:
                    raise
            ## Parse Funding Rate
            funding_rates.append([
                datetime.strptime(data['nextFundingTime'], "%Y-%m-%dT%H:%M:%S+00:00"),
                underlying_symbol.upper(),
                'USD',
                float(data['nextFundingRate']),
            ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        return df


    def get_historical_funding_rates(self,
        underlying_symbols: List[str], start: datetime, end: datetime
    ) -> pd.DataFrame:
        '''
        Gets historical funding rates for the symbols requested and date rnage requested.
            NOTE: the max FTX will give is the last 500 funding rates.
        Method Inputs
        -------------
        pairs: a list of str
            Format is ['BTC', 'ETH', etc...]
        start : datetime
        end : datetime

        Endpoint Inputs
        ---------------
        FTX: /funding_rates
            - "symbol": <underlying_ticker>-PERP
                - E.g. "BTC-PERP"

        Endpoint Output
        ---------------
        {'success': True, 'result': [
            {'future': 'BTC-PERP', 'rate': -7e-06, 'time': '2021-05-04T19:00:00+00:00'},
            {'future': 'BTC-PERP', 'rate': 2e-06, 'time': '2021-05-04T18:00:00+00:00'}
        ]}

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.02 |   'BTC'    | 'USD' |  0.001
        2022.01.02 |   'ETH'    | 'USD' |  0.002
        2022.01.01 |   'BTC'    | 'USD' |  0.001
        2022.01.01 |   'ETH'    | 'USD' |  0.002
        '''
        funding_rates = []

        ## Get formatted start and end epoch time - this is not needed as 500 (the max) records are returned when no datetimes are given
        # now = datetime.now(timezone.utc)
        # start_time = int((now - timedelta(hours=5000)).timestamp())  # even if this is called exactly on the hour, and two are provided, everything is okay because the latest one is the first item in the list
        # end_time = int(now.timestamp())

        url = f'{self.url}/funding_rates'       # By default, gives 4 hours of historical funding rates (4 rates) per trading pair

        for underlying_symbol in underlying_symbols:
            pair = underlying_symbol + 'USD'
            payload = {
                "future": f"{underlying_symbol}-PERP",
                # "start_time": start_time,
                # "end_time": end_time,
            }
            response = requests.request("GET", url, params=payload).json()
            try:
                data = response['result']
            except KeyError:
                if response['error'].startswith('No such future'):
                    self.logger.debug(f'Bybit does not have funding rates for pair "{pair}".')
                    continue
                else:
                    self.logger.exception('')
                    continue

            ## Parse Funding Rates for each time period
            for time_dict in data:
                funding_rates.append([
                    self.parse_ftx_datetime_string(time_dict['time']),
                    underlying_symbol.upper(),
                    'USD',
                    float(time_dict['rate']),
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
        ''' Returns a list of all assets that can be borrowed from FTX. '''
        url = self.url + '/spot_margin/borrow_rates'
        data = self.send_request(method='GET', url=url, sign=True)
        assets = [d['coin'].upper() for d in data]
        return assets


    def get_current_borrow_rate(self, symbols: Union[List[str], None] = None) -> pd.DataFrame:
        '''
        Returns a DataFrame of spot borrow interest rates for the upcoming hour, per asset.
            NOTE: FTX updates interest rates hourly.

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
        this_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        ## Query interest Rates
        url = self.url + '/spot_margin/borrow_rates'
        data = self.send_request(method='GET', url=url, sign=False)

        ## Parse Interest Rate Data
        interest_rates = [[this_hour, d.get('coin'), d.get('estimate')] for d in data]
        df = pd.DataFrame(columns=['datetime', 'symbol', 'rate'], data=interest_rates)

        ## Clean Rates for Requested Symbols
        if symbols is not None:
            mask = df.symbol.isin(symbols)
            df = df[mask]
        return df


    def get_historical_borrow_rate(self, start: datetime, end: datetime, symbols: Union[List[str], None] = None) -> pd.DataFrame:
        '''
        Returns a DataFrame of spot borrow interest rates, per asset and per day.
            NOTE: FTX sets interest rates hourly.


        Inputs
        ------
        start : datetime
        end : datetime
        symbols : List[str] or NoneType
            A list of symbols to get interest rates for.
            NOTE: all symbols are returned by the endpoint, so this argument will only
                  decrease the amount of data provided.

        Endpoint Notes
        --------------
        Returns rates as far back as their margin spot launch (2020.11.26).
        Maximum 2 months date range

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
        freq = pd.DateOffset(days=2)  # max range is 30 days
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )

        ## Must make this distinction as FTX includes the start and end dates given to its endpoint
        periods = list(periods)
        start_periods = periods[:-1]
        end_periods = periods[1:]
        end_periods = [period - pd.DateOffset(hours=1) for period in end_periods]
        end_periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps

        interest_rates = []
        url = self.url + '/spot_margin/history'

        ## Query per sub-date range
        for start_period, end_period in zip(start_periods, end_periods):

            payload = {
                "start_time": self.time_to_unix(start_period),
                "end_time": self.time_to_unix(end_period)
            }

            data = self.send_request(method='GET', url=url, params=payload, sign=True)

            ## Ignore assets not supported by Binance
            if data is None:
                continue

            ## Parse Interest Rates
            for d in data:
                interest_rates.append([
                    self.parse_ftx_datetime_string(d['time']),
                    d['coin'].upper(),
                    float(d['rate']),
                ])
        df = pd.DataFrame(columns=['datetime', 'symbol', 'rate'], data=interest_rates)
        df = df.drop_duplicates()

        ## Clean Rates for Requested Symbols
        if symbols is not None:
            mask = df.symbol.isin(symbols)
            df = df[mask]
        return df




if __name__ == '__main__':
    pass

    ## EXAMPLE USAGE ##

    # CommsFTX().get_historical_funding_rates(underlying_symbols=['BOOPEDOOP', 'ETH', 'BTC'])

    # CommsFTX().get_historical_spot_price(
    #     pairs=[('BTC','USDT'), ('eth', 'USD'), ('uvdsjka', 'USD')],
    #     start=datetime(2022,4,1,23),
    #     end=datetime(2022,4,12),
    #     interval='7320s'
    # )
