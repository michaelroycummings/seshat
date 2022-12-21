## Internal Modules
from seshat.utils import try5times
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Tuple, Union
import requests
import json
import pandas as pd
import re
from datetime import datetime, timedelta


class CommsOKX:

    def __init__(self):
        ''' OKEx is not OKX '''
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_OKX.value)
        self.url = 'https://www.okex.com'  # Same base URL and endpoint for C2C (USDT) and C2F (USD)

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
            21600: '6Hutc',
            43200: '12Hutc',
            86400: '1Dutc',
            172800: '2Dutc',
            259200: '3Dutc',
            604800: '1Wutc',
            2628000: '1Mutc',
            7884000: '3Mutc',
            15768000: '6Mutc',
            31536000: '1Yutc',
        }


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


    def interval_to_offset(self, interval: str, multiplier: int) -> Tuple[int, pd.DateOffset]:
        '''
        Takes an interval str, and a multiplier, converts the interval into the
        nearest OKX-accepted interval times (in seconds), multiplies this by the multiplier,
        and returns a pd.DateOffset of that value, as well as the OKX-accepted interval in seconds.
            - If the multiplier is the number of records to return per request,
              then the returned Offset value is the period of time for that API request,
              and can be used in a pd.date_range(freq=offset) to get start/end dates to make
              multiple requests over a large start/end date range.

        Inputs
        ------
        interval : str
            Any float + [m,h,d] is allowed. Will convert this to the
            nearest OKX-accepted interval.
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


    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at OKX.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        underlying | quote
        -------------------
          'BTC'    | 'USDT'
          'ETH'    | 'USDT'
        '''
        perp_pairs = []
        url = f'{self.url}/api/v5/public/instruments'
        payload = {'instType': 'SWAP'}
        response = requests.request("GET", url, params=payload).json()
        for data_dict in response['data']:

            ## Linear Contracts
            if data_dict['ctType'] == 'linear':
                underlying_asset = data_dict['ctValCcy']
                quote_asset      = data_dict['settleCcy']

            ## Inverse Contracts
            elif data_dict['ctType'] == 'inverse':
                underlying_asset = data_dict['settleCcy']
                quote_asset      = data_dict['ctValCcy']

            ## Add Pair
            perp_pairs.append([
                underlying_asset.upper(),
                quote_asset.upper(),
            ])
        df = pd.DataFrame(columns=['underlying', 'quote'], data=perp_pairs)
        return df


    def get_next_funding_rate(self, pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')

        Endpoint Inputs
        ----------------
        OKEx: /api/v5/public/funding-rate
            - "instrument_id": <underlying_asset>-<quote_asset>-SWAP
                - E.g. BTC-USDT-SWAP
                - E.g. BTC-USD-SWAP

        Endpoint Output
        ---------------
        {'code': '0', 'data': [{'fundingRate': '0.00001034', 'fundingTime': '1620691200000', 'instId': 'BTC-USD-SWAP', 'instType': 'SWAP', 'nextFundingRate': '0.00081', 'nextFundingTime': ''}], 'msg': ''}

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        funding_rates = []

        for (underlying, quote) in pairs:
            asset_id = f'{underlying}-{quote}-SWAP'
            url = f'{self.url}/api/v5/public/funding-rate'
            payload = {
                "instId": asset_id
            }
            response = requests.request("GET", url, params=payload).json()
            try:
                data = response['data'][0]
            except IndexError:
                if response['msg'].startswith('Instrument ID does not exist.'):
                    pass
                else:
                    self.logger.exception('')
                    continue

            ## Parse Funding Rate
            funding_rates.append([
                self.unix_to_dt(data['fundingTime']),
                underlying.upper(),
                quote.upper(),
                float(data['fundingRate']),
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
        OKEx: /api/v5/public/funding-rate-history
            - "instrument_id": <underlying_asset>-<quote_asset>-SWAP
                - E.g. BTC-USDT-SWAP
                - E.g. BTC-USD-SWAP

        Endpoint Output
        --------------
        {'code': '0', 'data': [{'fundingRate': '0.000994862634', 'fundingTime': '1620662400000', 'instId': 'BTC-USD-SWAP', 'instType': 'SWAP', 'realizedRate': '0.0009946484255244'}], 'msg': ''}

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
        data_per_request = 100  # max is 100
        freq = pd.DateOffset(hours=(8 * data_per_request))  # OKX funding rate occurs every eight hours
        periods = pd.date_range(
            start=start, end=end,
            freq=freq
        )
        periods = list(periods)
        periods.append(end)  # NOTE: appending a datetime to a list of pd.Timestamps
        funding_rates = []

        ## Query per Pair
        for (underlying, quote) in pairs:

            ## Set URL and OKX-specific Symbol
            asset_id = f'{underlying}-{quote}-SWAP'
            url = f'{self.url}/api/v5/public/funding-rate-history'
            payload = {
                "instId": asset_id,
                "limit": data_per_request
            }

            ## Send Query for each sub-date range
            for i in range(len(periods))[:-1]:
                payload = {
                    "instId": asset_id,
                    "before": self.time_to_unix(periods[i]),
                    "after": self.time_to_unix(periods[i + 1]),
                    "limit": data_per_request
                }

                response = requests.request("GET", url, params=payload).json()

                ## Skip Pairs without Futures Contracts
                if int(response['code']) == 51000:
                    continue  # no contract for this pair

                ## Parse Funding Rates for each time period
                data = response['data']
                for time_dict in data:
                    funding_rates.append([
                        self.unix_to_dt(time_dict['fundingTime']),
                        underlying.upper(),
                        quote.upper(),
                        float(time_dict['fundingRate']),
                    ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        df = df.drop_duplicates()
        return df


if __name__ == '__main__':
    pass

    ## EXAMPLE USAGE ##

    # CommsOKX().get_historical_funding_rates(
    #     pairs=(("Robbysnoby", "USDT"), ("BTC", "USDT"), ("ETH", "USD")),
    #     start=datetime(2019,1,1),
    #     end=datetime(2022,5,1))
