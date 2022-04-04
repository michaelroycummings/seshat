## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Union
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import re


class CommsFTX:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_FTX.value)
        self.url = 'https://ftx.com/api'  # Only has C2F (quoted against USD) at the moment


    @try5times
    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at FTX.
        Can be plugged directly into CommsFundingRates.
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        url = f'{self.url}/futures'
        response_ftx= requests.request("GET", url).json()['result']
        for data_dict in response_ftx:
            if data_dict.get('perpetual') == True:
                ## Get Perp Futures Contract data
                perp_pairs = perp_pairs.append({
                    'underlying': data_dict["underlying"],
                    'quote': 'USD',
                }, ignore_index=True)
        return perp_pairs


    @try5times
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
        '''
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])
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
            parsed_dict = {
                    'datetime': datetime.strptime(data['nextFundingTime'], "%Y-%m-%dT%H:%M:%S+00:00"),
                    'underlying': underlying_symbol,
                    'quote': 'USD',
                    'rate': float(data['nextFundingRate']),
                }
            funding_rates = funding_rates.append(parsed_dict, ignore_index=True)
        return funding_rates


    @staticmethod
    def parse_ftx_datetime_string(string: str):
        parsed_string = re.match('(.+?)(\+\d\d:\d\d)', string).group(1)
        return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')


    @try5times
    def get_historical_funding_rates(self, underlying_symbols: List[str]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of str
            Format is ['BTC', 'ETH', etc...]

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

        Fuction Output
        --------------
        funding_rates = {
            BTCUSD: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
            ETHUSD: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
        }
        '''
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

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
                parsed_dict = {
                        'datetime': self.parse_ftx_datetime_string(time_dict['time']),
                        'underlying': underlying_symbol,
                        'quote': 'USD',
                        'rate': float(time_dict['rate']),
                    }
                funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates


if __name__ == '__main__':
    CommsFTX().get_historical_funding_rates(underlying_symbols=['BOOPEDOOP', 'ETH', 'BTC'])