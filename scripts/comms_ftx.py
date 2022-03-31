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
    def get_perp_pairs(self) -> Dict[str, dict]:
        '''
        Gets all the perpetual pairs traded at FTX.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        perp_pairs = {
            BTCUSD: {
                'c2c': False,
                'underlying_asset': BTC,
                'quote_asset': USD
            },
        }
        '''
        perp_pairs = {}
        url = f'{self.url}/futures'
        response_ftx= requests.request("GET", url).json()['result']
        for data_dict in response_ftx:
            if data_dict.get('perpetual') == True:
                data_pair = f'{data_dict["underlying"]}USD'
                perp_pairs[data_pair] = {
                    'c2c': False,
                    'underlying_asset': data_dict['underlying'],
                    'quote_asset': 'USD'  # they don't state a quote asset because all are in USD
                }
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, underlying_symbols: List[str]) -> Dict[str, float]:
        '''
        Endpoint Inputs
        ----------------
        FTX: /futures/{symbol}/stats
            - "symbol": <underlying_asset>-PERP
                - E.g. "BTC-PERP"

        Endpoint Output
        ---------------
        {'success': True, 'result': {'volume': 78639.7612, 'nextFundingRate': -2e-06, 'nextFundingTime': '2021-05-04T20:00:00+00:00', 'openInterest': 30195.1692}}
        '''
        funding_rates = {}
        for underlying in underlying_symbols:
            pair = underlying + 'USD'
            url = f"{self.url}/futures/{underlying}-PERP/stats"
            response = requests.request("GET", url).json()
            try:
                funding_rates[pair] = float(response['result']['nextFundingRate'])
            except KeyError:
                if response['error'].startswith('No such future'):
                    continue
                else:
                    raise
        return funding_rates


    @staticmethod
    def parse_ftx_datetime_string(string: str):
        parsed_string = re.match('(.+?)(\+\d\d:\d\d)', string).group(1)
        return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')


    @try5times
    def get_historical_funding_rates(self, underlying_symbols: List[str]) -> pd.Series:
        '''
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
        funding_rates = {}

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
                    continue
                else:
                    raise
            pair_rates = {self.parse_ftx_datetime_string(d['time']): float(d['rate']) for d in data}
            funding_rates[pair] = pd.Series(pair_rates, name='ftx')

        return funding_rates


if __name__ == '__main__':
    CommsFTX().get_historical_funding_rates(underlying_symbols=['BOOPEDOOP', 'ETH', 'BTC'])