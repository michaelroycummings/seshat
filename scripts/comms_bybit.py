## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Dict, Union
import requests
import json
import pandas as pd
from datetime import datetime
import re


class CommsBybit:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_BYBIT.value)
        self.url = 'https://api.bybit.com'  # Same base URL for C2C (USDT) and C2F (USD), but different extentions
        self.api_key = 'Gm7FZ0e8xDBlM0cIEV'
        self.private_key = '124JAdK23yKIa4JJ5bHZY17mZW8dZzJmOLTz'


    @try5times
    def get_perp_pairs(self) -> Dict[str, dict]:
        '''
        Gets all the perpetual pairs traded at Bybit.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        perp_pairs = {
            BTCUSDT: {
                'c2c': True,
                'underlying_asset': BTC,
                'quote_asset': USDT
            },
        }
        '''
        perp_pairs = {}
        url = f'{self.url}/v2/public/symbols'
        response = requests.request("GET", url).json()
        for data_dict in response['result']:
            data_pair = data_dict['name']

            ## Skip Futures that are not Perpetual
            if any(char.isdigit() for char in data_pair):
                continue

            ## Get Perp Futures Contract data
            underlying_asset = data_dict['base_currency']
            quote_asset = data_dict['quote_currency']
            c2c = False if quote_asset == 'USD' else True
            perp_pairs[data_pair] = {
                'c2c': c2c,
                'underlying_asset': underlying_asset,
                'quote_asset': quote_asset
            }
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pairs: List[str]) -> Dict[str, float]:
        '''
        Endpoint Inputs
        ----------------
        C2C: /private/linear/funding/predicted-funding
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSDT
        C2F: /v2/private/funding/predicted-funding
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSD

        Endpoint Output
        ---------------
        C2C: {'ret_code': 0, 'ret_msg': 'OK', 'ext_code': '', 'ext_info': '', 'result': {'predicted_funding_rate': 0.000375, 'predicted_funding_fee': 0}, 'time_now': '1620414619.583153', 'rate_limit_status': 119, 'rate_limit_reset_ms': 1620414619578, 'rate_limit': 120}
        C2F: {'ret_code': 0, 'ret_msg': 'OK', 'ext_code': '', 'ext_info': '', 'result': {'predicted_funding_rate': 0.0001, 'predicted_funding_fee': 0}, 'time_now': '1620414684.777262', 'rate_limit_status': 119, 'rate_limit_reset_ms': 1620414684769, 'rate_limit': 120}
        '''
        funding_rates = {}

        for pair in pairs:
            if pair.endswith('USDT'):
                url = f'{self.url}/private/linear/funding/predicted-funding'
            elif pair.endswith('USD'):
                url = f'{self.url}/v2/private/funding/predicted-funding'
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Pair: {pair}.')
                continue
            for _ in [0,1,2,3,4]:
                try:
                    payload = {
                        "api_key": self.bybit_api_key,
                        "symbol": pair,
                        # "timestamp": round(datetime.now(timezone.utc).timestamp()*1000)
                        "timestamp": round(float(requests.request("GET", 'https://api.bybit.com/v2/public/time').json()['time_now'])*1000)
                    }
                    payload.update({"sign": self._get_signature(params=payload, private_key=self.bybit_private_key)})
                    response = requests.request("GET", url, params=payload).json()
                    funding_rates[pair] = float(response['result']['predicted_funding_rate'])
                except Exception:
                    pass  # this endpoint fails often
        return funding_rates


    @staticmethod
    def parse_bybit_datetime_string(unparsed_time: Union[str, int]):
        if isinstance(unparsed_time, str):
            parsed_string = re.match('(.+?)(\.\d\d\dZ)', unparsed_time).group(1)
            return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')
        else:
            return datetime.fromtimestamp(unparsed_time)


    @try5times
    def get_historical_funding_rates(self, pairs: List[str]) -> pd.Series:
        '''
        !!!!!!!! READ ME !!!!!!!!
        Bybit provides only the last funding rate via API.
        For further historical funding rates, consult their csv files... lol.
        https://www.bybit.com/data/basic/inverse/funding-history?symbol=BTCUSD

        Endpoint Inputs
        ----------------
        C2C: /public/linear/funding/prev-funding-rate
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSDT
        C2F: /v2/public/funding/prev-funding-rate
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSD

        Endpoint Output
        --------------
        C2C: {'ret_code': 0, 'ret_msg': 'OK', 'ext_code': '', 'ext_info': '', 'result': {'symbol': 'BTCUSDT', 'funding_rate': 0.0001, 'funding_rate_timestamp': '2021-05-07T16:00:00.000Z'}, 'time_now': '1620415171.002726'}
        C2F: {'ret_code': 0, 'ret_msg': 'OK', 'ext_code': '', 'ext_info': '', 'result': {'symbol': 'BTCUSD', 'funding_rate': '0.0001', 'funding_rate_timestamp': 1620403200}, 'time_now': '1620415187.301584'}

        Fuction Output
        --------------
        funding_rates = {
            BTCUSD: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
            BTCUSDT: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
        }
        '''
        funding_rates = {}
        for pair in pairs:
            if pair.endswith('USDT'):
                url = f'{self.url}/public/linear/funding/prev-funding-rate'
            elif pair.endswith('USD'):
                url = f'{self.url}/v2/public/funding/prev-funding-rate'
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Pair: {pair}.')
                continue
            payload = {
                "symbol": pair,
            }
            response = requests.request("GET", url, params=payload).json()
            if response['ret_code'] == 10001:
                continue  # no contract for this pair
            else:
                data = response['result']
            pair_rates = {self.parse_bybit_datetime_string(data['funding_rate_timestamp']): float(data['funding_rate'])}
            funding_rates[pair] = pd.Series(pair_rates, name='bybit')

        return funding_rates


if __name__ == '__main__':
    CommsBybit().get_historical_funding_rates(pairs=['BOOPEDOOPUSDT', 'BTCUSDT', 'ETHUSD'])
