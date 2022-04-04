## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Dict, Union, Tuple
import requests
import json
import pandas as pd
from datetime import datetime
import re
import hashlib  # for signing
import hmac     # for signing



class CommsBybit:

    def __init__(self):
        '''
        WARNING
        -------
        Bybit API Key only lasts 3 months because it is not linked to a specific IP address.
        '''
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_BYBIT.value)
        self.url = 'https://api.bybit.com'  # Same base URL for C2C (USDT) and C2F (USD), but different extentions
        self.api_key = '1zNYm9RlLQZqUPpu0t'
        self.private_key = 'AUPmsdSu0vl8ZxuKOPpsuKqxVh8KXnRmWnZV'


    @staticmethod
    def _get_signature(params: dict, private_key: str) -> str:
        sign = ''
        for key in sorted(params.keys()):
            value = params[key]
            sign = f'{sign}{key}={value}&'
        sign = sign[:-1]
        hash = hmac.new(f'{private_key}'.encode('utf-8'), sign.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()  # the signature


    @try5times
    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Bybit.
        Can be plugged directly into CommsFundingRates.
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        url = f'{self.url}/v2/public/symbols'
        response = requests.request("GET", url).json()
        for data_dict in response['result']:
            data_pair = data_dict['name']

            ## Skip Futures that are not Perpetual
            if any(char.isdigit() for char in data_pair):
                continue

            ## Get Perp Futures Contract data
            perp_pairs = perp_pairs.append({
                'underlying': data_dict['base_currency'],
                'quote': data_dict['quote_currency'],
            }, ignore_index=True)
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')

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
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        for (underlying, quote) in pairs:
            ## Determine endpoint to Use
            if quote == 'USDT':
                url = f'{self.url}/private/linear/funding/predicted-funding'
            elif quote == 'USD':
                url = f'{self.url}/v2/private/funding/predicted-funding'
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Underlying: {underlying}. Quote: {quote}.')
                continue
            pair = f'{underlying}{quote}'
            count = 0
            while True:
                count += 1
                try:
                    payload = {
                        "api_key": self.api_key,
                        "symbol": pair,
                        # "timestamp": round(datetime.now(timezone.utc).timestamp()*1000)
                        "timestamp": round(float(requests.request("GET", 'https://api.bybit.com/v2/public/time').json()['time_now'])*1000)
                    }
                    payload.update({"sign": self._get_signature(params=payload, private_key=self.private_key)})
                    response = requests.request("GET", url, params=payload).json()
                    ## Parse Funding Rate
                    parsed_dict = {
                            'datetime': self.parse_bybit_datetime_string(response['time_now']),
                            'underlying': underlying,
                            'quote': quote,
                            'rate': float(response['result']['predicted_funding_rate']),
                        }
                    funding_rates = funding_rates.append(parsed_dict, ignore_index=True)
                    break
                except Exception:
                    if count >= 5:
                        break
                    elif response['ret_msg'] == 'api_key expire' or response['ret_code'] == 33004:
                        self.logger.critical('BYBIT EXCHANGE API KEY IS EXPIRED.')
                        raise Exception('BYBIT EXCHANGE API KEY IS EXPIRED.')
                    elif response['ret_msg'] == "Param validation for 'symbol' failed on the 'linear_symbol' tag" or response['ret_code'] == 10001:
                        self.logger.debug(f'Bybit does not have funding rates for pair "{pair}".')
                        break
                    else:
                        pass  # this endpoint fails often
        return funding_rates


    @staticmethod
    def parse_bybit_datetime_string(unparsed_time: Union[str, int]):
        if isinstance(unparsed_time, str):
            if re.match('\d+?\.\d+?', unparsed_time):
                return datetime.fromtimestamp(float(unparsed_time))
            else:
                parsed_string = re.match('(.+?)(\.\d\d\dZ)', unparsed_time).group(1)
                return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')
        else:
            return datetime.fromtimestamp(unparsed_time)


    @try5times
    def get_historical_funding_rates(self, pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        '''
        !!!!!!!! READ ME !!!!!!!!
        Bybit provides only the last funding rate via API.
        For further historical funding rates, consult their csv files... lol.
        https://www.bybit.com/data/basic/inverse/funding-history?symbol=BTCUSD

        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')

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
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        for (underlying, quote) in pairs:
            ## Determine endpoint to Use
            if quote == 'USDT':
                url = f'{self.url}/public/linear/funding/prev-funding-rate'
            elif quote == 'USD':
                url = f'{self.url}/v2/public/funding/prev-funding-rate'
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Underlying: {underlying}. Quote: {quote}.')
                continue
            payload = {
                "symbol": f'{underlying}{quote}',
            }
            response = requests.request("GET", url, params=payload).json()
            ## Ignore empty responses
            if response['ret_code'] == 10001:
                continue  # no contract for this pair
            else:
                data = response['result']
            ## Parse Funding Rate
            parsed_dict = {
                    'datetime': self.parse_bybit_datetime_string(data['funding_rate_timestamp']),
                    'underlying': underlying,
                    'quote': quote,
                    'rate': float(data['funding_rate']),
                }
            funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates


if __name__ == '__main__':
    CommsBybit().get_historical_funding_rates(pairs=[('BOOPEDOOP', 'USDT'), ('BTC', 'USDT'), ('ETH', 'USD')])
