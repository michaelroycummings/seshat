## Internal Modules
from seshat.utils import try5times
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc

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
        with open(self.DataLoc.File.API_CONFIG.value) as json_file:
            keys = json.load(json_file)['bybit']
        self.api_key = keys['api_key']
        self.private_key = keys['private_key']


    @staticmethod
    def _get_signature(params: dict, private_key: str) -> str:
        '''
        Hashes the query string with SHA256 and return the hexstring.
        The output should be given to a "sign" param for Bybit API requests.
        '''
        for key in sorted(params.keys()):
            value = params[key]
            sign = f'{key}={value}&'
        sign = sign[:-1]
        hash = hmac.new(f'{private_key}'.encode('utf-8'), sign.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()  # the signature


    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Bybit.
        Can be plugged directly into CommsFundingRates.

        Output
        ------
        underlying | quote
        -------------------
          'BTC'    | 'USDT'
          'ETH'    | 'USDT'
        '''
        perp_pairs = []
        url = f'{self.url}/v2/public/symbols'
        response = requests.request("GET", url).json()
        for data_dict in response['result']:
            data_pair = data_dict['name']

            ## Skip Futures that are not Perpetual
            if any(char.isdigit() for char in data_pair):
                continue

            ## Get Perp Futures Contract data
            perp_pairs.append([
                data_dict['base_currency'].upper(),
                data_dict['quote_currency'].upper(),
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

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        funding_rates = []

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
                    funding_rates.append([
                        self.parse_bybit_datetime_string(response['time_now']),
                        underlying.upper(),
                        quote.upper(),
                        float(response['result']['predicted_funding_rate']),
                    ])
                    break
                except Exception:
                    if count >= 5:
                        break
                    elif response['ret_code'] == 10004:
                        message = 'Signing error while using Bybit API'
                        self.logger.critical(message)
                        raise Exception(message)
                    elif response['ret_msg'] == 'api_key expire' or response['ret_code'] == 33004:
                        message = 'BYBIT EXCHANGE API KEY IS EXPIRED.'
                        self.logger.critical(message)
                        raise Exception(message)
                    elif response['ret_msg'] == "Param validation for 'symbol' failed on the 'linear_symbol' tag" or response['ret_code'] == 10001:
                        self.logger.debug(f'Bybit does not have funding rates for pair "{pair}".')
                        break
                    else:
                        pass  # this endpoint fails often
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        return df


    @staticmethod
    def parse_bybit_datetime_string(unparsed_time: Union[str, int]):
        if isinstance(unparsed_time, str):
            if re.match('\d+?\.\d+?', unparsed_time):
                return datetime.utcfromtimestamp(float(unparsed_time))
            else:
                parsed_string = re.match('(.+?)(\.\d\d\dZ)', unparsed_time).group(1)
                return datetime.strptime(parsed_string, '%Y-%m-%dT%H:%M:%S')
        else:
            return datetime.utcfromtimestamp(unparsed_time)


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

        Method Output
        --------------
         datetime  | underlying | quote | rate
        ----------------------------------------
        2022.01.02 |   'BTC'    |'USDT' |  0.001
        2022.01.02 |   'ETH'    |'USDT' |  0.002
        2022.01.01 |   'BTC'    |'USDT' |  0.001
        2022.01.01 |   'ETH'    |'USDT' |  0.002
        '''
        funding_rates = []

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
            funding_rates.append([
                self.parse_bybit_datetime_string(data['funding_rate_timestamp']),
                underlying.upper(),
                quote.upper(),
                float(data['funding_rate']),
            ])
        df = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'], data=funding_rates)
        df = df.drop_duplicates()
        return df


if __name__ == '__main__':
    pass

    ## EXAMPLE USAGE ##

    # CommsBybit().get_historical_funding_rates(pairs=[('BOOPEDOOP', 'USDT'), ('BTC', 'USDT'), ('ETH', 'USD')])
