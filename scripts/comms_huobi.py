## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Tuple, Union
import requests
import json
import re
import pandas as pd
from datetime import datetime


class CommsHuobi:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_HUOBI.value)
        self.url = 'https://api.hbdm.com'  # Same base URL for C2C (USDT) and C2F (USD), but different extentions


    @try5times
    def get_perp_pairs(self) -> Dict[str, dict]:
        '''
        Gets all the perpetual pairs traded at Huobi.
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
        for url, is_c2c, quote_asset in [
            (f'{self.url}/linear-swap-api/v1/swap_batch_funding_rate', True, 'USDT'),
            (f'{self.url}/swap-api/v1/swap_batch_funding_rate', False, 'USD')
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('data'):
                data_pair = f'{data_dict["symbol"]}{quote_asset}'
                perp_pairs[data_pair] = {
                    'c2c': is_c2c,
                    'underlying_asset': data_dict['symbol'],
                    'quote_asset': quote_asset,
                    'huobi': True
                }
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pairs: Union[List[str], None] = None) -> Dict[str, float]:
        '''
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
        '''
        funding_rates = {}

        ## Perpetual Funding Rates
        for url in [
            f'{self.url}/linear-swap-api/v1/swap_batch_funding_rate',
            f'{self.url}/swap-api/v1/swap_batch_funding_rate'
        ]:
            for data_dict in requests.request("GET", url).json()['data']:
                pair = data_dict['contract_code'].replace('-', '')  # symbol format from the api: "BTC-USDT"
                if re.match('(.+?)(\d\d\d\d\d\d)', pair):
                    continue  # some expiring futures are also given
                else:
                    try:
                        funding_rates[pair] = float(data_dict['funding_rate'])
                    except TypeError:
                        continue   # another way to find expiring futures, as they don't have funding rates

        ## Get Rates only for requests Pairs
        if pairs is not None:
            funding_rates = {pair: rate for pair, rate in funding_rates.items() if pair in pairs}

        return funding_rates


    @try5times
    def get_historical_funding_rates(self, pair_tuples: List[Tuple[str, str]]) -> pd.Series:
        '''
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

        Fuction Output
        --------------
        funding_rates = {
            BTCUSD: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
            BTCUSDT: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
        }
        '''
        funding_rates = {}
        unparsed_rates = {}
        for (underlying, quote) in pair_tuples:
            pair = underlying + quote
            if quote in ['USDT']:
                url = f'{self.url}/linear-swap-api/v1/swap_historical_funding_rate'
            elif quote in ['USD']:
                url = f'{self.url}/swap-api/v1/swap_historical_funding_rate'
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
                    'page_size': 100  # 100 is max size
                }
                response = requests.request("GET", url, params=payload).json()

                ## Handle Pairs that don't exist
                try:
                    data = response['data']['data']
                except KeyError:
                    if response['err_msg'] == 'The contract doesnt exist.' or response['err_code'] == 1332:
                        break
                    else:
                        raise

                ## Break Querying by pagination if no more results are returned
                if len(data) == 0:
                    break
                ## Format Funding rates into dict
                else:
                    unparsed_rates.update({datetime.fromtimestamp(int(d['funding_time'])/1000): float(d['funding_rate']) for d in data})

            if len(unparsed_rates) != 0:  # Empty rates for contracts that don't exist
                funding_rates[pair] = pd.Series(unparsed_rates, name='huobi')

        return funding_rates



if __name__ == '__main__':
    CommsHuobi().get_historical_funding_rates(pair_tuples=(("Robbysnoby", "USDT"), ("BTC", "USDT"), ("ETH", "USD")))
    pass
