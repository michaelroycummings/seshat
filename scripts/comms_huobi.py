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
    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Huobi.
        Can be plugged directly into CommsFundingRates.
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        for url, quote_asset in [
            (f'{self.url}/linear-swap-api/v1/swap_batch_funding_rate', 'USDT'),
            (f'{self.url}/swap-api/v1/swap_batch_funding_rate', 'USD')
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('data'):
                perp_pairs = perp_pairs.append({
                    'underlying': data_dict['symbol'],
                    'quote': quote_asset,
                }, ignore_index=True)
        return perp_pairs


    @try5times
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
        '''
        pairs_lookup = {f'{underlying}{quote}': (underlying, quote) for (underlying, quote) in pairs}
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])


        ## Perpetual Funding Rates
        for url in [
            f'{self.url}/linear-swap-api/v1/swap_batch_funding_rate',
            f'{self.url}/swap-api/v1/swap_batch_funding_rate'
        ]:
            for data_dict in requests.request("GET", url).json()['data']:
                pair = data_dict['contract_code'].replace('-', '')  # symbol format from the api: "BTC-USDT"
                ## Check for non-perpetual futures contract
                if re.match('(.+?)(\d\d\d\d\d\d)', pair):
                    continue  # some expiring futures are also given
                try:
                    rate = float(data_dict['funding_rate'])
                except TypeError:
                    continue   # another way to find expiring futures, as they don't have funding rates
                ## Ignore Pairs not requested
                try:
                    underlying_quote = pairs_lookup[pair]
                except KeyError:
                    continue
                ## Parse Funding Rate
                parsed_dict = {
                        'datetime': datetime.fromtimestamp(int(data_dict['next_funding_time'])/1000),
                        'underlying': underlying_quote[0],
                        'quote': underlying_quote[1],
                        'rate': rate,
                    }
                funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates


    @try5times
    def get_historical_funding_rates(self, pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        '''
        Method Inputs
        -------------
        pairs: a list of tuples (str, str)
            Format is ('underlying_asset', 'quote_asset')

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
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        for (underlying, quote) in pairs:
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
                        self.logger.exception('')
                        continue

                ## Break Querying by pagination if no more results are returned
                if len(data) == 0:
                    break

                ## Parse Funding Rates for each time period
                for time_dict in data:
                    parsed_dict = {
                            'datetime': datetime.fromtimestamp(int(time_dict['funding_time'])/1000),
                            'underlying': underlying,
                            'quote': quote,
                            'rate': float(time_dict['funding_rate']),
                        }
                    funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates



if __name__ == '__main__':
    CommsHuobi().get_next_funding_rate(pairs=(("Robbysnoby", "USDT"), ("BTC", "USDT"), ("ETH", "USD")))
    pass

'1648771200000'