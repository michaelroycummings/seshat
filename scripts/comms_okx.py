## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Tuple, Union
import requests
import json
import pandas as pd
from datetime import datetime


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


    @try5times
    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at OKX.
        Can be plugged directly into CommsFundingRates.
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
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
            perp_pairs = perp_pairs.append({
                'underlying': underlying_asset,
                'quote': quote_asset,
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
        OKEx: /api/v5/public/funding-rate
            - "instrument_id": <underlying_asset>-<quote_asset>-SWAP
                - E.g. BTC-USDT-SWAP
                - E.g. BTC-USD-SWAP

        Endpoint Output
        ---------------
        {'code': '0', 'data': [{'fundingRate': '0.00001034', 'fundingTime': '1620691200000', 'instId': 'BTC-USD-SWAP', 'instType': 'SWAP', 'nextFundingRate': '0.00081', 'nextFundingTime': ''}], 'msg': ''}
        '''
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

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
            parsed_dict = {
                    'datetime': datetime.fromtimestamp(int(data['fundingTime'])/1000),
                    'underlying': underlying,
                    'quote': quote,
                    'rate': float(data['fundingRate']),
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
        ----------------
        OKEx: /api/v5/public/funding-rate-history
            - "instrument_id": <underlying_asset>-<quote_asset>-SWAP
                - E.g. BTC-USDT-SWAP
                - E.g. BTC-USD-SWAP

        Endpoint Output
        --------------
        {'code': '0', 'data': [{'fundingRate': '0.000994862634', 'fundingTime': '1620662400000', 'instId': 'BTC-USD-SWAP', 'instType': 'SWAP', 'realizedRate': '0.0009946484255244'}], 'msg': ''}

        Fuction Output
        --------------
        funding_rates = {
            BTCUSD: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
            BTCUSDT: pd.Series({datetime_1: rate_1, datetime_2: rate_2}),
        }
        '''
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        for (underlying, quote) in pairs:
            asset_id = f'{underlying}-{quote}-SWAP'
            url = f'{self.url}/api/v5/public/funding-rate-history'
            payload = {
                "instId": asset_id,
                "limit": 10000000
            }
            response = requests.request("GET", url, params=payload).json()

            ## Skip Pairs without Futures Contracts
            if int(response['code']) == 51000:
                continue  # no contract for this pair

            ## Parse Funding Rates for each time period
            data = response['data']
            for time_dict in data:
                parsed_dict = {
                        'datetime': datetime.fromtimestamp(int(time_dict['fundingTime'])/1000),
                        'underlying': underlying,
                        'quote': quote,
                        'rate': float(time_dict['fundingRate']),
                    }
                funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates


if __name__ == '__main__':
    CommsOKX().get_historical_funding_rates(pairs=(("Robbysnoby", "USDT"), ("BTC", "USDT"), ("ETH", "USD")))
    pass
