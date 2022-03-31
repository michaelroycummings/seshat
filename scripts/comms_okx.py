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
    def get_perp_pairs(self) -> Dict[str, dict]:
        '''
        Gets all the perpetual pairs traded at OKX.
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
            data_pair = f'{underlying_asset}{quote_asset}'
            c2c = False if quote_asset == 'USD' else True
            perp_pairs[data_pair] = {
                'c2c': c2c,
                'underlying_asset': underlying_asset,
                'quote_asset': quote_asset
            }
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pair_tuples: List[Tuple[str, str]]) -> Dict[str, float]:
        '''
        Function Input
        --------------
        pair_tuples : list
            E.g. [('BTC', 'USDT'), ('BTC', 'USD'), ('ETH', 'USDT'), ('ETH', 'USD')]

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
        funding_rates = {}

        for (underlying, quote) in pair_tuples:
            pair = underlying + quote
            asset_id = f'{underlying}-{quote}-SWAP'
            url = f'{self.url}/api/v5/public/funding-rate'
            payload = {
                "instId": asset_id
            }
            response = requests.request("GET", url, params=payload).json()
            try:
                funding_rates[pair] = float(response['data'][0]['fundingRate'])
            except IndexError:
                if response['msg'].startswith('Instrument ID does not exist.'):
                    pass
                else:
                    raise
        return funding_rates


    @try5times
    def get_historical_funding_rates(self, pair_tuples: List[Tuple[str, str]]) -> pd.Series:
        '''
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
        funding_rates = {}
        for (underlying, quote) in pair_tuples:
            pair = underlying + quote
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

            ## Parse Funding Rates
            data = response['data']
            pair_rates = {datetime.fromtimestamp(int(d['fundingTime'])/1000): float(d['fundingRate']) for d in data}
            funding_rates[pair] = pd.Series(pair_rates, name='okx')

        return funding_rates


if __name__ == '__main__':
    CommsOKX().get_historical_funding_rates(pair_tuples=(("Robbysnoby", "USDT"), ("BTC", "USDT"), ("ETH", "USD")))
    pass
