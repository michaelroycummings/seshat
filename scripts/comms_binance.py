## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Union
import requests
import json
import re
import pandas as pd
from datetime import datetime


class CommsBinance:

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]

        ## Other Attributes
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_BINANCE.value)
        self.url_c2c = 'https://fapi.binance.com/fapi'  # C2C means coin / coin (USDT) pairs
        self.url_c2f = 'https://dapi.binance.com/dapi'  # C2F means coin / fiat (USD) pairs


    @try5times
    def get_perp_pairs(self) -> Dict[str, dict]:
        '''
        Gets all the perpetual pairs traded at Binance.
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
        for url, is_c2c in [
            (f'{self.url_c2c}/v1/exchangeInfo', True),
            (f'{self.url_c2f}/v1/exchangeInfo', False)
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('symbols'):
                if data_dict.get('contractType') == 'PERPETUAL':  # do not consider expiring futures
                    data_pair = data_dict['pair']
                    perp_pairs[data_pair] = {
                        'c2c': is_c2c,
                        'underlying_asset': data_dict['baseAsset'],
                        'quote_asset': data_dict['quoteAsset']
                    }
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pairs: Union[List[str], None] = None) -> Dict[str, float]:
        '''
        Returns a dict, for all perpetuals listed at Binance, of the upcoming funding rate.
            - NOTE that Binance rates are "Estimated Until Paid", so the final rate is not
              known until the begining of its period, when the rate is charged.

        Endpoint Inputs
        ----------------
        C2C: fapi/v1/premiumIndex format:
            - "symbol": <underlying_asset><quote_asset>
                - E.g. BTCUSDT
                - NOTE: for this endpoint, expiring futures have a symbol format: ETHUSDT_210625
        C2F: dapi/v1/premiumIndex provides data with this:
            - "symbol": <underlying_asset><quote_asset>_PERP
                - E.g. BTCUSD_PERP
            - "pair": <underlying_asset><quote_asset>
                - E.g. BTCUSD

        Example Output
        --------------
        C2C: [
            {'symbol': 'SUSHIUSDT', 'markPrice': '16.84710000', 'indexPrice': '16.82849111', 'estimatedSettlePrice': '16.82813723', 'lastFundingRate': '0.00044160', 'interestRate': '0.00010000', 'nextFundingTime': 1620403200000, 'time': 1620399721000},
            {'symbol': 'CVCUSDT', 'markPrice': '0.63221192', 'indexPrice': '0.63072313', 'estimatedSettlePrice': '0.63270632', 'lastFundingRate': '0.00073579', 'interestRate': '0.00010000', 'nextFundingTime': 1620403200000, 'time': 1620399721000}
        ]
        C2F: [
            {'symbol': 'XRPUSD_210625', 'pair': 'XRPUSD', 'markPrice': '1.67816237', 'indexPrice': '1.62303728', 'estimatedSettlePrice': '1.62475630', 'lastFundingRate': '', 'interestRate': '', 'nextFundingTime': 0, 'time': 1620399663000},
            {'symbol': 'LTCUSD_PERP', 'pair': 'LTCUSD', 'markPrice': '370.12016832', 'indexPrice': '356.87782325', 'estimatedSettlePrice': '356.72048049', 'lastFundingRate': '', 'interestRate': '', 'nextFundingTime': 0, 'time': 1620399663000}
        ]
        '''
        funding_rates = {}

        ## Get C2C (USDT quote) Perpetual Funding Rates
        url = f'{self.url_c2c}/v1/premiumIndex'
        for data_dict in requests.request("GET", url).json():
            pair = data_dict['symbol']
            if re.match('(\w+?)(_)(\d+)', pair):
                continue  # some expiring futures are also given
            else:
                funding_rates[pair] = float(data_dict['lastFundingRate'])

        ## Get C2F (USD quote) Perpetual Funding Rates
        url = f'{self.url_c2f}/v1/premiumIndex'
        for data_dict in requests.request("GET", url).json():  # C2F
            if 'PERP' in data_dict['symbol']:  # do not get normal / expiring futures contracts
                funding_rates[data_dict['pair']] = float(data_dict['lastFundingRate'])  # MUST USE "PAIR" and not "symbol"

        ## Get Rates only for requests Pairs
        if pairs is not None:
            funding_rates = {pair: rate for pair, rate in funding_rates.items() if pair in pairs}

        return funding_rates


    @try5times
    def get_historical_funding_rates(self, pairs: List[str]) -> pd.Series:
        '''
        Function Input
        --------------
        pairs : List[str]
            In the format: ['BTCUSDT', 'BTCUSD', 'ETHUSDT']

        Endpoint Inputs
        ----------------
            C2C: fapi/v1/premiumIndex format:
                - "symbol": "<underlying_asset><quote_asset>
                    - E.g. BTCUSDT
            C2F: dapi/v1/premiumIndex format:
                - "symbol": "<underlying_asset><quote_asset>_PERP
                    - E.g. BTCUSD_PERP

        Endpoint Output
        ---------------
        C2C: [
            {'symbol': 'BTCUSDT', 'fundingTime': 1620345600000, 'fundingRate': '0.00044645'},
            {'symbol': 'BTCUSDT', 'fundingTime': 1620374400000, 'fundingRate': '0.00053313'}
        ]
        C2F: [
            {'symbol': 'BTCUSD_PERP', 'fundingTime': 1620345600004, 'fundingRate': '0.00010000'},
            {'symbol': 'BTCUSD_PERP', 'fundingTime': 1620374400002, 'fundingRate': '0.00010000'}
        ]

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
                url = f'{self.url_c2c}/v1/fundingRate'
                payload = {
                    "symbol": pair,
                    "limit": 1000  # 1000 is the max; there is no pagination
                }
            elif pair.endswith('USD'):
                url = f'{self.url_c2f}/v1/fundingRate'
                payload = {
                    "symbol": f'{pair}_PERP',
                    "limit": 1000  # 1000 is the max; there is no pagination
                }
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Pair: {pair}.')
                continue
            response = requests.request("GET", url, params=payload).json()
            if len(response) == 0:
                continue
            pair_rates = {datetime.fromtimestamp(d['fundingTime']/1000): float(d['fundingRate']) for d in response}
            funding_rates[pair] = pd.Series(pair_rates, name='binance')

        return funding_rates



if __name__ == '__main__':
    CommsBinance().get_historical_funding_rates(pairs=['BOOPEDOOPUSDT', 'BTCUSDT', 'ETHUSD'])
    pass