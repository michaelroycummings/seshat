## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc

## External Libraries
from typing import Dict, List, Union, Tuple
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
    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Gets all the perpetual pairs traded at Binance.
        Can be plugged directly into CommsFundingRates.
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        for url in [
            f'{self.url_c2c}/v1/exchangeInfo',
            f'{self.url_c2f}/v1/exchangeInfo',
        ]:
            response = requests.request("GET", url).json()
            for data_dict in response.get('symbols'):
                if data_dict.get('contractType') == 'PERPETUAL':  # do not consider expiring futures
                    perp_pairs = perp_pairs.append({
                        'underlying': data_dict['baseAsset'],
                        'quote': data_dict['quoteAsset'],
                    }, ignore_index=True)
        return perp_pairs


    @try5times
    def get_next_funding_rate(self, pairs: Union[List[Tuple[str, str]], None]) -> pd.DataFrame:
        '''
        Returns a dict, for requested pairs (or all pairs if NoneType given) listed at Binance,
        of the upcoming funding rates.
            - NOTE that Binance rates are "Estimated Until Paid", so the final rate is not
              known until the begining of its period, when the rate is charged.

        Method Inputs
        -------------
        pairs: a list of tuples (str, str) or NoneType
            Format is ('underlying_asset', 'quote_asset')
            If NoneType, returns all funding rates that Binance has.

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

        Endpoint Output
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
        pairs_lookup = {f'{underlying}{quote}': (underlying, quote) for (underlying, quote) in pairs}
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        ## Get C2C (USDT quote) Perpetual Funding Rates
        url = f'{self.url_c2c}/v1/premiumIndex'
        response = requests.request("GET", url).json()
        for data_dict in response:
            pair = data_dict['symbol']
            ## Ignore non-perpetual futures
            if re.match('(\w+?)(_)(\d+)', pair):
                continue  # some expiring futures are also given
            ## Ignore Pairs not requested
            try:
                underlying_quote = pairs_lookup[pair]
            except KeyError:
                continue
            ## Parse Funding Rate
            parsed_dict = {
                    'datetime': datetime.fromtimestamp(data_dict['nextFundingTime']/1000),
                    'underlying': underlying_quote[0],
                    'quote': underlying_quote[1],
                    'rate': float(data_dict['lastFundingRate']),
                }
            funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        ## Get C2F (USD quote) Perpetual Funding Rates
        url = f'{self.url_c2f}/v1/premiumIndex'
        response = requests.request("GET", url).json()
        for data_dict in response:  # C2F
            contract_id = data_dict['symbol']  # format BTCUSD_PERP
            pair = data_dict['pair']
            ## Ignore non-perpetual futures
            if contract_id.endswith('_PERP') is False:
                continue
            ## Ignore Pairs not requested
            try:
                underlying_quote = pairs_lookup[pair]
            except KeyError:
                continue
            ## Parse Funding Rate
            parsed_dict = {
                    'datetime': datetime.fromtimestamp(data_dict['nextFundingTime']/1000),
                    'underlying': underlying_quote[0],
                    'quote': underlying_quote[1],
                    'rate': float(data_dict['lastFundingRate']),
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
        funding_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote', 'rate'])

        for (underlying, quote) in pairs:
            if quote == 'USDT':
                url = f'{self.url_c2c}/v1/fundingRate'
                payload = {
                    "symbol": f'{underlying}{quote}',
                    "limit": 1000  # 1000 is the max; there is no pagination
                }
            elif quote == 'USD':
                url = f'{self.url_c2f}/v1/fundingRate'
                payload = {
                    "symbol": f'{underlying}{quote}_PERP',
                    "limit": 1000  # 1000 is the max; there is no pagination
                }
            else:
                self.logger.critical(f'Pair requested with unrecognised quote currency. Underlying: {underlying}. Quote: {quote}.')
                continue
            response = requests.request("GET", url, params=payload).json()
            if len(response) == 0:
                continue
            ## Parse Funding Rates for each time period
            for time_dict in response:
                parsed_dict = {
                        'datetime': datetime.fromtimestamp(time_dict['fundingTime']/1000),
                        'underlying': underlying,
                        'quote': quote,
                        'rate': float(time_dict['fundingRate']),
                    }
                funding_rates = funding_rates.append(parsed_dict, ignore_index=True)

        return funding_rates



if __name__ == '__main__':
    CommsBinance().get_next_funding_rate(pairs=[('BOOPEDOOP', 'USDT'), ('BTC', 'USDT'), ('ETH', 'USD')])
    pass