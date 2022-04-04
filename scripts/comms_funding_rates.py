## Internal Modules
from scripts.utils import try5times
from scripts.utils_logging import MyLogger
from scripts.utils_data_locations import DataLoc
from scripts.comms_binance import CommsBinance
from scripts.comms_ftx import CommsFTX
from scripts.comms_huobi import CommsHuobi
from scripts.comms_bybit import CommsBybit
from scripts.comms_okx import CommsOKX

## External Libraries
import requests
import json
import numpy as np
import pandas as pd
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import List
import argparse
import sys
import pickle as pkl
from functools import wraps
import hashlib  # for signing
import hmac     # for signing

import threading
import queue
import os
import signal


class CommsFundingRates:
    '''
    Example Use
    -----------
    ...

    Funding Fee Payment Schedule
    ----------------------------
        - Binance : 00:00, 08:00, 16:00 UTC
        - FTX     : 24x per day, on the hour
        - Huobi   : 00:00, 08:00, 16:00 SST (UTC+8) ~ UTC
        - Bybit   : 00:00, 08:00, 16:00 UTC
        - OKX     : 02:00, 10:00, 18:00 CEST (UTC+2) ~ UTC

     Funding Schedule Resources
     --------------------------
        - Binance : https://www.binance.com/en/support/faq/360033525031
        - FTX     : https://help.ftx.com/hc/en-us/articles/360027946571-Funding
        - Huobi   : https://huobiapi.github.io/docs/usdt_swap/v1/en/#order-and-trade
        - Bybit   : https://help.bybit.com/hc/en-us/articles/360039261114-What-is-funding-rate-and-predicted-rate-
        - OKX    : https://www.okx.com/support/hc/en-us/articles/360020412631-XI-Funding-Rate-Calculations

    Funding Schedule Procedure
    --------------------------

        Estimated Until Paid
        --------------------
            - Description:
                If a period is 08:00-16:00, then a funding fee will be applied at to all contract owners at 16:00.
                This fee fluctuates throughout the period, until 16:00.
            - Used by:
                - Binance
                - FTX
                - Bybit

        Fixed Before Paid
        -----------------
            - Description:
                If a period is 08:00-16:00, then a funding fee will be applied at to all contract owners at 16:00.
                This fee fluctuates throughout the previous period (00:00-08:00) and is set at the beginning of this period (08:00).
            - Used by:
                - Huobi
                - OKX
    '''

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]
        ## Logger
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.COMMS_FUNDING_RATES.value)
        ## Comms
        self.CommsBinance = CommsBinance()  # has C2C (coin to coin; aka USDT) pairs and C2F (coin to fiat; aka USD) pairs
        self.CommsFTX = CommsFTX()          # has only C2F pairs
        self.CommsHuobi = CommsHuobi()      # has C2C and C2F pairs
        self.CommsBybit = CommsBybit()      # has C2C and C2F pairs
        self.CommsOKX = CommsOKX()          # has C2C and C2F pairs

        ## Variables
        self.available_perps = None  # Placeholder
        self.predicted_rates = None  # Placeholder
        self.historical_rates = None  # Placeholder
        self.show = None  # Placeholder
        self.interval = None  # Placeholder
        self.q = queue.Queue()  # create a queue.Queue object to communicate between the Main Thread (for user input) and the Side Thread (for the entire rest of the program)


    @staticmethod
    def _round_minutes(dt, resolution):
        new_hour = (dt.hour // resolution + 1) * resolution
        return (dt.replace(hour=0, minute=0, second=0) + timedelta(hours=new_hour))


    @staticmethod
    def _get_signature(params: dict, private_key: str) -> str:
        sign = ''
        for key in sorted(params.keys()):
            value = params[key]
            sign = f'{sign}{key}={value}&'
        sign = sign[:-1]
        hash = hmac.new(f'{private_key}'.encode('utf-8'), sign.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()  # the signature


    def get_available_perps(self):
        '''
        Populate self.available_perps, for each pair traded at the exchanges, with a dict containing info on that pair.

        Output
        ------

        underlying | quote | binance | ftx  | huobi | bybit | okx
        ---------------------------------------------------------
           'BTC'   | 'USD' |  True   | True |  True | True  | True
           'BTC'   | 'USDT'|  True   | False|  True | True  | True
           'ETH'   | 'USDT'|  True   | True |  True | True  | True
        '''
        self.available_perps = pd.DataFrame(columns=['underlying', 'quote'])
        ## Get Exchange Pairs
        unparsed_pairs = {
            'binance': self.CommsBinance.get_perp_pairs,
            'ftx': self.CommsFTX.get_perp_pairs,
            'huobi': self.CommsHuobi.get_perp_pairs,
            'bybit': self.CommsBybit.get_perp_pairs,
            'okx': self.CommsOKX.get_perp_pairs
        }

        for exchange, get_pairs_func in unparsed_pairs.items():
            pairs_df = get_pairs_func()
            pairs_df[exchange] = True
            self.available_perps = self.available_perps.merge(pairs_df, how='outer', on=['underlying', 'quote'])

        self.available_perps.fillna(False, inplace=True)
        return self.available_perps


    def get_predicted_rates(self):

        ## Get perp futures pairs available at all exchanges
        if self.available_perps is None:
            self.get_available_perps()

        pairs = list(self.available_perps.loc[:, ['underlying', 'quote']].to_records(index=False))
        underlying_symbols = list(self.available_perps['underlying'])
        self.predicted_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote'])

        ## Set Up Functions to Execute
        unparsed_rates = {
            'binance' : (self.CommsBinance.get_next_funding_rate, {'pairs': pairs}),
            'ftx'     : (self.CommsFTX.get_next_funding_rate, {'underlying_symbols': underlying_symbols}),
            'huobi'   : (self.CommsHuobi.get_next_funding_rate, {'pairs': pairs}),
            'bybit'   : (self.CommsBybit.get_next_funding_rate, {'pairs': pairs}),
            'okx'     : (self.CommsOKX.get_next_funding_rate, {'pairs': pairs}),
        }

        ## Format Predicted Rates
        for exchange, (exchange_function, kwargs) in unparsed_rates.items():
            exchange_df = exchange_function(**kwargs)
            exchange_df = exchange_df.rename(columns={'rate': exchange})
            self.predicted_rates = self.predicted_rates.merge(exchange_df, how='outer', on=['datetime', 'underlying', 'quote'])

        self.predicted_rates.fillna(np.NaN, inplace=True)
        return self.predicted_rates


    def get_historical_rates(self):

        ## Get perp futures pairs available at all exchanges
        if self.available_perps is None:
            self.get_available_perps()

        pairs = list(self.available_perps.loc[:, ['underlying', 'quote']].to_records(index=False))
        underlying_symbols = list(self.available_perps['underlying'])
        self.historical_rates = pd.DataFrame(columns=['datetime', 'underlying', 'quote'])

        ## Set Up Functions to Execute
        unparsed_rates = {
            'binance' : (self.CommsBinance.get_historical_funding_rates, {'pairs': pairs}),
            'ftx'     : (self.CommsFTX.get_historical_funding_rates, {'underlying_symbols': underlying_symbols}),
            'huobi'   : (self.CommsHuobi.get_historical_funding_rates, {'pairs': pairs}),
            'bybit'   : (self.CommsBybit.get_historical_funding_rates, {'pairs': pairs}),
            'okx'     : (self.CommsOKX.get_historical_funding_rates, {'pairs': pairs}),
        }

        ## Format Historical Rates
        for exchange, (exchange_function, kwargs) in unparsed_rates.items():
            exchange_df = exchange_function(**kwargs)
            exchange_df = exchange_df.rename(columns={'rate': exchange})
            self.historical_rates = self.historical_rates.merge(exchange_df, how='outer', on=['datetime', 'underlying', 'quote'])

        self.historical_rates.fillna(np.NaN, inplace=True)
        return self.historical_rates


if __name__ == '__main__':

    CommsFundingRates().get_predicted_rates()
    CommsFundingRates().get_historical_rates()
    pass
