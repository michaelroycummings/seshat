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

    WARNING
    -------
    Bybit API Key only lasts 3 months because it is not linked to a specific IP address.

    Funding Fee Payment Schedule
    ----------------------------
        - Binance : 00:00, 08:00, 16:00 UTC
        - FTX     : 24x per day, on the hour
        - Huobi   : 00:00, 08:00, 16:00 SST (UTC+8) ~ UTC
        - Bybit   : 00:00, 08:00, 16:00 UTC
        - OKEx    : 02:00, 10:00, 18:00 CEST (UTC+2) ~ UTC

     Funding Schedule Resources
     --------------------------
        - Binance : https://www.binance.com/en/support/faq/360033525031
        - FTX     : https://help.ftx.com/hc/en-us/articles/360027946571-Funding
        - Huobi   : https://huobiapi.github.io/docs/usdt_swap/v1/en/#order-and-trade
        - Bybit   : https://help.bybit.com/hc/en-us/articles/360039261114-What-is-funding-rate-and-predicted-rate-
        - OKEx    : https://www.okex.com/support/hc/en-us/articles/360020412631-XI-Funding-Rate-Calculations

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
                - OKEx
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


    @try5times
    def get_available_perps(self, count=1):
        '''
        Populate self.available_perps, for each pair traded at the exchanges, with a dict containing info on that pair.

        Output
        ------
        self.available_perps = {
            BTC: {
                USDT: {
                    'binance': True,
                    'ftx': False,
                    'huobi': True,
                    'bybit': True,
                    'okx': True
                },
                USD: {
                    'binance': True,
                    'ftx': True,
                    'huobi': True,
                    'bybit': True,
                    'okx': True
                }
            }
        '''
        self.available_perps = {}
        ## Get Exchange Pairs
        unparsed_pairs = {
            'binance': self.CommsBinance.get_perp_pairs,
            'ftx': self.CommsFTX.get_perp_pairs,
            'huobi': self.CommsHuobi.get_perp_pairs,
            'bybit': self.CommsBybit.get_perp_pairs,
            'okx': self.CommsOKX.get_perp_pairs
        }

        for exchange, get_pairs_func in unparsed_pairs.items():
            pairs = get_pairs_func()
            for pair, pair_dict in pairs.items():
                underlying = pair_dict['underlying_asset']
                quote = pair_dict['quote_asset']
                count += 1
                if count == 80:
                    x = 1
                try: # underlying asset has a dict, and quote asset has a dict
                    self.available_perps[underlying][quote].update({exchange: True})
                except KeyError:  # quote asset has no dict in underlying asset dict
                    try:
                        self.available_perps[underlying].update({quote: {exchange: True}})
                    except:  # underlying asset has no dict in self.available_perps
                        self.available_perps.update({underlying: {quote: {exchange: True}}})

        ## Add {Exchange: False} to each quote_dict in available_perps
        exchange_names = ['binance', 'ftx', 'huobi', 'bybit', 'okx']
        for underlying, quote_dict in self.available_perps.items():
            for quote, exchange_dict in quote_dict.items():
                for exchange in exchange_names:
                    exchange_dict.update({exchange: exchange_dict.get(exchange, False)})
                quote_dict.update({quote: exchange_dict})
            self.available_perps.update({underlying: quote_dict})
        return self.available_perps


    def get_predicted_rates(self):

        ## Create self.predicted_rates dict
        if self.available_perps is None:
            self.get_available_perps()
        self.predicted_rates = {underlying: {quote: {} for quote in self.available_perps[underlying].keys()} for underlying in self.available_perps.keys()}

        ## Get Predicted Rates
        underlying_symbols = list(self.predicted_rates.keys())
        pair_tuples = [(underlying, quote) for underlying in self.predicted_rates.keys() for quote in self.predicted_rates[underlying].keys()]
        pairs =  [f'{underlying}{quote}' for (underlying, quote) in pair_tuples]
        unparsed_rates = {
            'binance': self.CommsBinance.get_next_funding_rate(),
            'ftx': self.CommsFTX.get_next_funding_rate(underlying_symbols=underlying_symbols),
            'huobi': self.CommsHuobi.get_next_funding_rate(),
            'bybit': self.CommsBybit.get_next_funding_rate(pairs=pairs),
            'okx': self.CommsOKX.get_next_funding_rate(pair_tuples=pair_tuples)
        }

        ## Format Predicted Rates
        for exchange, exchange_specific_rates in unparsed_rates.items():
            for underlying, quote_dict in self.predicted_rates.items():
                for quote in quote_dict.keys():
                    pair = underlying + quote
                    try:
                        rate = exchange_specific_rates[pair]
                    except KeyError:
                        rate = None
                    self.predicted_rates[underlying][quote][exchange] = rate

        return self.predicted_rates


    def get_historical_rates(self):

        ## Create self.historical_rates dict
        if self.available_perps is None:
            self.get_available_perps()
        pair_rates_df_template = pd.DataFrame()
        self.historical_rates = {underlying: {quote: pair_rates_df_template.copy(deep=True) for quote in self.available_perps[underlying].keys()} for underlying in self.available_perps.keys()}

        ## Get Historical Rates
        unparsed_rates = {
            'binance': self.CommsBinance.get_historical_funding_rates(),
            'ftx': self.CommsFTX.get_historical_funding_rates(),
            'huobi': self.CommsHuobi.get_historical_funding_rates(),
            'bybit': self.CommsBybit.get_historical_funding_rates(),
            'okx': self.CommsOKX.get_historical_funding_rates(),
        }

        ## Format Historical Rates
        for exchange, exchange_specific_rates in unparsed_rates.items():
            for underlying, quote_dict in self.historical_rates.items():
                for quote in quote_dict.keys():
                    pair = underlying + quote
                    try:
                        rate = exchange_specific_rates[pair]
                    except KeyError:
                        rate = None
                    self.historical_rates[underlying][quote][exchange] = rate

        return self.historical_rates


CommsFundingRates().get_predicted_rates()


'''
To Do:
 - give necessary symbol formats to historical rates functions, then format the data into dataframes, then function is done, so work on handling this data in the strategy script
 - could do start/end time pagination for OKX and another one (FTX or Huobi). See if the data is smaller than other and then impliment if necessary
'''