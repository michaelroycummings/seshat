## Internal Modules
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc
from seshat.comms_binance import CommsBinance
from seshat.comms_ftx import CommsFTX
from seshat.comms_huobi import CommsHuobi

## External Libraries
import json
import pandas as pd
import numpy as np
from typing import List
from datetime import datetime


class ReportPrices:
    '''
    This class provides methods to call organised price data from multiple exchanges.
    '''

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]
        ## Logger
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.REPORT_PRICES.value)
        ## Comms
        self.CommsBinance = CommsBinance()
        self.CommsFTX = CommsFTX()
        self.CommsHuobi = CommsHuobi()


    def get_spot_pairs(self) -> pd.DataFrame:
        '''
        Returns a DataFrame of all tradable spot pairs at each exchange.

        Output
        ------
        pd.DataFrame
            underlying | quote | binance | ftx  | huobi | bybit | okx
            ----------------------------------------------------------
               'BTC'   | 'USD' |  True   | True |  Fals | True  | True
               'BTC'   | 'USDT'|  True   | False|  True | True  | True
               'ETH'   | 'USDT'|  True   | True |  True | True  | True
        '''
        spot_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        exchange_funcs = {
            'binance': self.CommsBinance.get_spot_pairs,
            'ftx': self.CommsFTX.get_spot_pairs,
            'huobi': self.CommsHuobi.get_spot_pairs,
        }
        ## Get Tradable Pairs for each Exchange
        for exchange, func in exchange_funcs.items():
            pairs_df = func()
            pairs_df[exchange] = True
            spot_pairs = spot_pairs.merge(pairs_df, how='outer', on=['underlying', 'quote'])

        ## Fill cells for assets not listed on that exchange
        spot_pairs.fillna(False, inplace=True)
        return spot_pairs


    def get_perp_pairs(self) -> pd.DataFrame:
        '''
        Returns a DataFrame of all tradable perp pairs at each exchange.

        Output
        ------
        pd.DataFrame
            underlying | quote | binance | ftx  | huobi | bybit | okx
            ----------------------------------------------------------
               'BTC'   | 'USD' |  True   | True |  Fals | True  | True
               'BTC'   | 'USDT'|  True   | False|  True | True  | True
               'ETH'   | 'USDT'|  True   | True |  True | True  | True
        '''
        perp_pairs = pd.DataFrame(columns=['underlying', 'quote'])
        exchange_funcs = {
            'binance': self.CommsBinance.get_perp_pairs,
            'ftx': self.CommsFTX.get_perp_pairs,
            'huobi': self.CommsHuobi.get_perp_pairs,
        }
        ## Get Tradable Pairs for each Exchange
        for exchange, func in exchange_funcs.items():
            pairs_df = func()
            pairs_df[exchange] = True
            perp_pairs = perp_pairs.merge(pairs_df, how='outer', on=['underlying', 'quote'])

        ## Fill cells for assets not listed on that exchange
        perp_pairs.fillna(False, inplace=True)
        return perp_pairs


    def get_current_prices(self, available_pairs: pd.DataFrame = None, requested_underlying: List[str] = None) -> pd.DataFrame:
        pass


    def get_historical_prices(self,
        interval: str, start: datetime, end: datetime, instrument: str,
        underlying: List[str] = None, quote: List[str] = None,
        available_pairs: pd.DataFrame = None
    ) -> pd.DataFrame:
        '''
        Inputs
        ------
        interval : str
            Format {number}{time unit}
            Where time unit is one of s (second), m (minute), h (hour), d (day)
        start : datetime
        end : datetime
        instrument : str
            One of ['spot', 'perp']
        underlying : List[str]
            A list of UPPERCASE symbols.
            Returns all pairs with any of these symbols as the underlying (vs quote) asset.
        quote : List[str]
            A list of UPPERCASE symbols.
            Returns all pairs with any of these symbols as the quote asset.
        available_pairs :  pd.DataFrame
            A DataFrame of all pairs available at each exchange. Only use this arguemnt if you've
            already called `self.get_spot_pairs()` and want to save execution time.

        Output
        ------
        pd.DataFrame
             datetime  | underlying | quote | binance |  ftx  |  huobi | bybit | okx
            --------------------------------------------------------------------------
            2022.01.02 |   'BTC'    |'USDT' |  42000  | 42000 |  42000 | 42000 | 42000
            2022.01.02 |   'ETH'    |'USDT' |   4030  |  4030 |   4030 |  4030 |  4030
            2022.01.01 |   'BTC'    |'USDT' |  42000  | 42000 |  42000 | 42000 | 42000
            2022.01.01 |   'ETH'    |'USDT' |   4030  |  4030 |   4030 |  4030 |  4030


        '''
        ## Clean Inputs
        if instrument not in ['spot', 'perp']:
            raise Exception(f'Value for "instrument" parameter should be one of [spot, perp]. Received "{instrument}.')

        main_cols = ['datetime', 'underlying', 'quote']
        historical_prices = pd.DataFrame(columns=main_cols)

        ## Get Trading Pairs trading at each exchange
        if available_pairs is None:
            if instrument == 'spot':
                available_pairs = self.get_spot_pairs()
            elif instrument == 'perp':
                available_pairs = self.get_perp_pairs()

        ## Shorten Trading Pairs df for Underlying Assets and Quote Assets requested
        if underlying is not None:
            available_pairs = available_pairs[available_pairs['underlying'].isin(underlying)]
        if quote is not None:
            available_pairs = available_pairs[available_pairs['quote'].isin(quote)]

        ## Function gets available pairs at a single exchange
        pairs_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, ['underlying', 'quote']].to_records(index=False))

        ## Set up functions to query price data
        if instrument == 'spot':
            funcs = {
                'binance': (
                    self.CommsBinance.get_historical_spot_price,
                    {
                        'pairs': pairs_at_exchange('binance', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                ),
                'ftx': (
                    self.CommsFTX.get_historical_spot_price,
                    {
                        'pairs': pairs_at_exchange('ftx', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                ),
                'huobi': (
                    self.CommsHuobi.get_historical_spot_price,
                    {
                        'pairs': pairs_at_exchange('huobi', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                )
            }
        elif instrument == 'perp':
            funcs = {
                'binance': (
                    self.CommsBinance.get_historical_perp_price,
                    {
                        'pairs': pairs_at_exchange('binance', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                ),
                'ftx': (
                    self.CommsFTX.get_historical_perp_price,
                    {
                        'pairs': pairs_at_exchange('binance', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                ),
                'huobi': (
                    self.CommsHuobi.get_historical_perp_price,
                    {
                        'pairs': pairs_at_exchange('binance', available_pairs),
                        'start': start,
                        'end': end,
                        'interval': interval
                    }
                )
            }

        ## Query Price Data
        for exchange, (func, kwargs) in funcs.items():

            ## Request Rates from API
            try:
                exchange_df = func(**kwargs)
            except Exception:
                message = f'{exchange.capitalize()} API failed while pulling historical prices for {instrument}.'
                self.logger.exception(message)
                print(message, flush=True)
                continue

            ## Parse Rates
            if not exchange_df.empty:
                exchange_df = exchange_df[['underlying', 'quote', 'open_time', 'open', 'close', 'high', 'low']]
                exchange_df = exchange_df.rename(columns={
                    'open_time': 'datetime',
                    'open': f'open_{exchange}',
                    'close': f'close_{exchange}',
                    'high': f'high_{exchange}',
                    'low': f'low_{exchange}',
                })
                historical_prices = historical_prices.merge(exchange_df, how='outer', on=main_cols)

        ## Fill and Clean Data
        historical_prices.fillna(np.NaN, inplace=True)  # fill cells for assets not listed on that exchange
        #

        #
        types = {column: 'float' for column in funcs.keys() if column in historical_prices.columns}
        historical_prices = historical_prices.astype(types)
        return historical_prices

        # ## Reorder Columns
        # rates_cols = list(historical_rates.columns.difference(main_cols))
        # column_order = main_cols + rates_cols
        # historical_rates = historical_rates.loc[:, column_order]

        # ## Slice Dataframe for requested dates
        # time_mask = (start <= historical_prices.datetime) & (historical_prices.datetime < end)
        # historical_prices = historical_prices[time_mask]

        # ## Clean Data
        # historical_rates.fillna(np.NaN, inplace=True)  # Fill cells for assets not listed on that exchange
        # types = {column: 'float' for column in rates_cols}
        # historical_rates = historical_rates.astype(types)  # Ensure float types for all rates

        # return historical_rates


if __name__ == '__main__':
    pass

    ## EXAMPLE USAGE ##

    # ReportPrices().get_current_prices()

    # ReportPrices(
    #     ).get_historical_prices(
    #         interval='1h',  # format is {float}{[s,m,h,d]}  # Examples: 30s, 1.5m, 12h, 7d
    #         start=datetime(2021,12,1),
    #         end=datetime(2022,2,1),
    #         instrument='spot',  # 'spot' or 'perp'
    #         underlying=['BTC', 'ETH'],
    #         quote=['USD', 'USDT'])