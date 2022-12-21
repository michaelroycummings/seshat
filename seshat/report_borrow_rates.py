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

class ReportBorrowRates:
    '''
    This class provides methods to call organised spot borrow rates (interest rates) data from multiple exchanges.
        NOTE: Right now the class is a bit redundant as it only grabs data from Binance.

    Binance sets interest rates daily.
    '''

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]
        ## Logger
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.REPORT_BORROW_RATES.value)
        ## Comms
        self.CommsBinance = CommsBinance()
        self.CommsFTX = CommsFTX()
        self.CommsHuobi = CommsHuobi()


    def get_borrowable_assets(self) -> pd.DataFrame:
        '''
        Returns a DataFrame of all borrowable assets at each exchange.

        Output
        ------
        pd.DataFrame
            symbol | binance | ftx  | huobi | bybit | okx
            ---------------------------------------------
            'BTC'  |  True   | True |  True | True  | True
            'ETH'  |  True   | False|  True | True  | True
            'LTC'  |  True   | True |  True | False | True
        '''
        borrowable_assets = pd.DataFrame(columns=['symbol'])
        exchange_funcs = {
            'binance': self.CommsBinance.get_borrowable_assets,
            'ftx': self.CommsFTX.get_borrowable_assets,
            # 'huobi': self.CommsHuobi.get_borrowable_assets,
        }
        ## Get Borrowable Assets for each Exchange
        for exchange, get_pairs_func in exchange_funcs.items():
            symbols = get_pairs_func()
            data = {
                'symbol': symbols,
                exchange: [True]*len(symbols),
            }
            exchange_df = pd.DataFrame(data)
            borrowable_assets = borrowable_assets.merge(exchange_df, how='outer', on=['symbol'])
        ## Fill cells for assets not listed on that exchange
        borrowable_assets.fillna(False, inplace=True)
        return borrowable_assets


    def get_current_rates(self,
        requested_symbols: List[str] = None, borrowable_assets: pd.DataFrame = None
    ) -> pd.DataFrame:
        '''
        Returns a DataFrame of today's interest rate for each borrowable asset at each exchange.

        Inputs
        ------
        requested_symbols : List[str]
            A list of UPPERCASE symbols to get rates for.
        borrowable_assets :  pd.DataFrame
            A DataFrame of columns ['symbol', 'exchange_1', 'exchange_2', etc.]
            with UPPERCASE str for symbols, and True or False for each exchange column
            value.

        Output
        ------
        pd.DataFrame
             datetime  | symbol | binance |  ftx  | huobi  | bybit | okx
            -------------------------------------------------------------
            2022.01.01 | 'BTC'  |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.01 | 'ETH'  |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
            2022.01.01 | 'LTC'  |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
        '''
        main_cols = ['datetime', 'symbol']
        current_rates = pd.DataFrame(columns=main_cols)

        ## Get borrowable assets available at all exchanges
        if borrowable_assets is None:
            borrowable_assets = self.get_borrowable_assets()
        ## Get borrowable assets requested
        if requested_symbols is not None:
            borrowable_assets = borrowable_assets[borrowable_assets['symbol'].isin(requested_symbols)]

        ## Function gets available pairs at a single exchange
        symbols_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, 'symbol'])

        ## Set up functions to query interest rate data
        funcs = {
            'binance': (
                self.CommsBinance.get_current_borrow_rate,
                {
                    'symbols': symbols_at_exchange('binance', borrowable_assets)
                }
            ),
            'ftx': (
                self.CommsFTX.get_current_borrow_rate,
                {
                    'symbols': symbols_at_exchange('ftx', borrowable_assets)
                }
            ),
            'huobi': (
                self.CommsHuobi.get_current_borrow_rate,
                {}
            ),
        }

        ## Query Borrow Rates Data
        for exchange, (func, kwargs) in funcs.items():

            ## Request Rates from API
            try:
                exchange_df = func(**kwargs)
            except Exception:
                message = f'{exchange.capitalize()} API failed while pulling spot borrow rates.'
                self.logger.exception(message)
                print(message, flush=True)
                continue

            ## Parse Rates
            exchange_df = exchange_df.rename(columns={'rate': exchange})
            current_rates = current_rates.merge(exchange_df, how='outer', on=['datetime', 'symbol'])

        ## Reorder Columns
        rates_cols = list(current_rates.columns.difference(main_cols))
        column_order = main_cols + rates_cols
        current_rates = current_rates.loc[:, column_order]

        ## Clean Data
        current_rates.fillna(np.NaN, inplace=True)  # Fill cells for assets not listed on that exchange
        types = {column: 'float' for column in rates_cols}
        current_rates = current_rates.astype(types)  # Ensure float types for all rates

        return current_rates



    def get_historical_rates(self,
        start: datetime, end: datetime, requested_symbols: List[str] = None,
        borrowable_assets: pd.DataFrame = None
    ) -> pd.DataFrame:
        '''
        Returns a DataFrame of historical interest rates for each borrowable asset
        at each exchange, from 30 days previous until today.

        Inputs
        ------
        start : datetime
        end : datetime
        requested_symbols : List[str]
            A list of UPPERCASE symbols to get rates for.
        borrowable_assets :  pd.DataFrame
            A DataFrame of columns ['symbol', 'exchange_1', 'exchange_2', etc.]
            with UPPERCASE str for symbols, and True or False for each exchange column
            value.


        Output
        ------
        pd.DataFrame
             datetime  | symbol | binance |  ftx  | huobi  | bybit | okx
            -------------------------------------------------------------
            2022.01.02 | 'BTC'  |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.02 | 'ETH'  |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
            2022.01.01 | 'BTC'  |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.01 | 'ETH'  |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
        '''
        main_cols = ['datetime', 'symbol']
        historical_rates = pd.DataFrame(columns=main_cols)

        ## Get borrowable assets available at all exchanges
        if borrowable_assets is None:
            borrowable_assets = self.get_borrowable_assets()
        ## Get borrowable assets requested
        if requested_symbols is not None:
            borrowable_assets = borrowable_assets[borrowable_assets['symbol'].isin(requested_symbols)]

        ## Function gets available pairs at a single exchange
        symbols_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, 'symbol'])

        ## Set up functions to query interest rate data
        funcs = {
            'binance': (
                self.CommsBinance.get_historical_borrow_rate,
                {
                    'symbols': symbols_at_exchange('binance', borrowable_assets),
                    'start': start,
                    'end': end,
                }
            ),
            'ftx': (
                self.CommsFTX.get_historical_borrow_rate,
                {
                    'symbols': symbols_at_exchange('ftx', borrowable_assets),
                    'start': start,
                    'end': end,
                }
            )
        }

        ## Query Borrow Rates Data
        for exchange, (func, kwargs) in funcs.items():

            ## Request Rates from API
            try:
                exchange_df = func(**kwargs)
            except Exception:
                message = f'{exchange.capitalize()} API failed while pulling spot borrow rates.'
                self.logger.exception(message)
                print(message, flush=True)
                continue

            ## Parse Rates
            exchange_df = exchange_df.rename(columns={'rate': exchange})
            historical_rates = historical_rates.merge(exchange_df, how='outer', on=main_cols)

        ## Reorder Columns
        rates_cols = list(historical_rates.columns.difference(main_cols))
        column_order = main_cols + rates_cols
        historical_rates = historical_rates.loc[:, column_order]

        ## Clean Data
        historical_rates.fillna(np.NaN, inplace=True)  # Fill cells for assets not listed on that exchange
        types = {column: 'float' for column in rates_cols}
        historical_rates = historical_rates.astype(types)  # Ensure float types for all rates

        return historical_rates


if __name__ == '__main__':
    pass

    ## EXAMPLE USAGE ##

    # current_df = ReportBorrowRates(
    #     ).get_current_rates(requested_symbols=['BTC', 'ETH', 'LTC'])

    # historical_df = ReportBorrowRates(
    #     ).get_historical_rates(
    #         start=datetime(2022,5,1),
    #         end=datetime(2022,5,5),
    #         requested_symbols=['ETH', 'DOT'])