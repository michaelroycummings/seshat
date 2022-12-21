## Internal Modules
from seshat.utils_logging import MyLogger
from seshat.utils_data_locations import DataLoc
from seshat.comms_binance import CommsBinance
from seshat.comms_ftx import CommsFTX
from seshat.comms_huobi import CommsHuobi
from seshat.comms_bybit import CommsBybit
from seshat.comms_okx import CommsOKX

## External Libraries
from typing import List
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta



class ReportFundingRates:
    '''
    This class provides methods to call organised funding rates data from multiple exchanges.

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
        - OKX     : https://www.okx.com/support/hc/en-us/articles/360020412631-XI-Funding-Rate-Calculations

    Funding Rate Calculation Period
    -------------------------------

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

    Funding Rate Obligation
    -----------------------
    Binance: funding rate are only received / applied to traders that have open
             open perp futures positions at the specified funding times (00:00, 8:00, 16:00).
             Existance or duration of open positions before / after funding rate times do not
             have an effect on paying/ receiving funding fees.

    '''

    def __init__(self):
        ## Get Data Locations and Config file for this class
        self.DataLoc = DataLoc()
        with open(self.DataLoc.File.CONFIG.value) as json_file:
            config = json.load(json_file)[self.__class__.__name__]
        ## Logger
        self.logger = MyLogger().configure_logger(fileloc=self.DataLoc.Log.REPORT_FUNDING_RATES.value)
        ## Comms
        self.CommsBinance = CommsBinance()  # has C2C (coin to coin; aka USDT) pairs and C2F (coin to fiat; aka USD) pairs
        self.CommsFTX = CommsFTX()          # has only C2F pairs
        self.CommsHuobi = CommsHuobi()      # has C2C and C2F pairs
        self.CommsBybit = CommsBybit()      # has C2C and C2F pairs
        self.CommsOKX = CommsOKX()          # has C2C and C2F pairs


    @staticmethod
    def _round_minutes(dt, resolution):
        new_hour = (dt.hour // resolution + 1) * resolution
        return (dt.replace(hour=0, minute=0, second=0) + timedelta(hours=new_hour))


    @staticmethod
    def funding_rate_per_day(income: float, exchange: float) -> float:
        '''
        Returns the daily return of a funding rate.
            NOTE: This method is NOT used in other methods of this class.
                  It is meant for strategies and classes that require time-standardized funding rates
                  across exchanges.

        Inputs
        ------
        income : float
            The return per funding rate period of holding the perpetual future.
        exchange : str
            The lowercase exchange name.

        Output
        ------
        float
        '''
        if exchange == 'ftx':
            return income * 24
        elif exchange in ['binance', 'huobi', 'bybit', 'okx']:
            return income * 8
        else:
            message = f'Funding rate frequency is not specified for this exchange: {exchange}.'
            raise Exception(message)


    def get_available_perps(self) -> pd.DataFrame:
        '''
        Returns a pd.DataFrame of perpetual futures that are traded at each exchange.

        Output
        ------
        pd.DataFrame
            underlying | quote | binance | ftx  | huobi | bybit | okx
            ----------------------------------------------------------
               'BTC'   | 'USD' |  True   | True |  True | True  | True
               'BTC'   | 'USDT'|  True   | False|  True | True  | True
               'ETH'   | 'USDT'|  True   | True |  True | True  | True
        '''
        available_perps = pd.DataFrame(columns=['underlying', 'quote'])
        ## Get Exchange Pairs
        unparsed_pairs = {
            'binance': self.CommsBinance.get_perp_pairs,
            'ftx': self.CommsFTX.get_perp_pairs,
            'huobi': self.CommsHuobi.get_perp_pairs,
            'bybit': self.CommsBybit.get_perp_pairs,
            'okx': self.CommsOKX.get_perp_pairs
        }

        for exchange, func in unparsed_pairs.items():
            pairs_df = func()
            pairs_df[exchange] = True
            available_perps = available_perps.merge(pairs_df, how='outer', on=['underlying', 'quote'])

        available_perps.fillna(False, inplace=True)
        return available_perps


    def get_predicted_rates(
        self, available_perps: pd.DataFrame = None, requested_symbols: List[str] = None
    ) -> pd.DataFrame:
        '''
        Returns a DataFrame of predicted funding rates (for the next funding period)
        for each pair at each exchange.

        Inputs
        ------
        requested_symbols : List[str]
            A list of UPPERCASE symbols to get rates for.
        available_perps :  pd.DataFrame
            A DataFrame of columns ['underlying', 'quote', 'exchange_1', 'exchange_2', etc.]
            with UPPERCASE str for 'underlying' and 'quote' columns, and True or False for
            each exchange column value.


        Output
        ------
        pd.DataFrame
             datetime  | underlying | quote | binance |  ftx  |  huobi | bybit | okx
            --------------------------------------------------------------------------
            2022.01.01 |   'BTC'    |'USDT' |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.01 |   'ETH'    |'USDT' |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
        '''
        main_cols = ['datetime', 'underlying', 'quote']
        predicted_rates = pd.DataFrame(columns=main_cols)

        ## Get perp futures pairs available at all exchanges
        if available_perps is None:
            available_perps = self.get_available_perps()
        ## Get perp futures for underlying assets requested
        if requested_symbols is not None:
            available_perps = available_perps[available_perps['underlying'].isin(requested_symbols)]

        ## Function gets available pairs at a single exchange
        underlying_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, 'underlying'])
        pairs_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, ['underlying', 'quote']].to_records(index=False))

        ## Set Up Functions to Execute
        funcs = {
            'binance': (
                self.CommsBinance.get_next_funding_rate,
                {
                    'pairs': pairs_at_exchange('binance', available_perps)
                }
            ),
            'ftx': (
                self.CommsFTX.get_next_funding_rate,
                {
                    'underlying_symbols': underlying_at_exchange('ftx', available_perps)
                }
            ),
            'huobi': (
                self.CommsHuobi.get_next_funding_rate,
                {
                    'pairs': pairs_at_exchange('huobi', available_perps)
                }
            ),
            'bybit': (
                self.CommsBybit.get_next_funding_rate,
                {
                    'pairs': pairs_at_exchange('bybit', available_perps)
                }
            ),
            'okx'     : (
                self.CommsOKX.get_next_funding_rate,
                {
                    'pairs': pairs_at_exchange('okx', available_perps)
                }
            ),
        }

        ## Query Predicted Rates
        for exchange, (func, kwargs) in funcs.items():

            ## Request Rates from API
            try:
                exchange_df = func(**kwargs)
            except Exception:
                message = f'{exchange.capitalize()} API failed while pulling funding rates.'
                self.logger.exception(message)
                print(message, flush=True)
                continue

            ## Parse Rates
            exchange_df = exchange_df.rename(columns={'rate': exchange})
            predicted_rates = predicted_rates.merge(exchange_df, how='outer', on=main_cols)

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
        available_perps: pd.DataFrame = None
    ) -> pd.DataFrame:
        '''
        Returns a DataFrame of historical funding rates for each pair at each exchange.
            NOTE: does not query data from Bybit as their api gives only the last value (they give historical data through csv sheets)

        Inputs
        ------
        start : datetime
        end : datetime
        requested_symbols : List[str]
            A list of UPPERCASE symbols to get rates for.
        available_perps :  pd.DataFrame
            A DataFrame of columns ['underlying', 'quote', 'exchange_1', 'exchange_2', etc.]
            with UPPERCASE str for 'underlying' and 'quote' columns, and True or False for
            each exchange column value.

        Output
        ------
        pd.DataFrame
             datetime  | underlying | quote | binance |  ftx  |  huobi | bybit | okx
            --------------------------------------------------------------------------
            2022.01.02 |   'BTC'    |'USDT' |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.02 |   'ETH'    |'USDT' |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
            2022.01.01 |   'BTC'    |'USDT' |  0.001  | 0.001 |  0.001 | 0.001 | 0.001
            2022.01.01 |   'ETH'    |'USDT' |  0.002  | 0.002 |  0.002 | 0.002 | 0.002
        '''
        main_cols = ['datetime', 'underlying', 'quote']
        historical_rates = pd.DataFrame(columns=main_cols)

        ## Get perp futures pairs available at all exchanges
        if available_perps is None:
            available_perps = self.get_available_perps()
        ## Get perp futures for underlying assets requested
        if requested_symbols is not None:
            available_perps = available_perps[available_perps['underlying'].isin(requested_symbols)]

        ## Function gets available pairs at a single exchange
        underlying_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, 'underlying'])
        pairs_at_exchange = lambda exchange, df: list(df.loc[df[exchange] == True, ['underlying', 'quote']].to_records(index=False))

        ## Set Up Functions to Execute
        funcs = {
            'binance' : (
                self.CommsBinance.get_historical_funding_rates,
                {
                    'pairs': pairs_at_exchange('binance', available_perps),
                    'start': start,
                    'end': end,
                }
            ),
            'ftx'     : (
                self.CommsFTX.get_historical_funding_rates,
                {
                    'underlying_symbols': underlying_at_exchange('ftx', available_perps),
                    'start': start,
                    'end': end,
                }
            ),
            'huobi'   : (
                self.CommsHuobi.get_historical_funding_rates,
                {
                    'pairs': pairs_at_exchange('huobi', available_perps),
                    'start': start,
                    'end': end,
                }
            ),
            'okx'     : (
                self.CommsOKX.get_historical_funding_rates,
                {
                    'pairs': pairs_at_exchange('okx', available_perps),
                    'start': start,
                    'end': end,
                }
            ),
        }

        ## Query Historical Rates
        for exchange, (func, kwargs) in funcs.items():

            ## Request Rates from API
            try:
                exchange_df = func(**kwargs)
            except Exception:
                message = f'{exchange.capitalize()} API failed while pulling funding rates.'
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

    # next_rates_df = ReportFundingRates().get_predicted_rates(requested_symbols=['BTC', 'ETH'])

    # historical_df = ReportFundingRates(
    #     ).get_historical_rates(
    #         start=datetime(2022,1,1),
    #         end=datetime(2022,2,1),
    #         requested_symbols=['ADA', 'MATIC'])