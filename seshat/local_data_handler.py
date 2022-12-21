## Internal Modules
from seshat.utils_data_locations import DataLoc
from seshat.report_funding_rates import ReportFundingRates
from seshat.report_borrow_rates import ReportBorrowRates
from seshat.report_prices import ReportPrices

## External Libraries
from typing import List
import pandas as pd
from typing import List
from datetime import datetime, timedelta, timezone


class LocalDataHandler:
    '''
    Save data locally and loads locally stored data for these data:
        - Funding rates
        - Borrow rates (spot assets)
    '''

    def __init__(self):
        self.DataLoc = DataLoc()


    def months_between(self, start: datetime, end: datetime):
        '''
        Takes a start and end date and returns a list of datetimes,
        one for each month that occurs between these dates (inclusive).
        Each datetime is at midnight on the last day of the month.

        Reasoning
        ---------
        This function is necessary because pandas `date_range` function
        only returns full months between two dates. For example, if start
        and end are the first and last day of the year, respectively,
        pandas function will return 11 months.

        Inputs
        ------
        start : datetime
            E.g. datetime(2022,1,20)
        end : datetime
            E.g. datetime(2022,3,10)

        Output
        ------
        E.g. [datetime(2022,1,31), datetime(2022,2,28), datetime(2022,3,31)]
        '''
        last_date = end.replace(day=1) + timedelta(days=31)  # pd.date_range does not include the last month
        first_date = start
        dates = pd.date_range(first_date, last_date, freq='1M', normalize=True)  # list of datetimes, first of every month
        return dates


    def save_funding_rates(self, df: pd.DataFrame):
        '''
        Takes a DataFrame of funding rates and saves them by month,
        in the funding rates folder.
        '''
        ## Clean Inputs
        if df.empty:
            raise Exception('No data to save; dataframe is empty.')
        ## Make List of datetimes for all months that the input df contains
        dates = self.months_between(start=df.datetime.min(), end=df.datetime.max())
        ## Save Data
        for dt in dates:  # midnight on the last day of the month

            ## Index the Data for this Month
            start_date = dt.replace(day=1)
            end_date = dt + timedelta(days=1)
            mask = (start_date <= df.datetime) & (df.datetime < end_date)
            new_df = df[mask]

            ## Make sure you're not overwriting old data
            old_df = self.load_funding_rates(start=start_date, end=start_date)
            if old_df is not None:
                new_df = pd.concat([old_df, new_df]).drop_duplicates()

            ## A bit of Cleaning
            new_df.sort_values('datetime', ascending=True).reset_index(drop=True)

            ## Save Data for that Month
            file_name = f'{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.FUNDING_RATES.value + '/' + file_name
            new_df.to_pickle(file_loc)
        return


    def save_borrow_rates(self, df: pd.DataFrame):
        '''
        Takes a DataFrame of spot borrow rates and saves them by month,
        in the borrow rates folder.
        '''
        ## Clean Inputs
        if df.empty:
            raise Exception('No data to save; dataframe is empty.')
        ## Make List of datetimes for all months that the input df contains
        dates = self.months_between(start=df.datetime.min(), end=df.datetime.max())
        ## Save Data
        for dt in dates:  # midnight on the last day of the month

            ## Index the Data for this Month
            start_date = dt.replace(day=1)
            end_date = dt + timedelta(days=1)
            mask = (start_date <= df.datetime) & (df.datetime < end_date)
            new_df = df[mask]

            ## Make sure you're not overwriting old data
            old_df = self.load_borrow_rates(start=start_date, end=start_date)
            if old_df is not None:
                new_df = pd.concat([old_df, new_df]).drop_duplicates()

            ## A bit of Cleaning
            new_df.sort_values('datetime', ascending=True).reset_index(drop=True)

            ## Save Data for that Month
            file_name = f'{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.BORROW_RATES.value  + '/' + file_name
            new_df.to_pickle(file_loc)
        return


    def save_prices(self, df: pd.DataFrame, instrument: str, interval: str):
        '''
        Takes a DataFrame of prices from multiple exchanges and saves them in the prices rates folder,
        by instrument, interval, and month.

        Inputs
        ------
        df : pd.DataFrame
        interval : str
            It can be any string as this is only used for naming and retrieving files
        instrument : str
            One of [spot, perp]
        '''
        ## Clean Inputs
        if df.empty:
            raise Exception('No data to save; dataframe is empty.')
        ## Make List of datetimes for all months that the input df contains
        dates = self.months_between(start=df.datetime.min(), end=df.datetime.max())
        ## Save Data
        for dt in dates:  # midnight on the last day of the month

            ## Index the Data for this Month
            start_date = dt.replace(day=1)
            end_date = dt + timedelta(days=1)
            mask = (start_date <= df.datetime) & (df.datetime < end_date)
            new_df = df[mask]

            ## Make sure you're not overwriting old data
            old_df = self.load_prices(start=start_date, end=start_date, interval=interval, instrument=instrument)
            if old_df is not None:
                new_df = pd.concat([old_df, new_df]).drop_duplicates()

            ## A bit of Cleaning
            new_df.sort_values('datetime', ascending=True).reset_index(drop=True)

            ## Save Data for that Month
            file_name = f'{instrument}_{interval}_{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.PRICES.value  + '/' + file_name
            new_df.to_pickle(file_loc)
        return


    def load_funding_rates(self, start: datetime, end: datetime, underlying_symbols: List[str] = []):
        df_list = []
        dates = self.months_between(start=start, end=end)
        for dt in dates:  # midnight on the last day of the month
            file_name = f'{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.FUNDING_RATES.value  + '/' + file_name
            try:
                df_list.append(pd.read_pickle(file_loc))
            except FileNotFoundError:
                pass
        ## Merge Monthly Data into one DataFrame
        if df_list:
            df = pd.concat(df_list)
        else:
            return None
        ## Index DataFrame for requested info
        if underlying_symbols:
            df = df[df.underlying.isin(underlying_symbols)]
        df.sort_values('datetime', ascending=True).reset_index(drop=True)
        return df


    def load_borrow_rates(self, start: datetime, end: datetime, symbols: List[str] = []):
        df_list = []
        dates = self.months_between(start=start, end=end)
        for dt in dates:  # midnight on the last day of the month
            file_name = f'{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.BORROW_RATES.value  + '/' + file_name
            try:
                df_list.append(pd.read_pickle(file_loc))
            except FileNotFoundError:
                pass
        ## Merge Monthly Data into one DataFrame
        if df_list:
            df = pd.concat(df_list)
        else:
            return None
        ## Index DataFrame for requested info
        if symbols:
            df = df[df.symbol.isin(symbols)]
        df.sort_values('datetime', ascending=True).reset_index(drop=True)
        return df


    def load_prices(self, start: datetime, end: datetime, interval: str, instrument, underlying_symbols: List[str] = []):
        '''
        Loads a DataFrame of prices from multiple exchanges.

        Inputs
        ------
        df : pd.DataFrame
        start : datetime
        end : datetime
        interval : str
            It can be any string as this is only used for naming and retrieving files
        instrument : str
            One of [spot, perp]
        underlying_symbols : str : optional
            A list of symbols to return prices for.
            If unspecified, returns all symbols with downloaded data.
        '''
        df_list = []
        dates = self.months_between(start=start, end=end)
        for dt in dates:  # midnight on the last day of the month
            file_name = f'{instrument}_{interval}_{dt.year}_{dt.month}.py'
            file_loc = self.DataLoc.Folder.PRICES.value  + '/' + file_name
            try:
                df_list.append(pd.read_pickle(file_loc))
            except FileNotFoundError:
                pass
        ## Merge Monthly Data into one DataFrame
        if df_list:
            df = pd.concat(df_list)
        else:
            return None
        ## Index DataFrame for requested info
        if underlying_symbols:
            df = df[df.underlying.isin(underlying_symbols)]
        df.sort_values('datetime', ascending=True).reset_index(drop=True)
        return df


if __name__ == '__main__':
    ''' Download Historical Funding Rates'''
    # LocalDataHandler().save_funding_rates(ReportFundingRates().get_historical_rates())

    ''' Download Historical Borrow Rates'''
    # df = ReportBorrowRates().get_historical_rates(
    #     start=datetime(2019,1,1, tzinfo=timezone.utc),
    #     end=datetime(2020,1,1, tzinfo=timezone.utc)
    # )
    # LocalDataHandler().save_borrow_rates(df)

    ''' Download Historical Prices from 2022 - 2021'''
    # interval = '1h'
    # instruments = ['spot', 'perp']
    # underlying = ReportFundingRates().get_available_perps().underlying.tolist()
    # quote = ['USDT', 'USD']
    # for start, end in [
    #     (datetime(2020,1,1), datetime(2021,1,1)),
    #     (datetime(2021,1,1), datetime(2022,1,1)),
    #     (datetime(2022,1,1), datetime(2022,5,1)),
    # ]:
    #     for instrument in instruments:
    #         df = ReportPrices().get_historical_prices(
    #             interval=interval,
    #             start=start,
    #             end=end,
    #             instrument=instrument,
    #             underlying=underlying,
    #             quote=quote
    #         )
    #         LocalDataHandler().save_prices(df=df, instrument=instrument, interval=interval)


    ''' Testing Functions '''
    # dates = pd.date_range(datetime(2022,1,1), datetime(2022,4,17), freq='1D')
    # df = pd.DataFrame(data={
    #     'datetime': dates,
    #     'col1': np.ones(len(dates)),
    #     'col2': np.zeros(len(dates)),
    # })
    # df.datetime = df.datetime.apply(pd.to_datetime)
    # LocalDataHandler().save_funding_rates(df)


    ## Fill and Clean Data
    # L = LocalDataHandler()
    # folder = L.DataLoc.Folder.BORROW_RATES.value
    # from os import listdir
    # from os.path import isfile, join
    # files = [join(folder, f) for f in listdir(folder)]
    # for file in files:
    #     df = pd.read_pickle(file)
    #     df2 = df.sort_values('datetime', ascending=True).reset_index(drop=True)
        # x = 1
        # types = {column: 'float' for column in df.columns if column not in ['underlying', 'quote', 'datetime']}
        # df2 = df.astype(types)
        # df2.to_pickle(file)
