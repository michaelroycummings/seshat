from typing import List
import pandas as pd
from typing import List
from datetime import datetime


class LocalDataHandler:
    '''
    Save data locally and loads locally stored data for these data:
        - Funding rates
        - Borrow rates (spot assets)
    '''

    def __init__(self):
        pass

    def save_funding_rates(df: pd.DataFrame):
        pass

    def save_borrow_rates(df: pd.DataFrame):
        pass

    def load_funding_rates(symbols: List[str], start: datetime, end: datetime):
        pass

    def load_borrow_rates(symbols: List[str], start: datetime, end: datetime):
        pass