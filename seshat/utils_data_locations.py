## External Libraries
from enum import Enum
from pathlib import Path
import json
import os


class Folder(Enum):
    parent = Path(__file__).resolve().parents[0]
    CONFIG = os.path.join(parent, 'config')
    LOGS = os.path.join(parent, 'logs')
    ## Data Folder
    DATA = os.path.join(parent, 'data')
    FUNDING_RATES = os.path.join(DATA, 'funding_rates')
    BORROW_RATES = os.path.join(DATA, 'borrow_rates')
    PRICES = os.path.join(DATA, 'prices')

class File(Enum):
    CONFIG = os.path.join(Folder.CONFIG.value, 'config.json')
    API_CONFIG = os.path.join(Folder.CONFIG.value, 'api_config.json')

class Log(Enum):
    with open(File.CONFIG.value) as json_file:
        config = json.load(json_file)
    COMMS_BINANCE             = os.path.join(Folder.LOGS.value, config['CommsBinance']['log_file_name'])
    COMMS_FTX                 = os.path.join(Folder.LOGS.value, config['CommsFTX']['log_file_name'])
    COMMS_HUOBI               = os.path.join(Folder.LOGS.value, config['CommsHuobi']['log_file_name'])
    COMMS_BYBIT               = os.path.join(Folder.LOGS.value, config['CommsBybit']['log_file_name'])
    COMMS_OKX                 = os.path.join(Folder.LOGS.value, config['CommsOKX']['log_file_name'])
    REPORT_FUNDING_RATES      = os.path.join(Folder.LOGS.value, config['ReportFundingRates']['log_file_name'])
    REPORT_BORROW_RATES       = os.path.join(Folder.LOGS.value, config['ReportBorrowRates']['log_file_name'])
    REPORT_PRICES             = os.path.join(Folder.LOGS.value, config['ReportPrices']['log_file_name'])

class DataLoc():
    '''
    Stores the absolute location of all folder, file, and log-file locations needed for this script.
    '''
    def __init__(self):
        self.Folder = Folder
        self.File = File
        self.Log = Log
        ## Make sure all Folders mentioned, Exist
        for folder_loc in Folder:
            os.makedirs(folder_loc.value, exist_ok=True)
