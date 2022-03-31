## External Libraries
from enum import Enum
from pathlib import Path
import json
import os


class Folder(Enum):
    parent = Path(__file__).resolve().parents[1]
    SCRIPTS = os.path.join(parent, 'scripts')
    CONFIG = os.path.join(parent, 'config')
    DATA = os.path.join(parent, 'data')
    LOGS = os.path.join(parent, 'logs')

class File(Enum):
    CONFIG = os.path.join(Folder.CONFIG.value, 'config.json')

class Log(Enum):
    with open(File.CONFIG.value) as json_file:
        config = json.load(json_file)
    COMMS_BINANCE            = os.path.join(Folder.LOGS.value, config['CommsBinance']['log_file_name'])
    COMMS_FTX                = os.path.join(Folder.LOGS.value, config['CommsFTX']['log_file_name'])
    COMMS_HUOBI              = os.path.join(Folder.LOGS.value, config['CommsHuobi']['log_file_name'])
    COMMS_BYBIT              = os.path.join(Folder.LOGS.value, config['CommsBybit']['log_file_name'])
    COMMS_OKX                = os.path.join(Folder.LOGS.value, config['CommsOKX']['log_file_name'])
    COMMS_FUNDING_RATES      = os.path.join(Folder.LOGS.value, config['CommsFundingRates']['log_file_name'])

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
