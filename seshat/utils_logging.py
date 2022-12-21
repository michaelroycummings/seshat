## External Libraries
import logging


class MyLogger():

    def __init__(self):
        pass


    def configure_logger(self, fileloc: str, add2self: bool = True):
        '''
        Parameters
        ----------
        fileloc : str
            The Path (in str format) that the log file will be saved to

        Future Additions
        ----------------
        Maybe put all logs into json file and use a Log Explorer: https://www.datadoghq.com/blog/python-logging-best-practices/
        Multiprocess logging without having to pass a log_queue to everything: https://stackoverflow.com/questions/60830938/python-multiprocessing-logging-via-queuehandler
        '''
        logger = logging.getLogger(fileloc)
        logger.setLevel(logging.DEBUG)
        ## Create and Add Handler to Logger
        # if class_name != '':
        #     if logger.hasHandlers():  # this stops the same log message from being added multiple and increasing number of times
        #         logger.handlers.clear()
        ## Create / add File Handler
        file_handler = logging.FileHandler(fileloc)
        file_handler.setFormatter(logging.Formatter(f'%(asctime)s \t %(levelname)s \t %(filename)s - %(funcName)s: \t %(message)s', datefmt='%y-%m-%d %H:%M:%S'))
        logger.addHandler(file_handler)
        if add2self is True:
            self.logger = logger
            return logger