## External Libraries
from functools import wraps
import time


def try5times(function):
    '''
    A retry decorator. Upon five failed attempts, returns the exception found on the fifth attempt at running the function.
    '''
    @wraps(function)
    def try5times_wrapper(self, *args, **kwargs):
        for repeat in range(5):
            try:
                return function(self, *args, **kwargs)
            except Exception:
                self.logger.exception(f'Error found when running {function}.')
                if repeat < 2:
                    time.sleep(0.5)
                elif repeat < 4:
                    time.sleep(2)
                else:  # repeat == 4 == 5th try
                    self.logger.critical(f'{function} failed to work after 5 attempts.')
                    raise
    return try5times_wrapper