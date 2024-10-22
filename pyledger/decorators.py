"""This module designed for storing decorators"""

from functools import cache, wraps
from time import time


def timed_cache(seconds: int):
    """Decorator to cache function results with a time-based expiration."""
    def wrapper(func):
        cache_expiration = {}

        @cache
        @wraps(func)
        def wrapped(*args, **kwargs):
            current_time = time()

            # Check if the result is cached and still valid
            if args in cache_expiration and current_time < cache_expiration[args]:
                return func(*args, **kwargs)

            # Cache expired or doesn't exist, recalculate
            result = func(*args, **kwargs)
            cache_expiration[args] = current_time + seconds
            return result

        return wrapped
    return wrapper
