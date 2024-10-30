"""Provides decorators for caching."""

from time import time
from functools import wraps


def timed_cache(seconds: int):
    """
    Decorator to cache a function's result for a specified duration.

    Caches the result of a function call and reuses it if the same arguments
    are used within `seconds` time. Once expired, the function is called
    again and the cache is refreshed.

    Args:
        seconds (int): Duration in seconds for the cache to remain valid.
    """
    def decorator(func):
        cache = {}

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Use args as the cache key for simplicity
            key = (args, frozenset(kwargs.items()))
            current_time = time()

            # If key is in cache and hasn't expired, return cached result
            if key in cache:
                result, timestamp = cache[key]
                if current_time - timestamp < seconds:
                    return result

            # If not in cache or expired, call the function and cache the result
            result = func(*args, **kwargs)
            cache[key] = (result, current_time)
            return result

        # Add a method to clear the cache manually
        def cache_clear():
            cache.clear()

        wrapper.cache_clear = cache_clear
        return wrapper
    return decorator
