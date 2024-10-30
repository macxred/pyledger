"""This module is designed for storing decorators."""

from time import time
from functools import wraps


def timed_cache(seconds: int):
    """Decorator to cache function result with expiration time in seconds."""
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
