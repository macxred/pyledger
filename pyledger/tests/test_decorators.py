import time
from pyledger.decorators import timed_cache


# Function using the timed_cache decorator
@timed_cache(seconds=2)
def slow_function(x):
    return x * x


def test_cache_works_within_timeout():
    result1 = slow_function(2)
    result2 = slow_function(2)

    assert result1 == result2
    assert result1 == 4


def test_cache_expires_after_timeout():
    result1 = slow_function(2)
    time.sleep(3)
    result2 = slow_function(2)

    assert result1 == result2
    assert result1 == 4
    assert result1 != slow_function(3)


def test_cache_handles_different_arguments():
    result1 = slow_function(2)
    result2 = slow_function(3)

    assert result1 != result2
    assert result1 == 4
    assert result2 == 9


def test_cache_with_immediate_timeout():
    @timed_cache(seconds=0)
    def fast_expiring_function(x):
        return x * x

    result1 = fast_expiring_function(2)
    time.sleep(1)
    result2 = fast_expiring_function(2)

    assert result1 == result2
    assert result1 == 4


def test_manual_invalidation():
    result1 = slow_function(2)
    result2 = slow_function(2)

    assert result1 == result2

    slow_function.cache_clear()
    result3 = slow_function(2)
    assert result1 == result3
    assert result2 == result3
