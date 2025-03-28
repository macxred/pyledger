"""Test suite for first_elements_as_str()."""

import pytest
from pyledger.helpers import first_elements_as_str


@pytest.mark.parametrize(
    "x,n,expected",
    [
        ([], 5, ""),  # Empty list
        ([1, 2, 3], 5, "1, 2, 3"),  # Less elements than n
        ([1, 2, 3, 4, 5], 5, "1, 2, 3, 4, 5"),  # Exactly n elements
        ([1, 2, 3, 4, 5, 6], 5, "1, 2, 3, 4, 5, ..."),  # More elements than n
        (["a", "b", "c", "d", "e", "f"], 3, "a, b, c, ..."),  # Strings and truncation
        ((["a", "b", "c", "d", "e", "f"]), 3, "a, b, c, ..."),  # Using tuple
        (set([1, 2, 3, 4, 5, 6]), 3, "1, 2, 3, ..."),  # Using set
        ([None, True, False], 2, "None, True, ..."),  # Mixed types and truncation
        ([None], 2, "None"),  # Single element
    ],
)
def test_first_elements_as_str(x, n, expected):
    assert first_elements_as_str(x, n) == expected
