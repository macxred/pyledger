"""Test suite for parse_account_range() method."""

import pytest
from pyledger import MemoryLedger
from pyledger.tests.base_test import BaseTest


@pytest.fixture
def engine():
    engine = MemoryLedger()
    engine.restore(accounts=BaseTest.ACCOUNTS, tax_codes=BaseTest.TAX_CODES)
    return engine


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Single account (int)
        (1000, {"add": [1000], "subtract": []}),
        # List of accounts
        ([1000, 1020, 1025], {"add": [1000, 1020, 1025], "subtract": []}),
        # Dict format
        ({"add": [1000, 1020], "subtract": [1025]}, {"add": [1000, 1020], "subtract": [1025]}),
        # String: single account
        ("1000", {"add": [1000], "subtract": []}),
        # String: account range
        ("1000:1020", {"add": [1000, 1005, 1010, 1015, 1020], "subtract": []}),
        # String: multiple accounts with +
        ("1000+1020:1025", {"add": [1000, 1020, 1025], "subtract": []}),
        # String: multiple accounts with +
        ("1000:1010+1020:1020", {"add": [1000, 1005, 1010, 1020], "subtract": []}),
        # String: range with subtraction
        ("1020:1025-1000", {"add": [1020, 1025], "subtract": [1000]}),
    ]
)
def test_parse_account_range_valid_inputs(input_value, expected_output, engine):
    """Test valid inputs for parse_account_range."""
    assert engine.parse_account_range(input_value) == expected_output


@pytest.mark.parametrize(
    "invalid_input",
    [
        "abc",  # Invalid string
        "1000:abc",  # Partially invalid string
        {"add": [1000, "wrong"], "subtract": [1025]},  # Invalid dict with string
        ["1000", 1020],  # Invalid list with a string
        3.14,  # Float input
        None,  # None input
    ]
)
def test_parse_account_range_invalid_inputs(invalid_input, engine):
    """Test invalid inputs for parse_account_range, expecting ValueError."""
    with pytest.raises(ValueError):
        engine.parse_account_range(invalid_input)
