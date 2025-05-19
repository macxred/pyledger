"""Test suite for parse_profit_center_range() method."""

import pytest
from pyledger import MemoryLedger


@pytest.fixture
def engine():
    """Fixture to initialize and restore the ledger engine."""
    engine = MemoryLedger()
    return engine


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Single profit center
        ("Shop", {"add": ["Shop"], "subtract": []}),
        # List of profit centers
        (["Shop", "Bakery"], {"add": ["Shop", "Bakery"], "subtract": []}),
        # Dict format
        ({"add": ["Shop"], "subtract": ["Bakery"]}, {"add": ["Shop"], "subtract": ["Bakery"]}),
        # String with + only
        ("Shop+General", {"add": ["Shop", "General"], "subtract": []}),
        # String with multiple + only
        ("Shop+General+Bakery", {"add": ["Shop", "General", "Bakery"], "subtract": []}),
        # String with + and -
        ("Shop+General-Bakery", {"add": ["Shop", "General"], "subtract": ["Bakery"]}),
        # Subtract only
        ("-General", {"add": [], "subtract": ["General"]}),
        # Multiple subtracts
        ("-General-Bakery", {"add": [], "subtract": ["General", "Bakery"]}),
        # Add and subtract, reversed order
        ("-Bakery+Shop", {"add": ["Shop"], "subtract": ["Bakery"]}),
    ]
)
def test_parse_profit_center_range_valid_inputs(input_value, expected_output, engine):
    """Test valid inputs for parse_profit_center_range."""
    assert engine.parse_profit_center_range(input_value) == expected_output


@pytest.mark.parametrize(
    "invalid_input",
    [
        {"add": ["Shop", 123], "subtract": ["Bakery"]},  # Invalid dict (non-str)
        {"add": ["Shop"]},  # Missing 'subtract'
        {"subtract": ["General"]},  # Missing 'add'
        ["Shop", 123],  # Invalid list (non-str)
        123,  # Non-string non-list
        3.14,  # Float
        None,  # None input
        "",  # Empty string input
        [],  # Empty list
        {"add": [], "subtract": []},  # Empty dict
    ]
)
def test_parse_profit_center_range_invalid_inputs(invalid_input, engine):
    """Test invalid inputs for parse_profit_center_range, expecting ValueError."""
    with pytest.raises(ValueError):
        engine.parse_profit_center_range(invalid_input)
