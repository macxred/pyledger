"""Test suite for parse_profit_centers() method."""

from io import StringIO
import pandas as pd
import pytest
from pyledger import MemoryLedger


@pytest.fixture
def engine():
    PROFIT_CENTERS_CSV = """
        profit_center,
            Shop,
            General,
    """
    profit_centers = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)
    engine = MemoryLedger()
    engine.restore(profit_centers=profit_centers)
    return engine


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        ("Shop", {"Shop"}),
        (["Shop", "General"], {"Shop", "General"}),
        ("Shop+General", {"Shop", "General"}),
        ("Shop+General+Bakery", {"Shop", "General"}),  # 'Bakery' discarded
    ]
)
def test_parse_profit_centers_valid_inputs(input_value, expected_output, engine):
    assert engine.parse_profit_centers(input_value) == expected_output


@pytest.mark.parametrize(
    "invalid_input",
    [
        {"add": ["Shop", 123]},  # dicts no longer allowed
        {"subtract": ["General"]},
        ["Shop", 123],           # Non-str in list
        123,
        3.14,
        None,
        "",                      # Empty string
        [],                      # Empty list
    ]
)
def test_parse_profit_centers_invalid_inputs(invalid_input, engine):
    with pytest.raises(ValueError):
        engine.parse_profit_centers(invalid_input)


def test_parse_profit_centers_discarded_warns(engine, caplog):
    input_value = "Shop+Unknown1+Unknown2"
    expected = {"Shop"}
    output = engine.parse_profit_centers(input_value)
    assert output == expected

    log_messages = caplog.text.strip().split("\n")
    assert len(log_messages) == 1
