"""Tests for the StandaloneLedger class in pyledger."""

import datetime
from io import StringIO
import pandas as pd
from pyledger import TestLedger
import pytest

# flake8: noqa: E501

POSTINGS_CSV = """
    date,          account, contra, currency, description,                                         tax_code, amount
    2024-07-15,    1020,    6000,   CHF,      Entry without tax code,                                      , -1000
    2024-07-15,    1020,    6100,   CHF,      Amount exempt from TAX,                              Exempt,   -1000
    2024-07-15,    1020,    6200,   CHF,      Amount Including Input Tax,                          InStd,    -1000
    2024-07-15,    6300,    1020,   CHF,      Amount Including Input Tax (accounts inverted),      InStd,     1000
    2024-07-15,    1030,    6400,   CHF,      Amount Excluding Output Tax,                         OutStdEx, -1000
    2024-07-15,    6500,    1045,   CHF,      Amount Excluding Output Tax (accounts inverted),     OutStdEx,  1000
"""

# flake8: enable

POSTINGS = pd.read_csv(StringIO(POSTINGS_CSV), skipinitialspace=True)
POSTINGS["date"] = datetime.date.today()


def test_standardize_ledger_columns():
    """Test the standardization of ledger columns."""
    ledger = TestLedger()
    postings = pd.DataFrame({
        "date": [datetime.date.today(), pd.NA, pd.NA, "2024-01-01"],
        "account": [1020, pd.NA, pd.NA, 1020],
        "contra": [pd.NA, 6000, 6100, 3000],
        "currency": "CHF",
        "description": ["Collective entry", pd.NA, pd.NA, "Simple entry"],
        "amount": [1000, 800, 200, 10],
    })
    ledger.standardize_ledger_columns(postings)

    with pytest.raises(ValueError):
        # Attempt to standardize an entry without required 'account' column
        posting = pd.DataFrame({
            "date": [datetime.date.today()],
            "currency": "CHF",
            "description": ["Entry without account column"],
            "amount": [1000],
        })
        ledger.standardize_ledger_columns(posting)

def test_standardize_ledger_columns_ensure_date_type():
    ledger = TestLedger()
    df = ledger.standardize_ledger_columns(None)

    # <M8[ns] is a correct format for pandas
    assert df["date"].dtype == "<M8[ns]"

def test_add_ledger_entry():
    """Test adding ledger entries."""
    ledger = TestLedger()

    # Add ledger entries
    for i in range(5):
        ledger.add_ledger_entry({
            "date": datetime.date.today(),
            "account": 1020,
            "contra": 6000,
            "currency": "CHF",
            "description": f"Entry {i + 1}",
            "amount": 100 * (i + 1),
        })

    with pytest.raises(ValueError):
        # Attempt to add an entry with a duplicate 'id'
        ledger.add_ledger_entry({
            "id": '1',  # Duplicate id
            "date": datetime.date.today(),
            "account": 1020,
            "contra": 3000,
            "currency": "CHF",
            "description": "Duplicate Entry",
            "amount": 100,
        })

    # Retrieve the original ledger
    original_ledger = ledger.ledger()
    assert isinstance(original_ledger, pd.DataFrame)
    assert len(original_ledger) == 5
    assert not original_ledger["id"].duplicated().any()

    # Retrieve serialized ledger
    serialized_ledger = ledger.serialized_ledger()
    assert isinstance(serialized_ledger, pd.DataFrame)
    assert len(serialized_ledger) == 10


@pytest.mark.parametrize(
    "index, expected_len, expected_account, expected_contra, expected_amount",
    [
        (0, 0, None, None, None),  # Posting without tax code
        (1, 0, None, None, None),  # Posting exempt from TAX
        (2, 1, 6200, 1170, round(-1000 * 0.077 / (1 + 0.077), 2)),  # Including Input Tax
        (3, 1, 6300, 1170, round(-1000 * 0.077 / (1 + 0.077), 2)),  # Including Input Tax (inverted)
        (4, 1, 1030, 2200, round(-1000 * 0.077, 2)),  # Excluding Output Tax
        (5, 1, 1045, 2200, round(-1000 * 0.077, 2)),  # Excluding Output Tax (inverted)
    ],
)
def test_tax_journal_entries(
    index: int, expected_len: int, expected_account: int | None,
    expected_contra: int | None, expected_amount: float | None
):
    """Test TAX journal entries creation."""
    ledger = TestLedger()
    postings = ledger.standardize_ledger(POSTINGS)
    postings = ledger.sanitize_ledger(postings)

    df = ledger.tax_journal_entries(postings.iloc[index:index + 1, :])
    assert len(df) == expected_len
    if expected_len > 0:
        assert df["account"].item() == expected_account
        assert df["contra"].item() == expected_contra
        assert df["amount"].item() == expected_amount


@pytest.mark.parametrize(
    "account, expected_length, expected_balance",
    [
        (6000, 1, 1000.0),
        (6100, 1, 1000.0),
        (6200, 2, round(1000 / (1 + 0.077), 2)),
        (6300, 2, round(1000 / (1 + 0.077), 2)),
        (6400, 1, 1000.0),
        (6500, 1, 1000.0),
        (1020, 4, -4000.0),
        (1030, 2, -1 * round(1000 * (1 + 0.077), 2)),
        (1045, 2, -1 * round(1000 * (1 + 0.077), 2)),
    ],
)
def test_validate_account_balance(account: int, expected_length: int, expected_balance: float):
    """Test the validation of account balances."""
    ledger = TestLedger()
    postings = ledger.standardize_ledger(POSTINGS)
    postings = ledger.sanitize_ledger(postings)
    ledger.add_ledger_entry(postings)

    assert len(ledger.account_history(account)) == expected_length
    assert ledger.account_balance(account)["CHF"] == expected_balance


@pytest.fixture
def ledger():
    PRECISION = {
        "CHF": 0.01,
        "USD": 0.01,
        "EUR": 0.01,
        "GBP": 0.01,
        "JPY": 1.0,
    }

    ledger = TestLedger()
    ledger._settings["precision"] = PRECISION
    return ledger


def test_rounding(ledger):
    result = ledger.round_to_precision([100.234, 300.891], ["USD", "GBP"])
    assert result == [100.23, 300.89], "Rounding failed."


def test_rounding_with_nan(ledger):
    result = ledger.round_to_precision([100.234, None, 300.891], ["USD", "EUR", "GBP"])
    assert result == [100.23, None, 300.89], "Rounding with None failed."


def test_rounding_with_different_precision(ledger):
    result = ledger.round_to_precision([100.234, 200.567, 300.891], ["USD", "JPY", "GBP"])
    assert result == [100.23, 201.0, 300.89], "Rounding with mixed precision failed."


def test_arguments_of_differing_length_raises_error(ledger):
    with pytest.raises(ValueError, match="Amount and ticker lists must be of the same length"):
        ledger.round_to_precision([100.234, 200.567], ["USD"])


def test_rounding_with_scalar_amount_and_ticker(ledger):
    result = ledger.round_to_precision(100.234, "USD")
    assert result == 100.23, "Rounding scalar amount failed."


def test_rounding_with_list_amount_and_scalar_ticker(ledger):
    result = ledger.round_to_precision([100.234, 200.567, 300.891], "USD")
    assert result == [100.23, 200.57, 300.89], "Rounding scalar ticker failed."


def test_rounding_with_empty_ticker(ledger):
    with pytest.raises(KeyError):
        ledger.round_to_precision([100.234], [""])


def test_rounding_with_unknown_ticker(ledger):
    with pytest.raises(KeyError):
        ledger.round_to_precision([100.234], ["XYZ"])
