"""Tests for the StandaloneLedger class in pyledger."""

import datetime
from io import StringIO
import pandas as pd
from pyledger import TestLedger
import pytest

# flake8: noqa: E501

POSTINGS_CSV = """
    date,          account, counter_account, currency, text,                                              vat_code, amount
    2024-07-15   , 1020   , 6000           , CHF     , Entry without VAT code                           ,         , -1000
    2024-07-15   , 1020   , 6100           , CHF     , Amount exempt from VAT                           , Exempt  , -1000
    2024-07-15   , 1020   , 6200           , CHF     , Amount Including Input Tax                       , InStd   , -1000
    2024-07-15   , 6300   , 1020           , CHF     , Amount Including Input Tax (accounts inverted)   , InStd   ,  1000
    2024-07-15   , 1030   , 6400           , CHF     , Amount Excluding Output Tax                      , OutStdEx, -1000
    2024-07-15   , 6500   , 1045           , CHF     , Amount Excluding Output Tax (accounts inverted)  , OutStdEx,  1000
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
        "counter_account": [pd.NA, 6000, 6100, 3000],
        "currency": "CHF",
        "text": ["Collective entry", pd.NA, pd.NA, "Simple entry"],
        "amount": [1000, 800, 200, 10],
    })
    ledger.standardize_ledger_columns(postings)

    with pytest.raises(ValueError):
        # Attempt to standardize an entry without required 'account' column
        posting = pd.DataFrame({
            "date": [datetime.date.today()],
            "currency": "CHF",
            "text": ["Entry without account column"],
            "amount": [1000],
        })
        ledger.standardize_ledger_columns(posting)


def test_add_ledger_entry():
    """Test adding ledger entries."""
    ledger = TestLedger()

    # Add ledger entries
    for i in range(5):
        ledger.add_ledger_entry({
            "date": datetime.date.today(),
            "account": 1020,
            "counter_account": 6000,
            "currency": "CHF",
            "text": f"Entry {i + 1}",
            "amount": 100 * (i + 1),
        })

    with pytest.raises(ValueError):
        # Attempt to add an entry with a duplicate 'id'
        ledger.add_ledger_entry({
            "id": 1,  # Duplicate id
            "date": datetime.date.today(),
            "account": 1020,
            "counter_account": 3000,
            "currency": "CHF",
            "text": "Duplicate Entry",
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
    "index, expected_len, expected_account, expected_counter_account, expected_amount",
    [
        (0, 0, None, None, None),  # Posting without VAT code
        (1, 0, None, None, None),  # Posting exempt from VAT
        (2, 1, 6200, 1170, round(-1000 * 0.077 / (1 + 0.077), 2)),  # Including Input Tax
        (3, 1, 6300, 1170, round(-1000 * 0.077 / (1 + 0.077), 2)),  # Including Input Tax (inverted)
        (4, 1, 1030, 2200, round(-1000 * 0.077, 2)),  # Excluding Output Tax
        (5, 1, 1045, 2200, round(-1000 * 0.077, 2)),  # Excluding Output Tax (inverted)
    ],
)
def test_vat_journal_entries(
    index: int, expected_len: int, expected_account: int | None,
    expected_counter_account: int | None, expected_amount: float | None
):
    """Test VAT journal entries creation."""
    ledger = TestLedger()
    postings = ledger.standardize_ledger(POSTINGS)
    postings = ledger.sanitize_ledger(postings)

    df = ledger.vat_journal_entries(postings.iloc[index:index + 1, :])
    assert len(df) == expected_len
    if expected_len > 0:
        assert df["account"].item() == expected_account
        assert df["counter_account"].item() == expected_counter_account
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

@pytest.mark.parametrize(
    "amounts, currencies, expected",
    [
        (
            pd.Series([100.234, 200.567, 300.891]),
            pd.Series(["USD", "EUR", "GBP"]),
            pd.Series([100.23, 200.57, 300.89])
        ),
        (
            pd.Series([0.0, 0.0, 0.0]),
            pd.Series(["USD", "EUR", "GBP"]),
            pd.Series([0.0, 0.0, 0.0])
        ),
        (
            pd.Series([123456.789, 987654.321, 111111.111]),
            pd.Series(["USD", "EUR", "GBP"]),
            pd.Series([123456.79, 987654.32, 111111.11])
        ),
        (
            pd.Series([100.234, 200.567, 300.891, 400.789]),
            pd.Series(["USD", "CHF", "CAD", "HKD"]),
            pd.Series([100.23, 200.57, 300.89, 400.79])
        ),
    ]
)
def test_rounding(amounts, currencies, expected):
    ledger = TestLedger()
    result = ledger.round_series_to_precision(amounts, currencies)
    pd.testing.assert_series_equal(result, expected)

@pytest.mark.parametrize(
    "amounts, currencies, expected",
    [
        (
            pd.Series([100.234, pd.NA, 300.891]),
            pd.Series(["USD", "EUR", "GBP"]),
            pd.Series([100.23, pd.NA, 300.89])
        ),
        (
            pd.Series([pd.NA, pd.NA, pd.NA]),
            pd.Series(["USD", "EUR", "GBP"]),
            pd.Series([pd.NA, pd.NA, pd.NA])
        ),
    ]
)
def test_rounding_with_nan(amounts, currencies, expected):
    ledger = TestLedger()
    result = ledger.round_series_to_precision(amounts, currencies)
    pd.testing.assert_series_equal(result, expected)

def test_rounding_with_different_precision():
    ledger = TestLedger()
    # Assuming a different precision setting, which might be manually set for the test
    ledger._settings["precision"]["JPY"] = 1.0  # Example precision for JPY

    amounts = pd.Series([100.234, 200.567, 300.891])
    currencies = pd.Series(["USD", "JPY", "GBP"])
    expected = pd.Series([100.23, 201.0, 300.89])

    result = ledger.round_series_to_precision(amounts, currencies)
    pd.testing.assert_series_equal(result, expected)