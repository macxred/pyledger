import datetime
from io import StringIO
import pandas as pd
import pytest
from pyledger import TestLedger


POSTINGS_CSV = """
    date,          account, counter_account, currency, text,                                              vat_code, amount
    2024-07-15   , 1020   , 6000           , CHF     , Entry without VAT code                           ,         , -1000
    2024-07-15   , 1020   , 6100           , CHF     , Amount exempt from VAT                           , Exempt  , -1000
    2024-07-15   , 1020   , 6200           , CHF     , Amount Including Input Tax                       , InStd   , -1000
    2024-07-15   , 6300   , 1020           , CHF     , Amount Including Input Tax (accounts inverted)   , InStd   ,  1000
    2024-07-15   , 1030   , 6400           , CHF     , Amount Excluding Output Tax                      , OutStdEx, -1000
    2024-07-15   , 6500   , 1045           , CHF     , Amount Excluding Output Tax (accounts inverted)  , OutStdEx,  1000
"""
POSTINGS = pd.read_csv(StringIO(POSTINGS_CSV), skipinitialspace=True)
POSTINGS['date'] = datetime.date.today()


def test_standardize_ledger_columns():
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
    ledger = TestLedger()

    # Add ledger entries
    for i in range(5):
        ledger.add_ledger_entry({
            "date": datetime.date.today(),
            "account": 1020,
            "counter_account": 6000,
            "currency": "CHF",
            "text": f"Entry {i+1}",
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
    index, expected_len, expected_account, expected_counter_account, expected_amount
):
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
def test_validate_account_balance(account, expected_length, expected_balance):
    ledger = TestLedger()
    postings = ledger.standardize_ledger(POSTINGS)
    postings = ledger.sanitize_ledger(postings)
    ledger.add_ledger_entry(postings)

    assert len(ledger.account_history(account)) == expected_length
    assert ledger.account_balance(account)["CHF"] == expected_balance