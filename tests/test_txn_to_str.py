"""This module contains the test suite for Ledger entries operations using the MemoryLedger
implementation. It inherits the common Ledger code tests from the BaseTestLedger abstract base
class.
"""

import pytest
import pandas as pd
from io import StringIO
from tests.base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger

# flake8: noqa: E501
LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10023,           19993,      CHF,     100.00,                     , Test_VAT_code, pytest single transaction 1,      /file1.txt
    2,  2024-05-24, 10022,                ,      USD,    -100.00,               -88.88, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,       1.00,                 0.89, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,      99.00,                87.99, Test_VAT_code, pytest collective txn 1 - line 1,
    3,  2024-04-24,      ,           10021,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 1, /document-col-alt.pdf
    3,  2024-04-24, 10021,                ,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 2, /document-col-alt.pdf
    4,  2024-05-25, 10022,           19992,      USD,     300.00,               450.45, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
"""
EXPECTED = {
    '1': (
        "2024-05-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "10023,100.0,,19993,CHF,/file1.txt,pytest single transaction 1,Test_VAT_code"
    ),
    '2': (
        "2024-05-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "10022,-100.0,-88.88,,USD,/subdir/file2.txt,pytest collective txn 1 - line 1,Test_VAT_code\n"
        "10022,1.0,0.89,,USD,/subdir/file2.txt,pytest collective txn 1 - line 1,Test_VAT_code\n"
        "10022,99.0,87.99,,USD,,pytest collective txn 1 - line 1,Test_VAT_code"
    ),
    '3': (
        "2024-04-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "10021,200.0,175.55,,EUR,/document-col-alt.pdf,pytest collective txn 2 - line 2,Test_VAT_code\n"
        ",200.0,175.55,10021,EUR,/document-col-alt.pdf,pytest collective txn 2 - line 1,Test_VAT_code"
    ),
    '4': (
        "2024-05-25,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "10022,300.0,450.45,19992,USD,/document-alt.pdf,pytest single transaction 2,Test_VAT_code"
    )
}
# flake8: enable

LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)


def test_txn_to_str():
    ledger = MemoryLedger()
    result = ledger.txn_to_str(LEDGER_ENTRIES)
    assert result == EXPECTED, "Transactions were not converted correctly."


def test_txn_to_str_empty_df():
    ledger = MemoryLedger()
    df = pd.DataFrame(columns=LEDGER_ENTRIES.columns)
    result = ledger.txn_to_str(df)
    assert result == {}, "Empty DataFrame did not return an empty dict."


def test_txn_to_str_different_representations():
    ledger = MemoryLedger()
    df1 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 1]
    df2 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 4]
    result1 = ledger.txn_to_str(df1)
    result2 = ledger.txn_to_str(df2)
    assert result1 != result2, (
        "Different transactions should have different string representations."
    )


def test_txn_to_str_non_unique_dates():
    ledger = MemoryLedger()
    df = LEDGER_ENTRIES[LEDGER_ENTRIES["id"].isin([1, 4])]
    df.loc[df["id"] == 4, "id"] = 1
    with pytest.raises(ValueError):
        ledger.txn_to_str(df)


def test_txn_to_str_same_transactions_different_order_dtypes():
    ledger = MemoryLedger()
    df1 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 1]
    # Reverse the column order
    df2 = df1[df1.columns[::-1]]
    # DataFrame with shuffled rows
    df3 = df1.sample(frac=1).reset_index(drop=True)  # Shuffle the rows
    # DataFrame with different dtypes (e.g., convert 'amount' to string, 'account' to float)
    df4 = df1.copy()
    df4["amount"] = df4["amount"].astype(str)
    df4["account"] = df4["account"].astype(float)

    result1 = ledger.txn_to_str(df1)
    result2 = ledger.txn_to_str(df2)
    result3 = ledger.txn_to_str(df3)
    result4 = ledger.txn_to_str(df4)
    assert result1 == result2 == result3 == result4, (
        "Same transactions should have identical string representations."
    )
