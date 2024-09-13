"""Test suite for txn_to_str()."""

import pytest
import pandas as pd
from io import StringIO
from pyledger import MemoryLedger

LEDGER_CSV = """
    id,   date, account, counter, currency,  amount, base_amount,  vat, text,          document
    1, 2024-05-24, 1023,    1993,      CHF,  100.00,            , In8%, single txn 1,  file1.txt
    2, 2024-05-24, 1022,        ,      USD,    1.00,        0.89,     , collective 1,  dir/file.txt
    2, 2024-05-24, 1022,        ,      USD,   99.00,       87.99,     , collective 1,
    2, 2024-05-24, 1022,        ,      USD, -100.00,      -88.88, In5%, collective 1,  dir/file.txt
    3, 2024-04-24,     ,    1021,      EUR,  200.00,            , In5%, collective 2A, doc1.pdf
    3, 2024-04-24, 1021,        ,      EUR,  200.00,            ,     , collective 2B, doc2.pdf
    4, 2024-05-25, 1022,    1992,      USD,  300.00,      450.45, In2%, single txn 2,  doc3.pdf
"""
EXPECTED = {
    '1': (
        "2024-05-24\n"
        "account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "1023,100.0,,1993,CHF,file1.txt,single txn 1,In8%"
    ),
    '2': (
        "2024-05-24\n"
        "account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "1022,-100.0,-88.88,,USD,dir/file.txt,collective 1,In5%\n"
        "1022,1.0,0.89,,USD,dir/file.txt,collective 1,\n"
        "1022,99.0,87.99,,USD,,collective 1,"
    ),
    '3': (
        "2024-04-24\n"
        "account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "1021,200.0,,,EUR,doc2.pdf,collective 2B,\n"
        ",200.0,,1021,EUR,doc1.pdf,collective 2A,In5%"
    ),
    '4': (
        "2024-05-25\n"
        "account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
        "1022,300.0,450.45,1992,USD,doc3.pdf,single txn 2,In2%"
    )
}

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


def test_txn_to_str_non_unique_dates():
    ledger = MemoryLedger()
    df = LEDGER_ENTRIES[LEDGER_ENTRIES["id"].isin([1, 4])]
    df.loc[df["id"] == 4, "id"] = 1
    with pytest.raises(ValueError):
        ledger.txn_to_str(df)


def test_txn_to_str_variations_of_same_transactions():
    ledger = MemoryLedger()
    df1 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 2]
    # Reverse the column order
    df2 = df1[df1.columns[::-1]]
    # Shuffle rows
    df3 = df1.sample(frac=1).reset_index(drop=True)
    # Change dtypes
    df4 = df1.copy()
    df4["amount"] = df4["amount"].astype(str)
    df4["account"] = df4["account"].astype(float)
    # Replace column short column name by full name
    df5 = df1.copy()
    df5 = df5.rename(columns={"base_currency": "base_currency_amount"})

    result1 = ledger.txn_to_str(df1)
    result2 = ledger.txn_to_str(df2)
    result3 = ledger.txn_to_str(df3)
    result4 = ledger.txn_to_str(df4)
    result5 = ledger.txn_to_str(df5)
    assert result1 == result2 == result3 == result4 == result5, (
        "Same transactions should have identical string representations."
    )
