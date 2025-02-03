"""Test suite for txn_to_str()."""

import pytest
import pandas as pd
from io import StringIO
from pyledger import MemoryLedger

JOURNAL_CSV = """
    id,   date, account, contra, currency,  amount, report_amount, tax_code, description, document
    1, 2024-05-24, 1023,   1993,      CHF,  100.00,              ,     In8%, single 1, file1.txt
    2, 2024-05-24, 1022,       ,      USD,    1.00,          0.89,         , coll 1,   dir/file.txt
    2, 2024-05-24, 1022,       ,      USD,   99.00,         87.99,         , coll 1,
    2, 2024-05-24, 1022,       ,      USD, -100.00,        -88.88,     In5%, coll 1,   dir/file.txt
    3, 2024-04-24,     ,   1021,      EUR,  200.00,              ,     In5%, coll 2A,  doc1.pdf
    3, 2024-04-24, 1021,       ,      EUR,  200.00,              ,         , coll 2B,  doc2.pdf
    4, 2024-05-25, 1022,   1992,      USD,  300.00,        450.45,     In2%, single 2, doc3.pdf
"""
EXPECTED = {
    "1": (
        "2024-05-24\n"
        "account,amount,contra,currency,description,document,profit_center,report_amount,tax_code\n"
        "1023,100.0,1993,CHF,single 1,file1.txt,,,In8%"
    ),
    "2": (
        "2024-05-24\n"
        "account,amount,contra,currency,description,document,profit_center,report_amount,tax_code\n"
        "1022,-100.0,,USD,coll 1,dir/file.txt,,-88.88,In5%\n"
        "1022,1.0,,USD,coll 1,dir/file.txt,,0.89,\n"
        "1022,99.0,,USD,coll 1,,,87.99,"
    ),
    "3": (
        "2024-04-24\n"
        "account,amount,contra,currency,description,document,profit_center,report_amount,tax_code\n"
        "1021,200.0,,EUR,coll 2B,doc2.pdf,,,\n"
        ",200.0,1021,EUR,coll 2A,doc1.pdf,,,In5%"
    ),
    "4": (
        "2024-05-25\n"
        "account,amount,contra,currency,description,document,profit_center,report_amount,tax_code\n"
        "1022,300.0,1992,USD,single 2,doc3.pdf,,450.45,In2%"
    )
}

JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)


def test_txn_to_str():
    engine = MemoryLedger()
    result = engine.txn_to_str(JOURNAL)
    assert result == EXPECTED, "Transactions were not converted correctly."


def test_txn_to_str_empty_df():
    engine = MemoryLedger()
    df = pd.DataFrame(columns=JOURNAL.columns)
    result = engine.txn_to_str(df)
    assert result == {}, "Empty DataFrame did not return an empty dict."


def test_txn_to_str_non_unique_dates():
    engine = MemoryLedger()
    df = JOURNAL[JOURNAL["id"].isin(['1', '4'])]
    df.loc[df["id"] == '4', "id"] = '1'
    with pytest.raises(ValueError):
        engine.txn_to_str(df)


def test_txn_to_str_variations_of_same_transactions():
    engine = MemoryLedger()
    df1 = JOURNAL[JOURNAL["id"] == 2]
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
    df5 = df5.rename(columns={"reporting_currency": "report_amount"})

    result1 = engine.txn_to_str(df1)
    result2 = engine.txn_to_str(df2)
    result3 = engine.txn_to_str(df3)
    result4 = engine.txn_to_str(df4)
    result5 = engine.txn_to_str(df5)
    assert result1 == result2 == result3 == result4 == result5, (
        "Same transactions should have identical string representations."
    )
