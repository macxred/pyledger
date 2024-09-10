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
    4,  2024-05-24, 10022,           19992,      USD,     300.00,               450.45, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
"""
# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()

    def test_txn_to_str(self):
        ledger = MemoryLedger()
        df = LEDGER_ENTRIES[LEDGER_ENTRIES["id"].isin([1, 2])]
        result = ledger.txn_to_str(df)
        # flake8: noqa: E501
        expected = [
            (
                "2024-05-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
                "10022.0,-100.0,-88.88,,USD,/subdir/file2.txt,pytest collective txn 1 - line 1,Test_VAT_code\n"
                "10022.0,1.0,0.89,,USD,/subdir/file2.txt,pytest collective txn 1 - line 1,Test_VAT_code\n"
                "10022.0,99.0,87.99,,USD,,pytest collective txn 1 - line 1,Test_VAT_code"
            ),
            (
                "2024-05-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
                "10023.0,100.0,,19993.0,CHF,/file1.txt,pytest single transaction 1,Test_VAT_code"
            )
        ]
        # flake8: enable
        assert result == expected, "Transactions were not converted correctly."

    def test_txn_to_str_sorted_transactions(self):
            ledger = MemoryLedger()
            df = LEDGER_ENTRIES
            result = ledger.txn_to_str(df)
            # flake8: noqa: E501
            expected_first_txn = (
                "2024-04-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
                "10021.0,200.0,175.55,,EUR,/document-col-alt.pdf,pytest collective txn 2 - line 2,Test_VAT_code\n"
                ",200.0,175.55,10021.0,EUR,/document-col-alt.pdf,pytest collective txn 2 - line 1,Test_VAT_code"
            )
            expected_last_txn = (
                "2024-05-24,account,amount,base_currency_amount,counter_account,currency,document,text,vat_code\n"
                "10023.0,100.0,,19993.0,CHF,/file1.txt,pytest single transaction 1,Test_VAT_code"
            )
            # flake8: enable
            assert result[0] == expected_first_txn, "First transaction was not sorted correctly."
            assert result[-1] == expected_last_txn, "Last transaction was not sorted correctly."

    def test_txn_to_str_empty_df(self):
        ledger = MemoryLedger()
        df = pd.DataFrame(columns=LEDGER_ENTRIES.columns)
        result = ledger.txn_to_str(df)
        expected = []
        assert result == expected, "Empty DataFrame did not return an empty list."

    def test_txn_to_str_different_representations(self):
        ledger = MemoryLedger()
        df1 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 1]
        df2 = LEDGER_ENTRIES[LEDGER_ENTRIES["id"] == 4]
        result1 = ledger.txn_to_str(df1)
        result2 = ledger.txn_to_str(df2)
        assert result1 != result2, "Different transactions should have different string representations."
