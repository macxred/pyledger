"""This module provides an abstract base class for testing dump
and restore operations. It defines common test case that can be inherited
and used by specific ledger implementations. The actual ledger implementation
must be provided by subclasses through the abstract ledger fixture.
"""

from io import StringIO
import pytest
import pandas as pd
from abc import ABC, abstractmethod


ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10023,      CHF,         , Test CHF Bank Account
"""

VAT_CSV = """
    id,             rate, account, inclusive, text
    Test_VAT_code,  0.02,   22000,      True, Input Tax 2%
"""

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10023,           19993,      CHF,     100.00,                     , Test_VAT_code, pytest single transaction 1,      /file1.txt
"""

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
TEST_VAT_CODE = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


class BaseTestDumpAndRestore(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_dump_and_restore(self, ledger):
        pass
