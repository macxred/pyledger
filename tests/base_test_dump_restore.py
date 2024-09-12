"""This module provides an abstract base class to test dump and restore operations."""

from io import StringIO
import pytest
import pandas as pd
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal


ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10023,      CHF,         , Test CHF Bank Account
"""

VAT_CSV = """
    id,             rate, account, inclusive, text
    Test_VAT_code,  0.02,   22000,      True, Input Tax 2%
"""

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, text,             document
    1,  2024-05-24,  1000,            1999,      CHF,     100.00, single entry,     file_1.txt
    42, 2024-06-21,  1020,                ,      USD,     100.00, collective entry, file_2.txt
    42, 2024-06-21,      ,            4000,      USD,      50.00, collective entry, file_2.txt
    42, 2024-06-21,      ,            5000,      USD,      50.00, collective entry, file_2.txt
"""


LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
VAT_CODES = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


class BaseTestDumpAndRestore(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    @pytest.fixture()
    def restore_initial_state(self, ledger):
        # Fetch original state
        initial_ledger = ledger.ledger()
        initial_vat_codes = ledger.vat_codes()
        initial_account_chart = ledger.account_chart()

        yield

        # Restore initial state
        ledger.mirror_ledger(initial_ledger, delete=True)
        ledger.mirror_vat_codes(initial_vat_codes, delete=True)
        ledger.mirror_account_chart(initial_account_chart, delete=True)

    def test_restore(self, ledger, restore_initial_state):
        ledger.clear()
        ledger.restore(
            ledger=LEDGER_ENTRIES, vat_codes=VAT_CODES, accounts=ACCOUNTS, base_currency="USD"
        )
        assert ledger.base_currency == "USD", "Base currency were not restored"
        assert_frame_equal(
            ledger.standardize_vat_codes(VAT_CODES), ledger.vat_codes(), ignore_index=True
        )
        assert_frame_equal(
            ledger.standardize_account_chart(ACCOUNTS), ledger.account_chart(), ignore_index=True
        )
        expected_ledger = sorted(
            ledger.txn_to_str(ledger.standardize_ledger(LEDGER_ENTRIES)).values()
        )
        ledger = sorted(ledger.txn_to_str(ledger.ledger()).values())
        assert expected_ledger == ledger

    def test_dump_and_restore_zip(self, ledger, tmp_path, restore_initial_state):
        # Populate with test data
        ledger.base_currency = "USD"
        ledger.mirror_vat_codes(VAT_CODES)
        ledger.mirror_ledger(LEDGER_ENTRIES)
        ledger.mirror_account_chart(ACCOUNTS)

        # Dumping current state
        vat_codes = ledger.vat_codes()
        account_chart = ledger.account_chart()
        ledger_entries = ledger.ledger()
        ledger.dump_to_zip(tmp_path / "ledger.zip")
        ledger.clear()

        # Restoring dumped state
        ledger.restore_from_zip(tmp_path / "ledger.zip")
        assert ledger.base_currency == "USD", "Base currency were not restored"
        assert_frame_equal(vat_codes, ledger.vat_codes(), ignore_index=True)
        assert_frame_equal(account_chart, ledger.account_chart(), ignore_index=True)
        assert sorted(ledger.txn_to_str(ledger_entries).values()) == \
               sorted(ledger.txn_to_str(ledger.ledger()).values())
