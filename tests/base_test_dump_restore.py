"""This module provides an abstract base class to test dump and restore operations."""

from io import StringIO
from typing import List
import pytest
import pandas as pd
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal, df_to_consistent_str, nest


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


# TODO: Use txn_to_str form the consistent_df package when it implemented
def txn_to_str(df: pd.DataFrame) -> List[str]:
    df = nest(df, columns=[col for col in df.columns if col not in ["id", "date"]], key="txn")
    df = df.drop(columns=["id"])
    result = [
        f"{str(date)},{df_to_consistent_str(txn)}" for date, txn in zip(df["date"], df["txn"])
    ]
    result.sort()
    return result


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

    def clear_ledger(self, ledger):
        """Clear all data from the ledger system.

        This method removes all entries from the ledger, VAT codes, and account chart,
        restoring the system to a pristine state. It is designed to be flexible and can be
        overridden by specific ledger implementations to adapt to the clearing process
        requirements of the integrating system.

        Args:
            ledger: The ledger system instance to be cleared.
        """
        ledger.mirror_ledger(None, delete=True)
        ledger.mirror_vat_codes(None, delete=True)
        ledger.mirror_account_chart(None, delete=True)

    def test_restore(self, ledger, restore_initial_state):
        # Clearing system to a pristine state to ensure restoring works
        self.clear_ledger(ledger)
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"

        # Restoring state from CSV files
        ledger.restore(ledger=LEDGER_ENTRIES, vat_codes=VAT_CODES, accounts=ACCOUNTS)
        assert ledger.base_currency == "USD", "Base currency were not restored"
        assert_frame_equal(
            ledger.standardize_vat_codes(VAT_CODES), ledger.vat_codes(), ignore_index=True
        )
        assert_frame_equal(
            ledger.standardize_account_chart(ACCOUNTS), ledger.account_chart(), ignore_index=True
        )
        assert txn_to_str(ledger.standardize_ledger(LEDGER_ENTRIES)) == txn_to_str(ledger.ledger())

    def test_dump_and_restore_zip(self, ledger, restore_initial_state):
        # Populate with test data
        ledger.mirror_vat_codes(VAT_CODES)
        ledger.mirror_account_chart(ACCOUNTS)
        ledger.mirror_ledger(LEDGER_ENTRIES)

        # Dumping current state
        vat_codes = ledger.vat_codes()
        account_chart = ledger.account_chart()
        ledger_entries = ledger.ledger()
        ledger.dump_to_zip("ledger.zip")

        # Clearing system to a pristine state to ensure restoring works
        self.clear_ledger(ledger)
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"

        # Restoring dumped state
        ledger.restore_from_zip("ledger.zip")
        assert txn_to_str(ledger_entries) == txn_to_str(ledger.ledger())
        assert_frame_equal(vat_codes, ledger.vat_codes(), ignore_index=True)
        assert_frame_equal(account_chart, ledger.account_chart(), ignore_index=True)
