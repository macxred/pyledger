"""This module provides an abstract base class for testing dump
and restore operations. It defines common test case that can be inherited
and used by specific ledger implementations. The actual ledger implementation
must be provided by subclasses through the abstract ledger fixture.
"""

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

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10023,           19993,      CHF,     100.00,                     , Test_VAT_code, pytest single transaction 1,      /file1.txt
"""

# flake8: enable


STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
TEST_VAT_CODE = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


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
        initial_vat_codes = ledger.vat_codes()
        initial_account_chart = ledger.account_chart()
        initial_ledger = ledger.ledger()

        yield

        # Restore initial state
        ledger.mirror_ledger(initial_ledger, delete=True)
        ledger.mirror_vat_codes(initial_vat_codes, delete=True)
        ledger.mirror_account_chart(initial_account_chart, delete=True)

    def test_dump_and_restore(self, ledger, restore_initial_state):
        # This test logic can be simplified:
        #
        # 1. Mirroring the desired state
        # 2. Mirror empty DFs
        # 3. Check is state now empty
        # 4. Restore dumped data
        # 5. Check is restored state matches desired
        # 6. Restore the initial state with a fixture
        #
        # The only disadvantage is that a lot of data should be processed (even default accounts)
        #
        # Also we can add delete_zip: bool = False to the restore() method

        # saving initial state
        initial_vat_codes = ledger.vat_codes()
        initial_account_chart = ledger.account_chart()
        initial_ledger = ledger.ledger()

        # mirroring desired state
        ledger.mirror_vat_codes(TEST_VAT_CODE)
        ledger.mirror_account_chart(TEST_ACCOUNTS)
        ledger.mirror_ledger(LEDGER_ENTRIES)
        vat_codes = ledger.vat_codes()
        account_chart = ledger.account_chart()
        ledger_entries = ledger.ledger()
        ledger.dump("ledger.zip")

        # restoring initial state
        ledger.mirror_ledger(initial_ledger, delete=True)
        ledger.mirror_vat_codes(initial_vat_codes, delete=True)
        ledger.mirror_account_chart(initial_account_chart, delete=True)
        assert txn_to_str(initial_ledger) == txn_to_str(ledger.ledger())
        assert_frame_equal(
            initial_vat_codes, ledger.vat_codes(), ignore_index=True,
        )
        assert_frame_equal(
            initial_account_chart, ledger.account_chart(), ignore_index=True,
        )

        # restoring desired state
        ledger.restore("ledger.zip")
        assert txn_to_str(ledger_entries) == txn_to_str(ledger.ledger())
        assert_frame_equal(
            vat_codes, ledger.vat_codes(), ignore_index=True,
        )
        assert_frame_equal(
            account_chart, ledger.account_chart(), ignore_index=True,
        )
