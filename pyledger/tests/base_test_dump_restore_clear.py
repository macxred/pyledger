"""Definition of abstract base class for testing dump, restore, and clear operations."""

import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestDumpRestoreClear(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_restore(self, ledger):
        ledger.restore(
            settings=self.SETTINGS,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES,
        )
        assert ledger.reporting_currency == "CHF", "Base currency were not restored"
        tax_codes = ledger.standardize_tax_codes(self.TAX_CODES)
        assert_frame_equal(tax_codes, ledger.tax_codes(), ignore_row_order=True, check_like=True)
        accounts = ledger.standardize_accounts(self.ACCOUNTS)
        assert_frame_equal(
            accounts, ledger.accounts(), ignore_row_order=True, check_like=True
        )
        target = ledger.txn_to_str(self.LEDGER_ENTRIES).values()
        actual = ledger.txn_to_str(ledger.ledger()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual ledger differ"

    def test_dump_and_restore_zip(self, ledger, tmp_path):
        # Populate with test data
        ledger.reporting_currency = "CHF"
        ledger.mirror_accounts(self.ACCOUNTS)
        ledger.mirror_tax_codes(self.TAX_CODES)
        ledger.mirror_ledger(self.LEDGER_ENTRIES)

        # Dumping current state
        accounts = ledger.accounts()
        tax_codes = ledger.tax_codes()
        ledger_entries = ledger.ledger()
        ledger.dump_to_zip(tmp_path / "ledger.zip")

        # Remove or alter data
        ledger.clear()
        ledger.reporting_currency = "EUR"

        # Restore dumped state
        ledger.restore_from_zip(tmp_path / "ledger.zip")
        assert ledger.reporting_currency == "CHF", "Base currency were not restored"
        assert_frame_equal(tax_codes, ledger.tax_codes(), ignore_row_order=True, ignore_index=True)
        assert_frame_equal(
            accounts, ledger.accounts(), ignore_row_order=True, ignore_index=True
        )
        assert sorted(ledger.txn_to_str(ledger_entries).values()) == \
               sorted(ledger.txn_to_str(ledger.ledger()).values())

    def test_clear(self, ledger):
        ledger.restore(
            settings=self.SETTINGS,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES,
        )
        ledger.clear()
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.tax_codes().empty, "TAX codes were not cleared"
        assert ledger.accounts().empty, "Accounts was not cleared"
        # TODO: Expand test logic to test price history, precision settings,
        # and revaluations when implemented
