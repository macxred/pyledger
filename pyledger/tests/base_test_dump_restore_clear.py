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
            assets=self.ASSETS,
        )
        assert ledger.reporting_currency == self.SETTINGS["REPORTING_CURRENCY"], (
            "Reporting currency was not restored"
        )
        tax_codes = ledger.standardize_tax_codes(self.TAX_CODES)
        assert_frame_equal(
            tax_codes, ledger.tax_codes.list(), ignore_row_order=True, check_like=True
        )
        accounts = ledger.standardize_accounts(self.ACCOUNTS)
        assert_frame_equal(
            accounts, ledger.accounts.list(), ignore_row_order=True, check_like=True
        )
        assets = ledger.standardize_assets(self.ASSETS)
        assert_frame_equal(
            assets, ledger.assets.list(), ignore_row_order=True
        )
        target = ledger.txn_to_str(self.LEDGER_ENTRIES).values()
        actual = ledger.txn_to_str(ledger.ledger.list()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual ledger differ"

    def test_dump_and_restore_zip(self, ledger, tmp_path):
        # Populate with test data
        ledger.reporting_currency = self.SETTINGS["REPORTING_CURRENCY"]
        ledger.accounts.mirror(self.ACCOUNTS)
        ledger.tax_codes.mirror(self.TAX_CODES)
        ledger.ledger.mirror(self.LEDGER_ENTRIES)
        ledger.assets.mirror(self.ASSETS)

        # Dumping current state
        accounts = ledger.accounts.list()
        tax_codes = ledger.tax_codes.list()
        ledger_entries = ledger.ledger.list()
        assets = ledger.assets.list()
        ledger.dump_to_zip(tmp_path / "ledger.zip")

        # Remove or alter data
        ledger.clear()
        ledger.reporting_currency = "EUR"

        # Restore dumped state
        ledger.restore_from_zip(tmp_path / "ledger.zip")
        assert ledger.reporting_currency == self.SETTINGS["REPORTING_CURRENCY"], (
            "Reporting currency was not restored"
        )
        assert_frame_equal(assets, ledger.assets.list(), ignore_row_order=True, ignore_index=True)
        assert_frame_equal(
            tax_codes, ledger.tax_codes.list(), ignore_row_order=True, ignore_index=True
        )
        assert_frame_equal(
            accounts, ledger.accounts.list(), ignore_row_order=True, ignore_index=True
        )
        assert sorted(ledger.txn_to_str(ledger_entries).values()) == \
               sorted(ledger.txn_to_str(ledger.ledger.list()).values())

    def test_clear(self, ledger):
        ledger.restore(
            settings=self.SETTINGS,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES,
            assets=self.ASSETS
        )
        ledger.clear()
        assert ledger.ledger.list().empty, "Ledger was not cleared"
        assert ledger.tax_codes.list().empty, "Tax codes were not cleared"
        assert ledger.assets.list().empty, "Assets was not cleared"
        assert ledger.accounts.list().empty, "Accounts was not cleared"
        # TODO: Expand test logic to test price history and revaluations when implemented
