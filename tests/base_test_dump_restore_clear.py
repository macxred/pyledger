"""Definition of abstract base class for testing dump, restore, and clear operations."""

import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


LEDGER = BaseTest.LEDGER_ENTRIES.head(7)


class BaseTestDumpRestoreClear(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_restore(self, ledger):
        ledger.restore(
            ledger=LEDGER, vat_codes=self.VAT_CODES, accounts=self.ACCOUNTS, settings=self.SETTINGS
        )
        assert ledger.base_currency == "CHF", "Base currency were not restored"
        vat_codes = ledger.standardize_vat_codes(self.VAT_CODES)
        assert_frame_equal(vat_codes, ledger.vat_codes(), check_like=True)
        accounts = ledger.standardize_account_chart(self.ACCOUNTS)
        assert_frame_equal(accounts, ledger.account_chart(), check_like=True)
        target = ledger.txn_to_str(LEDGER).values()
        actual = ledger.txn_to_str(ledger.ledger()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual ledger differ"

    def test_dump_and_restore_zip(self, ledger, tmp_path):
        # Populate with test data
        ledger.base_currency = "CHF"
        ledger.mirror_account_chart(self.ACCOUNTS)
        ledger.mirror_vat_codes(self.VAT_CODES)
        ledger.mirror_ledger(LEDGER)

        # Dumping current state
        account_chart = ledger.account_chart()
        vat_codes = ledger.vat_codes()
        ledger_entries = ledger.ledger()
        ledger.dump_to_zip(tmp_path / "ledger.zip")

        # Remove or alter data
        ledger.clear()
        ledger.base_currency = "EUR"

        # Restore dumped state
        ledger.restore_from_zip(tmp_path / "ledger.zip")
        assert ledger.base_currency == "CHF", "Base currency were not restored"
        assert_frame_equal(vat_codes, ledger.vat_codes(), ignore_index=True)
        assert_frame_equal(account_chart, ledger.account_chart(), ignore_index=True)
        assert sorted(ledger.txn_to_str(ledger_entries).values()) == \
               sorted(ledger.txn_to_str(ledger.ledger()).values())

    def test_clear(self, ledger):
        ledger.restore(
            ledger=LEDGER, vat_codes=self.VAT_CODES, accounts=self.ACCOUNTS, settings=self.SETTINGS
        )
        ledger.clear()
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"
        # TODO: Expand test logic to test price history, precision settings,
        # and FX adjustments when implemented
