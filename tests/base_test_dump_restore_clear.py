"""Definition of abstract base class for testing dump, restore, and clear operations."""

import pytest
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal
from .constants import LEDGER_ENTRIES, ACCOUNTS, VAT_CODES


TEST_LEDGER = LEDGER_ENTRIES.head(10)


class BaseTestDumpRestoreClear(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_restore(self, ledger):
        ledger.clear()
        ledger.restore(
            ledger=TEST_LEDGER, vat_codes=VAT_CODES, accounts=ACCOUNTS, base_currency="USD"
        )
        assert ledger.base_currency == "USD", "Base currency were not restored"
        assert_frame_equal(
            ledger.standardize_vat_codes(VAT_CODES), ledger.vat_codes(), ignore_index=True
        )
        assert_frame_equal(
            ledger.standardize_account_chart(ACCOUNTS), ledger.account_chart(), ignore_index=True
        )
        target = ledger.txn_to_str(TEST_LEDGER).values()
        actual = ledger.txn_to_str(ledger.ledger()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual ledger differ"

    def test_dump_and_restore_zip(self, ledger, tmp_path):
        # Populate with test data
        ledger.base_currency = "USD"
        ledger.mirror_vat_codes(VAT_CODES)
        ledger.mirror_ledger(TEST_LEDGER)
        ledger.mirror_account_chart(ACCOUNTS)

        # Dumping current state
        vat_codes = ledger.vat_codes()
        account_chart = ledger.account_chart()
        ledger_entries = ledger.ledger()
        ledger.dump_to_zip(tmp_path / "ledger.zip")

        # Remove or alter data
        ledger.clear()
        ledger.base_currency = "EUR"

        # Restore dumped state
        ledger.restore_from_zip(tmp_path / "ledger.zip")
        assert ledger.base_currency == "USD", "Base currency were not restored"
        assert_frame_equal(vat_codes, ledger.vat_codes(), ignore_index=True)
        assert_frame_equal(account_chart, ledger.account_chart(), ignore_index=True)
        assert sorted(ledger.txn_to_str(ledger_entries).values()) == \
               sorted(ledger.txn_to_str(ledger.ledger()).values())

    def test_clear(self, ledger):
        ledger.restore(
            ledger=TEST_LEDGER, vat_codes=VAT_CODES, accounts=ACCOUNTS, base_currency="USD"
        )
        ledger.clear()
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"
        # TODO: Expand test logic to test price history, precision settings,
        # and FX adjustments when implemented
