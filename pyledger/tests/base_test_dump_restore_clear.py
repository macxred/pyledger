"""Definition of abstract base class for testing dump, restore, and clear operations."""

import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestDumpRestoreClear(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_restore(self, engine):
        engine.restore(
            configuration=self.CONFIGURATION,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            journal=self.JOURNAL_ENTRIES,
            assets=self.ASSETS,
            price_history=self.PRICES,
            revaluations=self.REVALUATIONS,
            profit_centers=self.PROFIT_CENTERS,
        )
        assert engine.reporting_currency == self.CONFIGURATION["REPORTING_CURRENCY"], (
            "Reporting currency was not restored"
        )
        assert_frame_equal(
            self.TAX_CODES, engine.tax_codes.list(), ignore_row_order=True, check_like=True
        )
        assert_frame_equal(
            self.ACCOUNTS, engine.accounts.list(), ignore_row_order=True, check_like=True
        )
        assert_frame_equal(
            self.PRICES, engine.price_history.list(), ignore_row_order=True, check_like=True
        )
        assert_frame_equal(
            self.REVALUATIONS, engine.revaluations.list(), ignore_row_order=True, check_like=True
        )
        assert_frame_equal(self.ASSETS, engine.assets.list(), ignore_row_order=True)
        assert_frame_equal(self.PROFIT_CENTERS, engine.profit_centers.list(), ignore_row_order=True)
        target = engine.txn_to_str(self.JOURNAL_ENTRIES).values()
        actual = engine.txn_to_str(engine.journal.list()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual journal differ"

    def test_dump_and_restore_zip(self, engine, tmp_path):
        # Populate with test data
        engine.reporting_currency = self.CONFIGURATION["REPORTING_CURRENCY"]
        engine.accounts.mirror(self.ACCOUNTS)
        engine.tax_codes.mirror(self.TAX_CODES)
        engine.journal.mirror(self.JOURNAL_ENTRIES)
        engine.assets.mirror(self.ASSETS)
        engine.price_history.mirror(self.PRICES)
        engine.revaluations.mirror(self.REVALUATIONS)
        engine.profit_centers.mirror(self.PROFIT_CENTERS)

        # Dumping current state
        accounts = engine.accounts.list()
        tax_codes = engine.tax_codes.list()
        journal_entries = engine.journal.list()
        assets = engine.assets.list()
        price_history = engine.price_history.list()
        revaluations = engine.revaluations.list()
        profit_centers = engine.profit_centers.list()
        engine.dump_to_zip(tmp_path / "ledger.zip")

        # Remove or alter data
        engine.clear()
        engine.reporting_currency = "EUR"

        # Restore dumped state
        engine.restore_from_zip(tmp_path / "ledger.zip")
        assert engine.reporting_currency == self.CONFIGURATION["REPORTING_CURRENCY"], (
            "Reporting currency was not restored"
        )
        assert_frame_equal(assets, engine.assets.list(), ignore_row_order=True, ignore_index=True)
        assert_frame_equal(
            price_history, engine.price_history.list(), ignore_row_order=True, ignore_index=True
        )
        assert_frame_equal(
            revaluations, engine.revaluations.list(), ignore_row_order=True, ignore_index=True
        )
        assert_frame_equal(
            tax_codes, engine.tax_codes.list(), ignore_row_order=True, ignore_index=True
        )
        assert_frame_equal(
            accounts, engine.accounts.list(), ignore_row_order=True, ignore_index=True
        )
        assert_frame_equal(
            profit_centers, engine.profit_centers.list(), ignore_row_order=True, ignore_index=True
        )
        assert sorted(engine.txn_to_str(journal_entries).values()) == \
               sorted(engine.txn_to_str(engine.journal.list()).values())

    def test_clear(self, engine):
        engine.restore(
            configuration=self.CONFIGURATION,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            journal=self.JOURNAL_ENTRIES,
            assets=self.ASSETS,
            price_history=self.PRICES,
            revaluations=self.REVALUATIONS,
            profit_centers=self.PROFIT_CENTERS,
        )
        engine.clear()
        assert engine.journal.list().empty, "Journal was not cleared"
        assert engine.tax_codes.list().empty, "Tax codes were not cleared"
        assert engine.assets.list().empty, "Assets was not cleared"
        assert engine.accounts.list().empty, "Accounts was not cleared"
        assert engine.price_history.list().empty, "Price history was not cleared"
        assert engine.revaluations.list().empty, "Revaluations was not cleared"
        assert engine.profit_centers.list().empty, "Profit centers was not cleared"
