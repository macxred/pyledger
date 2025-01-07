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
        # TODO: Remove once the test data in self.LEDGER_ENTRIES is extended with profit centres
        LEDGER_ENTRIES = self.LEDGER_ENTRIES.copy()
        default_profit_center = self.PROFIT_CENTERS.iloc[0]["profit_center"]
        LEDGER_ENTRIES["profit_center"] = \
            LEDGER_ENTRIES["profit_center"].fillna(default_profit_center)
        engine.restore(
            settings=self.SETTINGS,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            ledger=LEDGER_ENTRIES,
            assets=self.ASSETS,
            price_history=self.PRICES,
            revaluations=self.REVALUATIONS,
            profit_centers=self.PROFIT_CENTERS,
        )
        assert engine.reporting_currency == self.SETTINGS["REPORTING_CURRENCY"], (
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
        target = engine.txn_to_str(LEDGER_ENTRIES).values()
        actual = engine.txn_to_str(engine.ledger.list()).values()
        assert sorted(target) == sorted(actual), "Targeted and actual ledger differ"

    def test_dump_and_restore_zip(self, engine, tmp_path):
        # Populate with test data
        # TODO: Remove once the test data in self.LEDGER_ENTRIES is extended with profit centres
        LEDGER_ENTRIES = self.LEDGER_ENTRIES.copy()
        default_profit_center = self.PROFIT_CENTERS.iloc[0]["profit_center"]
        LEDGER_ENTRIES["profit_center"] = \
            LEDGER_ENTRIES["profit_center"].fillna(default_profit_center)
        engine.reporting_currency = self.SETTINGS["REPORTING_CURRENCY"]
        engine.accounts.mirror(self.ACCOUNTS)
        engine.tax_codes.mirror(self.TAX_CODES)
        engine.ledger.mirror(LEDGER_ENTRIES)
        engine.assets.mirror(self.ASSETS)
        engine.price_history.mirror(self.PRICES)
        engine.revaluations.mirror(self.REVALUATIONS)
        engine.profit_centers.mirror(self.PROFIT_CENTERS)

        # Dumping current state
        accounts = engine.accounts.list()
        tax_codes = engine.tax_codes.list()
        ledger_entries = engine.ledger.list()
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
        assert engine.reporting_currency == self.SETTINGS["REPORTING_CURRENCY"], (
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
        assert sorted(engine.txn_to_str(ledger_entries).values()) == \
               sorted(engine.txn_to_str(engine.ledger.list()).values())

    def test_clear(self, engine):
        engine.restore(
            settings=self.SETTINGS,
            accounts=self.ACCOUNTS,
            tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES,
            assets=self.ASSETS,
            price_history=self.PRICES,
            revaluations=self.REVALUATIONS,
            profit_centers=self.PROFIT_CENTERS,
        )
        engine.clear()
        assert engine.ledger.list().empty, "Ledger was not cleared"
        assert engine.tax_codes.list().empty, "Tax codes were not cleared"
        assert engine.assets.list().empty, "Assets was not cleared"
        assert engine.accounts.list().empty, "Accounts was not cleared"
        assert engine.price_history.list().empty, "Price history was not cleared"
        assert engine.revaluations.list().empty, "Revaluations was not cleared"
        assert engine.profit_centers.list().empty, "Profit centers was not cleared"
