"""Definition of abstract base class for testing asset operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestAssets(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_asset_accessor_mutators(self, ledger, ignore_row_order=False):
        ledger.restore(settings=self.SETTINGS)
        assets = self.ASSETS.sample(frac=1).reset_index(drop=True)
        for asset in assets.to_dict('records'):
            ledger.assets.add([asset])
        assert_frame_equal(
            ledger.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

        rows = [0, 3, len(assets) - 1]
        for i in rows:
            assets.loc[i, "increment"] = 0.001
            ledger.assets.modify([assets.loc[i]])
            assert_frame_equal(
                ledger.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
            )

        ledger.assets.delete([{
            "ticker": assets['ticker'].iloc[rows[0]], "date": assets['date'].iloc[rows[0]]
        }])
        assets = assets.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_asset_raises_error(
        self, ledger, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_asset = {"ticker": "AUD", "increment": 0.01, "date": datetime.date(2023, 1, 1)}
        ledger.assets.add([new_asset])
        with pytest.raises(error_class, match=error_message):
            ledger.assets.add([new_asset])

    def test_modify_nonexistent_asset_raises_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.assets.modify([{
                "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
            }])

    def test_delete_asset_allow_missing(self, ledger):
        ledger.restore(assets=self.ASSETS, settings=self.SETTINGS)
        with pytest.raises(ValueError, match="Some ids are not present in the data."):
            ledger.assets.delete([{
                "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
            }], allow_missing=False)
        ledger.assets.delete([{
            "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
        }], allow_missing=True)

    def test_mirror_assets(self, ledger):
        ledger.restore(settings=self.SETTINGS)
        target = pd.concat([self.ASSETS, ledger.assets.list()], ignore_index=True)
        original_target = target.copy()
        ledger.assets.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror_assets() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            ledger.assets.standardize(target), ledger.assets.list(), ignore_row_order=True
        )

        target = self.ASSETS.query("ticker not in ['USD', 'JPY']")
        ledger.assets.mirror(target, delete=True)
        assert_frame_equal(target, ledger.assets.list(), ignore_row_order=True)

        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["ticker"] == "AUD", "increment"] = 0.02
        ledger.assets.mirror(target, delete=True)
        assert_frame_equal(target, ledger.assets.list(), ignore_row_order=True)

    def test_mirror_empty_assets(self, ledger):
        ledger.restore(assets=self.ASSETS, settings=self.SETTINGS)
        assert not ledger.assets.list().empty
        ledger.assets.mirror(ledger.assets.standardize(None), delete=True)
        assert ledger.assets.list().empty

    @pytest.fixture()
    def ledger_with_assets(self, ledger):
        ledger.restore(settings=self.SETTINGS, assets=self.ASSETS)
        return ledger

    @pytest.mark.parametrize(
        "ticker, date, expected",
        [
            # Default increment for reporting_currency (CHF); date=None uses today
            ("reporting_currency", None, 0.01),
            # Exact date match for AUD on 2023-01-01
            ("AUD", datetime.date(2023, 1, 1), 0.001),
            # Default to today’s date for EUR
            ("TRY", None, 1),
            # Date before all CAD entries, uses NaT entry
            ("CAD", datetime.date(2020, 1, 1), 0.1),
            # Date after all AUD dates, uses latest increment
            ("AUD", datetime.date(2222, 1, 1), 0.01),
            # Latest date on/before 2023-12-31 for AUD
            ("AUD", datetime.date(2023, 12, 31), 0.001),
            # NaT and valid dates for EUR, date before all valid dates
            ("CHF", datetime.date(2023, 1, 1), 0.001),
        ]
    )
    def test_precision(self, ledger_with_assets, ticker, date, expected):
        assert ledger_with_assets.precision(ticker, date) == expected

    @pytest.mark.parametrize(
        "ticker, date, expected_exception, match",
        [
            ("XYZ", None, ValueError, "No asset definition available for ticker 'XYZ'"),
            ("GBP", datetime.date(2022, 1, 1), ValueError, "No asset definition available"),
        ]
    )
    def test_precision_exceptions(
        self, ledger_with_assets, ticker, date, expected_exception, match
    ):
        with pytest.raises(expected_exception, match=match):
            ledger_with_assets.precision(ticker, date)
