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
    def engine(self):
        pass

    def test_asset_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(settings=self.SETTINGS)

        # Add assets one by one and with multiple rows
        assets = self.ASSETS.sample(frac=1).reset_index(drop=True)
        for asset in assets.head(-3).to_dict('records'):
            engine.assets.add([asset])
        engine.assets.add(assets.tail(3))
        assert_frame_equal(
            engine.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        assets.loc[0, "increment"] = 0.001
        engine.assets.modify([{
            "ticker": assets.loc[0, "ticker"],
            "date": assets.loc[0, "date"],
            "increment": 0.001
        }])
        assert_frame_equal(
            engine.assets.list(), assets,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        assets.loc[3, "increment"] = 0.000001
        engine.assets.modify([assets.loc[3]])
        assert_frame_equal(
            engine.assets.list(), assets,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        assets.loc[assets.index[[1, -1]], "increment"] = 0.000001
        engine.assets.modify(assets.loc[assets.index[[1, -1]]])
        assert_frame_equal(
            engine.assets.list(), assets,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.assets.delete([{
            "ticker": assets['ticker'].iloc[0], "date": assets['date'].iloc[0]
        }])
        assets = assets.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.assets.delete(assets.iloc[[1, -1]])
        assets = assets.drop(assets.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.assets.list(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_asset_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_asset = {"ticker": "AUD", "increment": 0.01, "date": datetime.date(2023, 1, 1)}
        engine.assets.add([new_asset])
        with pytest.raises(error_class, match=error_message):
            engine.assets.add([new_asset])

    def test_modify_nonexistent_asset_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.assets.modify([{
                "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
            }])

    def test_delete_asset_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.assets.delete([{
                "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
            }], allow_missing=False)
        engine.assets.delete([{
            "ticker": "FAKE", "increment": 100.0, "date": datetime.date(2023, 1, 1)
        }], allow_missing=True)

    def test_mirror_assets(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.ASSETS, engine.assets.list()], ignore_index=True)
        original_target = target.copy()
        engine.assets.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror_assets() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            engine.assets.standardize(target), engine.assets.list(), ignore_row_order=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.ASSETS.query("ticker not in ['USD', 'JPY']")
        engine.assets.mirror(target, delete=False)
        assert_frame_equal(original_target, engine.assets.list(), ignore_row_order=True)

        # Mirror with delete=True should change the data
        engine.assets.mirror(target, delete=True)
        assert_frame_equal(target, engine.assets.list(), ignore_row_order=True)

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["ticker"] == "AUD", "increment"] = 0.02
        engine.assets.mirror(target, delete=True)
        assert_frame_equal(target, engine.assets.list(), ignore_row_order=True)

    def test_mirror_empty_assets(self, engine):
        engine.restore(assets=self.ASSETS, settings=self.SETTINGS)
        assert not engine.assets.list().empty
        engine.assets.mirror(engine.assets.standardize(None), delete=True)
        assert engine.assets.list().empty

    @pytest.fixture()
    def engine_with_assets(self, engine):
        engine.restore(settings=self.SETTINGS, assets=self.ASSETS)
        return engine

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
    def test_precision(self, engine_with_assets, ticker, date, expected):
        assert engine_with_assets.precision(ticker, date) == expected

    @pytest.mark.parametrize(
        "ticker, date, expected_exception, match",
        [
            ("XYZ", None, ValueError, "No asset definition available for ticker 'XYZ'"),
            ("GBP", datetime.date(2022, 1, 1), ValueError, "No asset definition available"),
        ]
    )
    def test_precision_exceptions(
        self, engine_with_assets, ticker, date, expected_exception, match
    ):
        with pytest.raises(expected_exception, match=match):
            engine_with_assets.precision(ticker, date)
