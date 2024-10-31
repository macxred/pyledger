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
        """Test to ensure that accessor mutator functions work as expected.
        With `ignore_row_order=False`, we ensure minimal invasive changes, preserving
        the original row order of the data so that Git diffs show only the intended modifications.
        """
        ledger.restore(settings=self.SETTINGS)
        assets = self.ASSETS.sample(frac=1).reset_index(drop=True)
        for asset in assets.to_dict('records'):
            ledger.add_asset(**asset)
        assert_frame_equal(
            ledger.assets(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

        rows = [0, 3, len(assets) - 1]
        for i in rows:
            assets.loc[i, "increment"] = 0.001
            ledger.modify_asset(**assets.loc[i].to_dict())
            assert_frame_equal(
                ledger.assets(), assets, check_like=True, ignore_row_order=ignore_row_order
            )

        ledger.delete_asset(assets['ticker'].iloc[rows[0]], assets['date'].iloc[rows[0]])
        assets = assets.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.assets(), assets, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_asset_raises_error(
        self, ledger, error_class=ValueError, error_message="already exists"
    ):
        new_asset = {"ticker": "AUD", "increment": 0.01, "date": datetime.date(2023, 1, 1)}
        ledger.add_asset(**new_asset)
        with pytest.raises(error_class, match=error_message):
            ledger.add_asset(**new_asset)

    def test_modify_nonexistent_asset_raises_error(
        self, ledger, error_class=ValueError, error_message="not found"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.modify_asset(
                ticker="FAKE", increment=100.0, date=datetime.date(2023, 1, 1)
            )

    def test_delete_asset_allow_missing(self, ledger):
        ledger.restore(assets=self.ASSETS, settings=self.SETTINGS)
        with pytest.raises(ValueError, match="not found"):
            ledger.delete_asset(
                ticker="NON_EXISTENT", date=datetime.date(2023, 1, 1), allow_missing=False
            )
        ledger.delete_asset(
            ticker="NON_EXISTENT", date=datetime.date(2023, 1, 1), allow_missing=True
        )

    def test_mirror_assets(self, ledger):
        initial_assets = ledger.assets()
        assets = ledger.standardize_assets(self.ASSETS)
        target_df = pd.concat([assets, initial_assets], ignore_index=True)
        target_df = ledger.standardize_assets(target_df)
        initial = target_df.copy()
        ledger.mirror_assets(target_df, delete=False)
        mirrored_df = ledger.assets()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True)
        # Mirroring should not change the initial df
        assert_frame_equal(initial, target_df, ignore_row_order=True)

        target_df = target_df[~target_df["ticker"].isin(["USD", "JPY"])]
        ledger.mirror_assets(target_df.copy(), delete=True)
        mirrored_df = ledger.assets()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True)

        target_df = target_df.sample(frac=1).reset_index(drop=True)
        target_df.loc[target_df["ticker"] == "AUD", "increment"] = 0.02
        ledger.mirror_assets(target_df.copy(), delete=True)
        mirrored_df = ledger.assets()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True)

    def test_mirror_empty_assets(self, ledger):
        ledger.restore(assets=self.ASSETS, settings=self.SETTINGS)
        assert not ledger.assets().empty
        ledger.mirror_assets(ledger.standardize_assets(None), delete=True)
        assert ledger.assets().empty
