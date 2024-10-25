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

    def test_asset_mutators(self, ledger):
        ledger.restore(assets=self.ASSETS, settings=self.SETTINGS)
        # Add the first new asset with None as the date (NaT in DataFrame)
        new_asset1 = {
            "ticker": "BTC",
            "increment": 0.001,
            "date": None,
        }
        assets = ledger.assets()
        ledger.add_asset(**new_asset1)
        updated_assets = ledger.assets()
        outer_join = pd.merge(assets, updated_assets, how="outer", indicator=True)
        created_assets = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_assets) == 1, "Expected exactly one row to be added"
        assert created_assets["ticker"].item() == new_asset1["ticker"]
        assert created_assets["increment"].item() == new_asset1["increment"]
        assert pd.isna(created_assets["date"].item()), "Expected the date to be None"

        # Add the second new asset with a specific date
        new_asset2 = {
            "ticker": "ETH",
            "increment": 0.02,
            "date": datetime.date(2023, 1, 12),
        }
        assets = ledger.assets()
        ledger.add_asset(**new_asset2)
        updated_assets = ledger.assets()
        outer_join = pd.merge(assets, updated_assets, how="outer", indicator=True)
        created_assets = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_assets) == 1, "Expected exactly one row to be added"
        assert created_assets["ticker"].item() == new_asset2["ticker"]
        assert created_assets["increment"].item() == new_asset2["increment"]
        assert created_assets["date"].item() == pd.Timestamp(new_asset2["date"])

        # Modify the first asset (with None as the date)
        modified_asset1 = {
            "ticker": "BTC",
            "increment": 0.002,
            "date": None,
        }
        assets = ledger.assets()
        ledger.modify_asset(**modified_asset1)
        updated_assets = ledger.assets()
        outer_join = pd.merge(assets, updated_assets, how="outer", indicator=True)
        modified_assets = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(modified_assets) == 1, "Expected exactly one updated row"
        assert modified_assets["ticker"].item() == modified_asset1["ticker"]
        assert modified_assets["increment"].item() == modified_asset1["increment"]
        assert pd.isna(modified_assets["date"].item()), "Expected the date to still be None"

        # Modify the second asset (with a specific date)
        modified_asset2 = {
            "ticker": "ETH",
            "increment": 0.03,
            "date": datetime.date(2023, 1, 12),
        }
        assets = ledger.assets()
        ledger.modify_asset(**modified_asset2)
        updated_assets = ledger.assets()
        outer_join = pd.merge(assets, updated_assets, how="outer", indicator=True)
        modified_assets = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(modified_assets) == 1, "Expected exactly one updated row"
        assert modified_assets["ticker"].item() == modified_asset2["ticker"]
        assert modified_assets["increment"].item() == modified_asset2["increment"]
        assert modified_assets["date"].item() == pd.Timestamp(modified_asset2["date"])

        # Delete both assets
        ledger.delete_asset(ticker="BTC", date=None)
        ledger.delete_asset(ticker="ETH", date=datetime.date(2023, 1, 12))
        updated_assets = ledger.assets()
        assert "BTC" not in updated_assets["ticker"].values
        assert "ETH" not in updated_assets["ticker"].values

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
