"""Test suite for assets operations."""

import pytest
from .base_test_assets import BaseTestAssets
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestAssets(BaseTestAssets):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_asset_mutators_does_not_change_order(self, ledger):
        """Test to ensure that mutator functions make minimal invasive changes to assets file,
        preserving the original row order so that Git diffs show only the intended modifications.
        """
        assets = self.ASSETS.sample(frac=1).reset_index(drop=True)
        for asset in assets.to_dict('records'):
            ledger.add_asset(**asset)
        assert_frame_equal(ledger.assets(), assets, check_like=True)

        rows = [0, 3, len(assets) - 1]
        for i in rows:
            assets.loc[i, "increment"] = 0.001
            ledger.modify_asset(**assets.loc[i].to_dict())
            assert_frame_equal(ledger.assets(), assets, check_like=True)

        ledger.delete_asset(assets['ticker'].iloc[rows[0]], assets['date'].iloc[rows[0]])
        assets = assets.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(ledger.assets(), assets, check_like=True)
