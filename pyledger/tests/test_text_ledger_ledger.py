"""Test suite for TextLedger ledger operations."""

import time
import pytest
import pandas as pd
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_ledger_invalidation(self, ledger):
        assert not ledger._is_expired(ledger._ledger_time)
        ledger._invalidate_ledger()
        assert ledger._is_expired(ledger._ledger_time)

    def test_ledger_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path, cache_timeout=1)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._ledger_time)

    def test_ledger_read_ledger_different_nesting_levels(self, tmp_path):
        ledger = TextLedger(root_path=tmp_path)

        # Create ledger entries with different nesting level
        ledger_level_1 = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        ledger_level_1["id"] = "level1/ledger2.csv:" + ledger_level_1["id"]
        ledger_level_2 = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        ledger_level_2["id"] = "level1/level2/ledger2.csv:" + ledger_level_2["id"]
        entries = ledger.standardize_ledger(
            pd.concat([ledger_level_1, ledger_level_2], ignore_index=True)
        )

        ledger.write_ledger_directory(entries)
        entries["id"] = "ledger/" + entries["id"]
        assert_frame_equal(ledger.ledger(), entries)

    def test_ledger_read_empty_files(self, tmp_path):
        """Ensure ledger() processes empty CSV files and returns an empty ledger."""
        ledger_dir = tmp_path / "ledger"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        (ledger_dir / "default.csv").mkdir()

        ledger = TextLedger(root_path=tmp_path)
        output = ledger.ledger()
        expected_ledger = ledger.standardize_ledger(None)
        assert_frame_equal(output, expected_ledger)

    def test_ledger_read_no_ledger_folder(self, tmp_path):
        """Ensure ledger() returns an empty DataFrame when the '/ledger' folder does not exist."""
        ledger = TextLedger(root_path=tmp_path)
        output = ledger.ledger()
        expected_ledger = ledger.standardize_ledger(None)
        assert_frame_equal(output, expected_ledger)
