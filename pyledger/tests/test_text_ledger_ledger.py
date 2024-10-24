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
        assert ledger._is_expired(ledger._ledger_time)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_time)
        ledger._invalidate_ledger()
        assert ledger._is_expired(ledger._ledger_time)

    def test_ledger_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path, cache_timeout=1)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._ledger_time)

    def test_write_ledger_directory(self, ledger):
        # Define ledger entries with different nesting level
        file_1 = self.LEDGER_ENTRIES.copy()
        file_1["id"] = "level1/level2/file1.csv:" + file_1["id"]
        ids = [str(id) for id in range(11, 15)]
        file_2 = self.LEDGER_ENTRIES.query(f"id in {ids}")
        file_2["id"] = "file2.csv:" + (file_2["id"].astype(int) - 10).astype(str)

        # Populate ledger directory and check that ledger() returns original data
        expected = pd.concat([file_1, file_2], ignore_index=True)
        ledger.write_ledger_directory(expected)
        assert_frame_equal(expected, ledger.ledger(), ignore_row_order=True)

        # Populate ledger directory with a subset and check that superfluous file is deleted
        ledger.write_ledger_directory(file_1)
        assert_frame_equal(file_1, ledger.ledger())
        ledger_root = ledger.root_path / "ledger"
        assert not (ledger_root / "file2.csv").exists(), "'file2.csv' was not deleted."

        # Clear ledger directory and ensure all ledger files are deleted
        ledger.write_ledger_directory(ledger.standardize_ledger(None))
        assert not any(ledger_root.rglob("*.csv")), "Some files were not deleted."
        assert ledger.ledger().empty, "Ledger is not empty."

    def test_write_empty_ledger_directory(self, ledger):
        ledger.write_ledger_directory(ledger.standardize_ledger(None))
        ledger_path = ledger.root_path / "ledger"
        assert not ledger_path.exists() or not any(ledger_path.iterdir()), (
            "The ledger directory should be empty or non-existent"
        )
        assert ledger.ledger().empty, "Reading ledger files should return empty df"

    def test_ledger_without_ledger_folder(self, ledger):
        """Ledger() is expected to return an empty data frame if the ledger folder is missing."""
        expected_ledger = ledger.standardize_ledger(None)
        assert_frame_equal(ledger.ledger(), expected_ledger)

    def test_ledger_mutators_does_not_change_order(self, ledger):
        """Test to ensure that mutator functions make minimal invasive changes to ledger files,
        preserving the original row order so that Git diffs show only the intended modifications.
        """
        ledger_ids = [str(id) for id in range(1, 15)]
        for id in ledger_ids:
            ledger.add_ledger_entry(self.LEDGER_ENTRIES.query(f"id == '{id}'"))
        expected = ledger.standardize_ledger(self.LEDGER_ENTRIES.query(f"id in {ledger_ids}"))
        expected["id"] = "default.csv:" + expected["id"]
        assert_frame_equal(ledger.ledger(), expected)

        expected.loc[expected["id"] == "default.csv:4", "amount"] = 8888888888
        df = expected.query("id == 'default.csv:4'")
        ledger.modify_ledger_entry(df)
        expected.loc[expected["id"] == "default.csv:9", "amount"] = 7777777777
        df = expected.query("id == 'default.csv:9'")
        ledger.modify_ledger_entry(df)
        assert_frame_equal(ledger.ledger(), expected)

        to_delete = ['default.csv:3', 'default.csv:10']
        expected = expected.query(f"id not in {to_delete}")
        ledger.delete_ledger_entries(to_delete)
        assert_frame_equal(ledger.ledger(), expected, ignore_columns=["id"], ignore_index=True)
