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

    def test_ledger_write_and_read_ledger_directory(self, ledger):
        # Define ledger entries with different nesting level
        level_1 = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        level_1["id"] = "level1/ledger1.csv:" + level_1["id"]
        level_2 = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        level_2["id"] = "level1/level2/ledger2.csv:" + level_2["id"]
        entries = pd.concat([level_1, level_2], ignore_index=True)
        ledger.write_ledger_directory(entries)

        # Verify if the file content matches the expected format
        result = pd.read_csv(ledger.root_path / "ledger/level1/ledger1.csv", skipinitialspace=True)
        result = ledger.standardize_ledger(result)
        assert_frame_equal(result, level_1, ignore_columns=["id"])
        result = pd.read_csv(
            ledger.root_path / "ledger/level1/level2/ledger2.csv", skipinitialspace=True
        )
        result = ledger.standardize_ledger(result)
        assert_frame_equal(result, level_1, ignore_columns=["id"])

        # Verify if the ledger is read correctly
        assert_frame_equal(ledger.ledger(), ledger.standardize_ledger(entries))

    def test_ledger_write_and_read_empty_ledger_directory(self, ledger):
        df = ledger.standardize_ledger(None)
        ledger.write_ledger_directory(df)

        ledger_path = ledger.root_path / "ledger"
        assert not ledger_path.exists() or not any(ledger_path.iterdir()), (
            "The ledger directory should be empty or non-existent"
        )
        assert ledger.ledger().empty, "Reading ledger files should return empty df"

    def test_ledger_read_no_ledger_folder_return_empty_df(self, ledger):
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
