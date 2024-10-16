"""Test suite for TextLedger ledger operations."""

import time
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
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

    def test_write_read_ledger_file(self, ledger):
        # Define ledger entries with inconsistent shape
        df = self.LEDGER_ENTRIES.query("id in ['1', '2', '3']").copy()
        df = df[np.random.permutation(df.columns)]
        df.sort_values(by="amount", inplace=True)

        path = Path(ledger.root_path / "ledger/test.csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        output = ledger.write_ledger_file(df, path)

        # Verify if the file content matches the expected format
        result = ledger.standardize_ledger(pd.read_csv(path, skipinitialspace=True))
        result["date"] = result["date"].where(~result.duplicated(subset="id"), None)
        assert_frame_equal(output, result, ignore_columns=["id"], ignore_index=True)

        # Verify if the ledger is read correctly
        result = ledger.standardize_ledger(result)
        result["id"] = "test.csv:" + result["id"]
        assert_frame_equal(ledger.read_ledger_files(), result, check_like=True)

    def test_write_read_empty_ledger_file(self, ledger):
        path = Path(ledger.root_path / "ledger/test.csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)

        with pytest.raises(pd.errors.EmptyDataError):
            pd.read_csv(path)
        assert ledger.read_ledger_files().empty, "Reading ledger files should return empty df"

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

    def test_ledger_write_ledger_directory_with_none(self, ledger):
        df = self.LEDGER_ENTRIES.query("id == '1'").copy().drop(columns=["id"])
        ledger.add_ledger_entry(df)
        ledger.write_ledger_directory()

        # Verify if the file content matches the expected format
        result = pd.read_csv(ledger.root_path / "ledger/default.csv", skipinitialspace=True)
        result = ledger.standardize_ledger(result)
        assert_frame_equal(result, df, ignore_columns=["id"], check_like=True)

        # Verify if the ledger is read correctly
        df["id"] = "default.csv:" + df["id"]
        assert_frame_equal(ledger.ledger(), ledger.standardize_ledger(df), check_like=True)

    def test_ledger_read_no_ledger_folder_return_empty_df(self, ledger):
        expected_ledger = ledger.standardize_ledger(None)
        assert_frame_equal(ledger.ledger(), expected_ledger)

    def test_add_modify_ledger_does_not_change_entries_order(self, ledger):
        ledger.add_ledger_entry(self.LEDGER_ENTRIES.query("id == '3'"))
        ledger.add_ledger_entry(self.LEDGER_ENTRIES.query("id == '2'"))
        ledger.add_ledger_entry(self.LEDGER_ENTRIES.query("id == '1'"))
        expected = pd.concat([
            self.LEDGER_ENTRIES.query("id == '3'"),
            self.LEDGER_ENTRIES.query("id == '2'"),
            self.LEDGER_ENTRIES.query("id == '1'"),
        ], ignore_index=True)
        expected = ledger.standardize_ledger(expected)
        assert_frame_equal(ledger.ledger(), expected, ignore_columns=["id"])

        df = self.LEDGER_ENTRIES.query("id == '1'").copy()
        df = df.assign(id="default.csv:3", amount=99999)
        ledger.modify_ledger_entry(df)
        expected = expected[expected["id"] != '1']
        expected = ledger.standardize_ledger(pd.concat([expected, df], ignore_index=True))
        assert_frame_equal(ledger.ledger(), expected, ignore_columns=["id"])
