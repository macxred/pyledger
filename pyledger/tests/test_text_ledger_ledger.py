"""Test suite for TextLedger ledger operations."""

import pytest
import pandas as pd
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    @pytest.mark.skip(reason="TextLedger ignores incoming IDs when adding entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_write_ledger_directory(self, engine):
        # Define ledger entries with different nesting level
        file_1 = self.LEDGER_ENTRIES.copy()
        file_1["id"] = "level1/level2/file1.csv:" + file_1["id"]
        ids = [str(id) for id in range(11, 15)]
        file_2 = self.LEDGER_ENTRIES.query(f"id in {ids}")
        id = file_2["id"].astype(int)
        file_2["id"] = "file2.csv:" + (id - min(id) + 1).astype(str)

        # Populate ledger directory and check that ledger() returns original data
        expected = pd.concat([file_1, file_2], ignore_index=True)
        engine.ledger.write_directory(expected)
        expected = engine.ledger.standardize(expected)
        assert_frame_equal(expected, engine.ledger.list(), ignore_row_order=True)

        # Populate ledger directory with a subset and check that superfluous file is deleted
        engine.ledger.write_directory(file_1)
        expected = engine.ledger.standardize(file_1)
        assert_frame_equal(expected, engine.ledger.list())
        ledger_root = engine.root / "ledger"
        assert not (ledger_root / "file2.csv").exists(), "'file2.csv' was not deleted."

        # Clear ledger directory and ensure all ledger files are deleted
        engine.ledger.write_directory(engine.ledger.standardize(None))
        assert not any(ledger_root.rglob("*.csv")), "Some files were not deleted."
        assert engine.ledger.list().empty, "Ledger is not empty."

    def test_write_empty_ledger_directory(self, engine):
        engine.ledger.write_directory(engine.ledger.standardize(None))
        ledger_path = engine.root / "ledger"
        assert not ledger_path.exists() or not any(ledger_path.iterdir()), (
            "The ledger directory should be empty or non-existent"
        )
        assert engine.ledger.list().empty, "Reading ledger files should return empty df"

    def test_ledger_without_ledger_folder(self, engine):
        """Ledger() is expected to return an empty data frame if the ledger folder is missing."""
        expected_ledger = engine.ledger.standardize(None)
        assert_frame_equal(engine.ledger.list(), expected_ledger)

    def test_extra_columns(self, engine):
        extra_cols = ["new_column", "second_new_column"]
        entries = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        ids = engine.ledger.add(entries)  # noqa: F841

        # Add ledger entries with a new column
        expected = self.LEDGER_ENTRIES.query("id in ['3', '4']").copy()
        expected[extra_cols[0]] = "test value"
        engine.ledger.add(expected)
        current = engine.ledger.list()
        assert current.query("id in @ids")[extra_cols[0]].isna().all(), (
            "Pre existing entries should have new column with all NA values"
        )
        assert_frame_equal(current, pd.concat([entries, expected]), ignore_columns=["id"])

        # Modify ledger entries with a new column
        expected = engine.ledger.list()
        expected[extra_cols[1]] = "second test value"
        engine.ledger.modify(expected)
        assert_frame_equal(
            engine.ledger.list(), expected, ignore_columns=["id"]
        )

        # Standardize() method with drop_extra_columns=True should drop extra columns
        expected_without_extra_cols = expected.copy().drop(columns=extra_cols)
        assert_frame_equal(
            engine.ledger.standardize(expected, drop_extra_columns=True),
            expected_without_extra_cols, ignore_columns=["id"]
        )

        # List() method with drop_extra_columns=True should drop extra columns
        assert_frame_equal(
            engine.ledger.list(drop_extra_columns=True), expected_without_extra_cols,
            ignore_columns=["id"]
        )
