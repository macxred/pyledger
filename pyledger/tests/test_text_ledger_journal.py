"""Test suite for TextLedger journal operations."""

import pytest
import pandas as pd
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_journal import BaseTestJournal


class TestLedger(BaseTestJournal):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    @pytest.mark.skip(reason="TextLedger ignores incoming IDs when adding entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_write_journal_directory(self, engine):
        # Define journal entries with different nesting level
        file_1 = self.JOURNAL.copy()
        file_1["id"] = "level1/level2/file1.csv:" + file_1["id"]
        ids = [str(id) for id in range(11, 15)]
        file_2 = self.JOURNAL.query(f"id in {ids}")
        id = file_2["id"].astype(int)
        file_2["id"] = "file2.csv:" + (id - min(id) + 1).astype(str)

        # Populate journal directory and check that journal() returns original data
        expected = pd.concat([file_1, file_2], ignore_index=True)
        engine.journal.write_directory(expected)
        expected = engine.journal.standardize(expected)
        assert_frame_equal(expected, engine.journal.list(), ignore_row_order=True)

        # Populate journal directory with a subset and check that superfluous file is deleted
        engine.journal.write_directory(file_1)
        expected = engine.journal.standardize(file_1)
        assert_frame_equal(expected, engine.journal.list())
        journal_root = engine.root / "journal"
        assert not (journal_root / "file2.csv").exists(), "'file2.csv' was not deleted."

        # Clear journal directory and ensure all journal files are deleted
        engine.journal.write_directory(engine.journal.standardize(None))
        assert not any(journal_root.rglob("*.csv")), "Some files were not deleted."
        assert engine.journal.list().empty, "Journal is not empty."

    def test_write_empty_journal_directory(self, engine):
        engine.journal.write_directory(engine.journal.standardize(None))
        journal_path = engine.root / "journal"
        assert not journal_path.exists() or not any(journal_path.iterdir()), (
            "The journal directory should be empty or non-existent"
        )
        assert engine.journal.list().empty, "Reading journal files should return empty df"

    def test_journal_without_journal_folder(self, engine):
        """Journal() is expected to return an empty data frame if the journal folder is missing."""
        expected_journal = engine.journal.standardize(None)
        assert_frame_equal(engine.journal.list(), expected_journal)

    def test_extra_columns(self, engine):
        extra_cols = ["new_column", "second_new_column"]
        entries = self.JOURNAL.query("id in ['1', '2']").copy()
        ids = engine.journal.add(entries)  # noqa: F841

        # Add journal entries with a new column
        expected = self.JOURNAL.query("id in ['3', '4']").copy()
        expected[extra_cols[0]] = "test value"
        engine.journal.add(expected)
        current = engine.journal.list()
        assert current.query("id in @ids")[extra_cols[0]].isna().all(), (
            "Pre existing entries should have new column with all NA values"
        )
        assert_frame_equal(current, pd.concat([entries, expected]), ignore_columns=["id"])

        # Modify journal entries with a new column
        expected = engine.journal.list()
        expected[extra_cols[1]] = "second test value"
        engine.journal.modify(expected)
        assert_frame_equal(
            engine.journal.list(), expected, ignore_columns=["id"]
        )

        # Standardize() method with drop_extra_columns=True should drop extra columns
        expected_without_extra_cols = expected.copy().drop(columns=extra_cols)
        assert_frame_equal(
            engine.journal.standardize(expected, drop_extra_columns=True),
            expected_without_extra_cols, ignore_columns=["id"]
        )

        # List() method with drop_extra_columns=True should drop extra columns
        assert_frame_equal(
            engine.journal.list(drop_extra_columns=True), expected_without_extra_cols,
            ignore_columns=["id"]
        )
