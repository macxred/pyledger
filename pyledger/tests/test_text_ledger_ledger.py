"""Test suite for TextLedger ledger operations."""

import pytest
import pandas as pd
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    @pytest.mark.skip(reason="TextLedger ignores incoming IDs when adding entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_write_ledger_directory(self, ledger):
        # Define ledger entries with different nesting level
        file_1 = self.LEDGER_ENTRIES.copy()
        file_1["id"] = "level1/level2/file1.csv:" + file_1["id"]
        ids = [str(id) for id in range(11, 15)]
        file_2 = self.LEDGER_ENTRIES.query(f"id in {ids}")
        id = file_2["id"].astype(int)
        file_2["id"] = "file2.csv:" + (id - min(id) + 1).astype(str)

        # Populate ledger directory and check that ledger() returns original data
        expected = pd.concat([file_1, file_2], ignore_index=True)
        ledger.ledger.write_directory(expected)
        expected = ledger.ledger.standardize(expected)
        assert_frame_equal(expected, ledger.ledger.list(), ignore_row_order=True)

        # Populate ledger directory with a subset and check that superfluous file is deleted
        ledger.ledger.write_directory(file_1)
        expected = ledger.ledger.standardize(file_1)
        assert_frame_equal(expected, ledger.ledger.list())
        ledger_root = ledger.root_path / "ledger"
        assert not (ledger_root / "file2.csv").exists(), "'file2.csv' was not deleted."

        # Clear ledger directory and ensure all ledger files are deleted
        ledger.ledger.write_directory(ledger.ledger.standardize(None))
        assert not any(ledger_root.rglob("*.csv")), "Some files were not deleted."
        assert ledger.ledger.list().empty, "Ledger is not empty."

    def test_write_empty_ledger_directory(self, ledger):
        ledger.ledger.write_directory(ledger.ledger.standardize(None))
        ledger_path = ledger.root_path / "ledger"
        assert not ledger_path.exists() or not any(ledger_path.iterdir()), (
            "The ledger directory should be empty or non-existent"
        )
        assert ledger.ledger.list().empty, "Reading ledger files should return empty df"

    def test_ledger_without_ledger_folder(self, ledger):
        """Ledger() is expected to return an empty data frame if the ledger folder is missing."""
        expected_ledger = ledger.ledger.standardize(None)
        assert_frame_equal(ledger.ledger.list(), expected_ledger)
