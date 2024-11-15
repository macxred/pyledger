"""Test suite for TextLedger ledger operations."""

import pytest
import pandas as pd
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def pristine_engine(self, tmp_path):
        return TextLedger(tmp_path)

    @pytest.mark.skip(reason="TextLedger ignores incoming IDs when adding entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_write_ledger_directory(self, pristine_engine):
        # Define ledger entries with different nesting level
        file_1 = self.LEDGER_ENTRIES.copy()
        file_1["id"] = "level1/level2/file1.csv:" + file_1["id"]
        ids = [str(id) for id in range(11, 15)]
        file_2 = self.LEDGER_ENTRIES.query(f"id in {ids}")
        id = file_2["id"].astype(int)
        file_2["id"] = "file2.csv:" + (id - min(id) + 1).astype(str)

        # Populate ledger directory and check that ledger() returns original data
        expected = pd.concat([file_1, file_2], ignore_index=True)
        pristine_engine.ledger.write_directory(expected)
        expected = pristine_engine.ledger.standardize(expected)
        assert_frame_equal(expected, pristine_engine.ledger.list(), ignore_row_order=True)

        # Populate ledger directory with a subset and check that superfluous file is deleted
        pristine_engine.ledger.write_directory(file_1)
        expected = pristine_engine.ledger.standardize(file_1)
        assert_frame_equal(expected, pristine_engine.ledger.list())
        ledger_root = pristine_engine.root_path / "ledger"
        assert not (ledger_root / "file2.csv").exists(), "'file2.csv' was not deleted."

        # Clear ledger directory and ensure all ledger files are deleted
        pristine_engine.ledger.write_directory(pristine_engine.ledger.standardize(None))
        assert not any(ledger_root.rglob("*.csv")), "Some files were not deleted."
        assert pristine_engine.ledger.list().empty, "Ledger is not empty."

    def test_write_empty_ledger_directory(self, pristine_engine):
        pristine_engine.ledger.write_directory(pristine_engine.ledger.standardize(None))
        ledger_path = pristine_engine.root_path / "ledger"
        assert not ledger_path.exists() or not any(ledger_path.iterdir()), (
            "The ledger directory should be empty or non-existent"
        )
        assert pristine_engine.ledger.list().empty, "Reading ledger files should return empty df"

    def test_ledger_without_ledger_folder(self, pristine_engine):
        """Ledger() is expected to return an empty data frame if the ledger folder is missing."""
        expected_ledger = pristine_engine.ledger.standardize(None)
        assert_frame_equal(pristine_engine.ledger.list(), expected_ledger)
