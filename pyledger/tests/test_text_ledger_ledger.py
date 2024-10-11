"""Test suite for TextLedger ledger operations."""

import time
import pytest
import pandas as pd
from io import StringIO
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


# flake8: noqa: E501
LEDGER_CSV = """
    id,                                 date,       account, contra, currency, amount, report_amount, tax_code,   description,                        document
    ledger/ledger1.csv:1,               2024-05-24,   9992,    9995,     CHF,  100.00,              ,   OutRed,   pytest single transaction 1,        /file1.txt
    ledger/ledger1.csv:2,               2024-05-24,   9991,        ,     USD, -100.00,        -88.88,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/ledger1.csv:2,               2024-05-24,   9991,        ,     USD,    1.00,          0.89,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/ledger1.csv:2,               2024-05-24,   9991,        ,     USD,   99.00,         87.99,   OutRed,   pytest collective txn 1 - line 1,
    ledger/level1/ledger2.csv:3,        2024-05-24,   9992,    9995,     CHF,  100.00,              ,   OutRed,   pytest single transaction 1,        /file1.txt
    ledger/level1/ledger2.csv:4,        2024-05-24,   9991,        ,     USD, -100.00,        -88.88,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/level1/ledger2.csv:4,        2024-05-24,   9991,        ,     USD,    1.00,          0.89,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/level1/ledger2.csv:4,        2024-05-24,   9991,        ,     USD,   99.00,         87.99,   OutRed,   pytest collective txn 1 - line 1,
    ledger/level1/level2/ledger3.csv:5, 2024-05-24,   9992,    9995,     CHF,  100.00,              ,   OutRed,   pytest single transaction 1,        /file1.txt
    ledger/level1/level2/ledger3.csv:6, 2024-05-24,   9991,        ,     USD, -100.00,        -88.88,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/level1/level2/ledger3.csv:6, 2024-05-24,   9991,        ,     USD,    1.00,          0.89,   OutRed,   pytest collective txn 1 - line 1,   /subdir/file2.txt
    ledger/level1/level2/ledger3.csv:6, 2024-05-24,   9991,        ,     USD,   99.00,         87.99,   OutRed,   pytest collective txn 1 - line 1,
"""
# flake8: enable

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

    def test_ledger_read_ledger_csv_files(self, tmp_path):
        """Test ensure ledger() can handle complex directory structures, reading
        multiple files across different folder levels.
        """
        # Create a nested folder structure under 'ledger'
        ledger_dir = tmp_path / "ledger"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        (ledger_dir / "level1").mkdir()
        (ledger_dir / "level1/level2").mkdir()

        # Populate the folders with CSV files containing ledger entries
        df = pd.DataFrame(self.LEDGER_ENTRIES.query("id in ['1', '2']"))
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)
        df.drop(columns=["id"], inplace=True)
        df.to_csv(ledger_dir / "ledger1.csv", index=False)
        df.to_csv(ledger_dir / "level1/ledger2.csv", index=False)
        df.to_csv(ledger_dir / "level1/level2/ledger3.csv", index=False)

        # Assert that the real ledger matches the expected ledger
        ledger = TextLedger(root_path=tmp_path)
        real_ledger = ledger.ledger()
        expected_ledger = ledger.standardize_ledger(pd.read_csv(
            StringIO(LEDGER_CSV), skipinitialspace=True
        ))
        assert_frame_equal(real_ledger, expected_ledger, ignore_row_order=True)

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
        assert_frame_equal(output, expected_ledger, ignore_row_order=True)

    def test_ledger_for_save(self, tmp_path):
        """Tests _ledger_for_save() for correct formatting and mapping of ledger entries."""

        ledger = TextLedger(root_path=tmp_path)
        dates = pd.Series([
            "2024-05-24", "2024-05-24", pd.NaT, pd.NaT,
            "2024-05-24", "2024-05-24", pd.NaT, pd.NaT,
            "2024-05-24", "2024-05-24", pd.NaT, pd.NaT
        ], name="date", dtype="datetime64[ns]")

        paths = pd.Series([
            "ledger/default.csv", "ledger/default.csv", "ledger/default.csv", "ledger/default.csv",
            "ledger/ledger1.csv", "ledger/ledger1.csv", "ledger/ledger1.csv", "ledger/ledger1.csv",
            "ledger/level1/ledger2.csv", "ledger/level1/ledger2.csv", "ledger/level1/ledger2.csv",
            "ledger/level1/ledger2.csv"
        ], name="__csv_path__")
        paths = paths.apply(lambda x: str(tmp_path / x))

        # Prepare ledger entries and assign full paths.
        ledger_entries = self.LEDGER_ENTRIES.query("id in ['1', '2']")
        ledger_entries = pd.concat([ledger_entries] * 3, ignore_index=True)
        ledger_entries["id"] = paths[:len(ledger_entries)].values + ":" + ledger_entries["id"]

        # Expected DataFrame setup with dates and paths.
        expected_ledger = ledger.standardize_ledger(ledger_entries).drop(columns="id")
        expected_ledger["date"] = dates
        expected_ledger["__csv_path__"] = paths.astype("string[python]")

        # Test and validate output.
        output = ledger._ledger_for_save(ledger_entries)
        assert_frame_equal(output, expected_ledger)
