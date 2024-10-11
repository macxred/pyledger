"""Test suite for save_files() method tests."""

import pytest
import pandas as pd
from io import StringIO
from pyledger.helpers import save_files
from consistent_df import assert_frame_equal


SAMPLE_CSV = """
    __csv_path__,              date,      account,  contra, currency,  amount
    file1.csv,                 2024-05-24,   9992,    9995,      CHF,  100.00
    level1/file2.csv,          2024-05-24,   9992,    9995,      CHF,  100.00
    level1/level2/file3.csv,   2024-05-24,   9992,    9995,      CHF,  100.00
"""
SAMPLE_DF = pd.read_csv(StringIO(SAMPLE_CSV), skipinitialspace=True)


def test_save_files(tmp_path):
    save_files(SAMPLE_DF, tmp_path)
    expected = SAMPLE_DF.query("__csv_path__ == 'file1.csv'")
    output = pd.read_csv(tmp_path / "file1.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["__csv_path__"]
    )

    expected = SAMPLE_DF.query("__csv_path__ == 'level1/file2.csv'")
    output = pd.read_csv(tmp_path / "level1/file2.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["__csv_path__"]
    )

    expected = SAMPLE_DF.query("__csv_path__ == 'level1/level2/file3.csv'")
    output = pd.read_csv(tmp_path / "level1/level2/file3.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["__csv_path__"]
    )


def test_save_files_remove_empty_folders_and_files(tmp_path):
    folder = tmp_path
    (folder / "empty_folder").mkdir(parents=True, exist_ok=True)
    (folder / "empty_folder/empty_subfolder").mkdir(parents=True, exist_ok=True)
    empty_file1 = folder / "empty_file1.csv"
    empty_file2 = folder / "empty_folder/empty_file2.csv"
    empty_file1.touch()
    empty_file2.touch()

    save_files(SAMPLE_DF.query("__csv_path__ == 'file1.csv'"), folder)
    assert not (folder / "empty_folder/empty_subfolder").exists(), (
        "The 'empty_subfolder' should be removed."
    )
    assert not (folder / "empty_folder").exists(), "The 'empty_folder' should be removed."
    assert not empty_file1.exists(), "The 'empty_file1.csv' should be removed."
    assert not empty_file2.exists(), "The 'empty_file2.csv' should be removed."
    assert (folder / "default.csv").exists, "Should create default csv file"


def test_save_files_no_path_column_raise_error(tmp_path):
    with pytest.raises(ValueError, match="The DataFrame must contain a '__csv_path__' column."):
        save_files(pd.DataFrame({}), tmp_path)
