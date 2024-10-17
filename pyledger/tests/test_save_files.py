"""Test suite for save_files() method."""

import pytest
import pandas as pd
from io import StringIO
from pyledger.helpers import save_files
from consistent_df import assert_frame_equal


SAMPLE_CSV = """
    __csv_path__,              date,      account,  contra, currency,  amount
    file1.csv,                 2024-05-24,   9992,    9995,      CHF,  100.00
    file1.csv,                 2024-05-24,   9992,    9995,      CHF,  200.00
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


def test_save_files_remove_empty_files(tmp_path):
    (tmp_path / "empty_folder").mkdir(parents=True, exist_ok=True)
    empty_file1 = tmp_path / "empty_file1.csv"
    empty_file2 = tmp_path / "empty_folder/empty_file2.csv"
    empty_file1.touch()
    empty_file2.touch()

    save_files(SAMPLE_DF.query("__csv_path__ == 'file1.csv'"), tmp_path)
    assert not empty_file1.exists(), "The 'empty_file1.csv' should be removed."
    assert not empty_file2.exists(), "The 'empty_file2.csv' should be removed."


def test_save_files_no_path_column_raise_error(tmp_path):
    with pytest.raises(ValueError, match="The DataFrame must contain a '__csv_path__' column."):
        save_files(pd.DataFrame({}), tmp_path)


def test_save_files_empty_dataframe_with_path_column(tmp_path):
    empty_df = pd.DataFrame({"__csv_path__": []})
    save_files(empty_df, tmp_path)
    assert not any(tmp_path.rglob("*.csv")), "No files should be created."


def test_save_files_overwrite(tmp_path):
    df = SAMPLE_DF.query("__csv_path__ == 'file1.csv'").copy()
    save_files(df, tmp_path)

    # Change data and save again
    df.loc[0, "amount"] = 200.00
    save_files(df, tmp_path)

    output = pd.read_csv(tmp_path / "file1.csv", skipinitialspace=True)
    assert output["amount"].iloc[0] == 200.00, "File content should be overwritten"
