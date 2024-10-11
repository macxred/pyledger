"""Test suite for TextLedger tests."""

import pandas as pd
from io import StringIO
from pyledger import TextLedger
from consistent_df import assert_frame_equal


SAMPLE_CSV = """
    file_path,                          date,      account,  contra, currency,  amount
    ledger/ledger1.csv,                 2024-05-24,   9992,    9995,      CHF,  100.00
    ledger/level1/ledger2.csv,          2024-05-24,   9992,    9995,      CHF,  100.00
    ledger/level1/level2/ledger3.csv,   2024-05-24,   9992,    9995,      CHF,  100.00
"""
SAMPLE_DF = pd.read_csv(StringIO(SAMPLE_CSV), skipinitialspace=True)


def test_save_files(tmp_path):
    ledger = TextLedger(tmp_path)
    ledger.save_files(SAMPLE_DF, "ledger")

    expected = SAMPLE_DF.query("file_path == 'ledger/ledger1.csv'")
    output = pd.read_csv(tmp_path / "ledger/ledger1.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["file_path"]
    )

    expected = SAMPLE_DF.query("file_path == 'ledger/level1/ledger2.csv'")
    output = pd.read_csv(tmp_path / "ledger/level1/ledger2.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["file_path"]
    )

    expected = SAMPLE_DF.query("file_path == 'ledger/level1/level2/ledger3.csv'")
    output = pd.read_csv(tmp_path / "ledger/level1/level2/ledger3.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["file_path"]
    )


def test_save_files_empty_file_path(tmp_path):
    ledger = TextLedger(tmp_path)
    df = SAMPLE_DF.copy()
    df["file_path"] = None
    ledger.save_files(df, "ledger")
    output = pd.read_csv(tmp_path / "ledger/default.csv", skipinitialspace=True)
    assert_frame_equal(
        output, df, ignore_row_order=True, ignore_columns=["file_path"]
    )


def test_save_files_no_file_path_column(tmp_path):
    ledger = TextLedger(tmp_path)
    df = SAMPLE_DF.copy()
    df.drop(columns=["file_path"], inplace=True)
    ledger.save_files(df, "ledger")
    expected = df.query("file_path == 'ledger/default.csv'")
    output = pd.read_csv(tmp_path / "ledger/default.csv", skipinitialspace=True)
    assert_frame_equal(
        output, expected, ignore_row_order=True, ignore_columns=["file_path", "id"]
    )


def test_save_files_creates_default_folder(tmp_path):
    ledger = TextLedger(tmp_path)
    ledger_folder = tmp_path / "ledger"

    ledger_folder.rmdir()
    ledger.save_files(pd.DataFrame({}), "ledger")
    assert ledger_folder.exists(), "The 'ledger' folder should be created."
    assert ledger_folder.is_dir(), "The 'ledger' path should be a directory."


def test_save_files_remove_empty_folders_and_files(tmp_path):
    ledger_folder = tmp_path / "ledger"
    (ledger_folder / "empty_folder").mkdir(parents=True, exist_ok=True)
    (ledger_folder / "empty_folder/empty_subfolder").mkdir(parents=True, exist_ok=True)
    empty_file1 = ledger_folder / "empty_file1.csv"
    empty_file2 = ledger_folder / "empty_folder/empty_file2.csv"
    empty_file1.touch()
    empty_file2.touch()

    ledger = TextLedger(tmp_path)
    ledger.save_files(pd.DataFrame({}), "ledger")
    assert not (ledger_folder / "empty_folder/empty_subfolder").exists(), (
        "The 'empty_subfolder' should be removed."
    )
    assert not (ledger_folder / "empty_folder").exists(), "The 'empty_folder' should be removed."
    assert not empty_file1.exists(), "The 'empty_file1.csv' should be removed."
    assert not empty_file2.exists(), "The 'empty_file2.csv' should be removed."
    assert (ledger_folder / "default.csv").exists, "Should create default csv file"
