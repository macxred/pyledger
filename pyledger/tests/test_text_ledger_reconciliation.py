"""Test suite for TextLedger reconciliation operations."""

import pandas as pd
import pytest
from pyledger.constants import RECONCILIATION_SCHEMA
from .base_test_reconciliation import BaseTestReconciliation
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestRevaluations(BaseTestReconciliation):

    @pytest.fixture
    def engine(self, tmp_path):
        self.RECONCILIATION["source"] = "default.csv"
        self.RECONCILIATION["source"] = self.RECONCILIATION["source"].astype("string[python]")
        return TextLedger(tmp_path)

    def test_reconciliation_without_reconciliation_folder(self, engine):
        """reconciliation.list() is expected to return an empty data frame
        if the reconciliation folder is missing.
        """
        expected_reconciliation = engine.reconciliation.standardize(None)
        assert_frame_equal(engine.reconciliation.list(), expected_reconciliation)

    def test_write_empty_reconciliation_file(self, engine):
        """Ensure write_reconciliation_file() saves mandatory columns for an empty DataFrame."""
        filename = engine.root / "empty.csv"
        engine.write_reconciliation_file(None, filename)
        df = pd.read_csv(filename)
        mandatory = RECONCILIATION_SCHEMA["column"][RECONCILIATION_SCHEMA["mandatory"]].tolist()
        assert df.columns.str.strip().tolist() == mandatory, "Missing mandatory columns"

    def test_extra_columns(self, engine):
        extra_cols = ["new_column", "second_new_column"]
        entries = self.RECONCILIATION.iloc[:2].copy()
        id_columns = RECONCILIATION_SCHEMA.query("id == True")["column"].to_list()
        ids = pd.DataFrame(engine.reconciliation.add(entries))

        # Add reconciliation entries with a new column
        expected = self.RECONCILIATION.iloc[3:4].copy()
        expected[extra_cols[0]] = "test value"
        engine.reconciliation.add(expected)
        current = engine.reconciliation.list()
        assert current.merge(ids, on=id_columns, how='inner')[extra_cols[0]].isna().all(), (
            "Pre existing entries should have new column with all NA values"
        )
        assert_frame_equal(current, pd.concat([entries, expected]), ignore_index=True)

        # Modify reconciliation entries with a new column
        expected = engine.reconciliation.list()
        expected[extra_cols[1]] = "second test value"
        engine.reconciliation.modify(expected)
        assert_frame_equal(engine.reconciliation.list(), expected, ignore_index=True)

        # Standardize() method with drop_extra_columns=True should drop extra columns
        expected_without_extra_cols = expected.copy().drop(columns=extra_cols)
        assert_frame_equal(
            engine.reconciliation.standardize(expected, drop_extra_columns=True),
            expected_without_extra_cols
        )

        # List() method with drop_extra_columns=True should drop extra columns
        assert_frame_equal(
            engine.reconciliation.list(drop_extra_columns=True), expected_without_extra_cols,
        )
