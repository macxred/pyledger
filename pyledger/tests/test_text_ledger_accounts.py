"""Test suite for TextLedger accounts operations."""

import pandas as pd
import pytest
from .base_test_accounts import BaseTestAccounts
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    def test_extra_columns(self, engine):
        extra_cols = ["new_column", "second_new_column"]
        accounts = self.ACCOUNTS.head(3).copy()
        engine.accounts.add(accounts)

        # Add accounts with a new column
        expected = self.ACCOUNTS.tail(-3).copy()
        expected[extra_cols[0]] = "test value"
        engine.accounts.add(expected)
        current = engine.accounts.list()
        assert current.query("account in @accounts['account']")[extra_cols[0]].isna().all(), (
            "Pre existing accounts should have new column with all NA values"
        )
        assert_frame_equal(current, pd.concat([accounts, expected]), check_like=True)

        # Modify accounts with a new column
        expected = engine.accounts.list()
        expected[extra_cols[1]] = "second test value"
        engine.accounts.modify(expected)
        assert_frame_equal(
            engine.accounts.list(), expected, check_like=True,
        )

        # Standardize() method with drop_extra_columns=True should drop extra columns
        expected_without_extra_cols = expected.copy().drop(columns=extra_cols)
        assert_frame_equal(
            engine.accounts.standardize(expected, drop_extra_columns=True),
            expected_without_extra_cols, check_like=True
        )

        # List() method with drop_extra_columns=True should drop extra columns
        assert_frame_equal(
            engine.accounts.list(drop_extra_columns=True), expected_without_extra_cols,
            check_like=True
        )
