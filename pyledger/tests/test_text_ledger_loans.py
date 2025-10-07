"""Test suite for TextLedger loan operations."""

import pytest
from .base_test_loans import BaseTestLoans
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestLoans(BaseTestLoans):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    def test_loans_list_include_source(self, engine):
        engine.loans.mirror(self.LOANS)
        result = engine.loans.list(include_source=True)

        # Check that source column exists and has the right format
        assert "source" in result.columns
        assert result["source"].dtype == "string[python]"

        # Check that all source values match the expected pattern
        assert all(result["source"].str.match(r"^loans\.csv:L#\d+$"))

        # Check that the data matches (excluding source column)
        result_without_source = result.drop(columns=["source"])
        expected_without_source = self.LOANS.drop(columns=["source"], errors="ignore")
        assert_frame_equal(
            expected_without_source, result_without_source,
            check_like=True, ignore_row_order=True
        )
