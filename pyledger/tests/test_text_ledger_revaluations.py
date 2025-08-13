"""Test suite for TextLedger revaluation operations."""

import pandas as pd
import pytest
from .base_test_revaluation import BaseTestRevaluations
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestRevaluations(BaseTestRevaluations):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    def test_revaluations_list_include_source(self, engine):
        engine.revaluations.mirror(self.REVALUATIONS)
        expected = engine.revaluations.standardize(self.REVALUATIONS.copy())
        source_column = [
            "revaluations.csv:L#2", "revaluations.csv:L#3",
            "revaluations.csv:L#4", "revaluations.csv:L#5"
        ]
        expected["source"] = pd.Series(source_column, dtype="string[python]")
        assert_frame_equal(
            expected, engine.revaluations.list(include_source=True),
            check_like=True, ignore_row_order=True
        )
