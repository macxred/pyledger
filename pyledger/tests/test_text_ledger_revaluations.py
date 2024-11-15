"""Test suite for TextLedger revaluation operations."""

import pytest
from .base_test_revaluation import BaseTestRevaluations
from pyledger import TextLedger


class TestRevaluations(BaseTestRevaluations):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
