"""Test suite for TextLedger cost center operations."""

import pytest
from .base_test_cost_centers import BaseTestCostCenters
from pyledger import TextLedger


class TestCostCenters(BaseTestCostCenters):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
