"""Test suite for cost center operations."""

import pytest
from .base_test_cost_centers import BaseTestCostCenters
from pyledger import MemoryLedger


class TestCostCenters(BaseTestCostCenters):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
