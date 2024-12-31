"""Test suite for profit center operations."""

import pytest
from .base_test_profit_centers import BaseTestProfitCenters
from pyledger import MemoryLedger


class TestProfitCenters(BaseTestProfitCenters):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
