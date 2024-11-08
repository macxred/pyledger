"""Test suite for price history operations."""

import pytest
from .base_test_price_history import BaseTestPriceHistory
from pyledger import MemoryLedger


class TestPriceHistory(BaseTestPriceHistory):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
