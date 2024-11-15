"""Test suite for TextLedger price history operations."""

import pytest
from .base_test_price_history import BaseTestPriceHistory
from pyledger import TextLedger


class TestPriceHistory(BaseTestPriceHistory):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
