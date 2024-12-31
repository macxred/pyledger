"""Test suite for TextLedger profit center operations."""

import pytest
from .base_test_profit_centers import BaseTestProfitCenters
from pyledger import TextLedger


class TestProfitCenters(BaseTestProfitCenters):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
