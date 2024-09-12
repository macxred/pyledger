"""Test suite for account chart operations"""

import pytest
from tests.base_account_chart import BaseTestAccountCharts
from pyledger import MemoryLedger


class TestVatCodes(BaseTestAccountCharts):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
