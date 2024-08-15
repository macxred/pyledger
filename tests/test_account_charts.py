"""This module contains the test suite for account chart operations using the MemoryLedger
implementation. It inherits the common account chart tests from the BaseTestAccountCharts
abstract base class.
"""

import pytest
from tests.base_test_account_chart import BaseTestAccountCharts
from pyledger import MemoryLedger


class TestVatCodes(BaseTestAccountCharts):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
