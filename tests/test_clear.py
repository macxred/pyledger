"""This module contains the test suite for clearing ledger system using the MemoryLedger
implementation. It inherits the common clear test from the BaseTestClear abstract base class.
"""

import pytest
from tests.base_test_clear import BaseTestClear
from pyledger import MemoryLedger


class TestClear(BaseTestClear):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
