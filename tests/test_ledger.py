"""This module contains the test suite for Ledger entries operations using the MemoryLedger
implementation. It inherits the common Ledger code tests from the BaseTestLedger abstract base
class.
"""

import pytest
from tests.base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
