"""This module contains the test suite for VAT code operations using the MemoryLedger
implementation. It inherits the common VAT code tests from the BaseTestVatCode abstract base
class.
"""

import pytest
from tests.base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
