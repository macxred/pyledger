"""This module contains the test suite for Ledger entries operations."""

import pytest
from tests.base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
