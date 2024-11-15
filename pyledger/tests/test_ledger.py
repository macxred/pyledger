"""Test suite for ledger operations."""

import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()

    def test_ledger_accessor_mutators(self, ledger):
        super().test_ledger_accessor_mutators(ledger, ignore_row_order=True)
