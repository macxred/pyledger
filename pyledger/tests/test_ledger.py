"""Test suite for ledger operations."""

import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()

    def test_ledger_accessor_mutators(self, engine):
        super().test_ledger_accessor_mutators(engine, ignore_row_order=True)
