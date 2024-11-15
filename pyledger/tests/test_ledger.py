"""Test suite for ledger operations."""

import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import MemoryLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def pristine_engine(self):
        return MemoryLedger()

    def test_ledger_accessor_mutators(self, pristine_engine):
        super().test_ledger_accessor_mutators(pristine_engine, ignore_row_order=True)
