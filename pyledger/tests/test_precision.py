"""Test suite for precision()."""

import pytest
from .base_test_precision import BaseTestPrecision
from pyledger import MemoryLedger


class TestAssets(BaseTestPrecision):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
