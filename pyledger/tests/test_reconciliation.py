"""Test suite for reconciliation operations."""

import pytest
from .base_test_reconciliation import BaseTestReconciliation
from pyledger import MemoryLedger


class TestReconciliation(BaseTestReconciliation):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
