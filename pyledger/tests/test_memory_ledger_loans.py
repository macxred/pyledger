"""Test suite for MemoryLedger loan operations."""

import pytest
from .base_test_loans import BaseTestLoans
from pyledger import MemoryLedger


class TestLoans(BaseTestLoans):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
