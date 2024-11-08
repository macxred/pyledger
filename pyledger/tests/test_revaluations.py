"""Test suite for revaluation operations."""

import pytest
from .base_test_revaluation import BaseTestRevaluations
from pyledger import MemoryLedger


class TestRevaluations(BaseTestRevaluations):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
