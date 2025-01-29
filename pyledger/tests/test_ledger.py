"""Test suite for journal operations."""

import pytest
from .base_test_ledger import BaseTestJournal
from pyledger import MemoryLedger


class TestLedger(BaseTestJournal):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()

    def test_journal_accessor_mutators(self, engine):
        super().test_journal_accessor_mutators(engine, ignore_row_order=True)
