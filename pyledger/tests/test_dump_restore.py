"""Test suite for testing dump, restore, and clear operations."""

import pytest
from .base_test_dump_restore_clear import BaseTestDumpRestoreClear
from pyledger import MemoryLedger


class TestDumpAndRestore(BaseTestDumpRestoreClear):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
