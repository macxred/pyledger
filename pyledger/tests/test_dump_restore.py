"""Test suite for testing dump, restore, and clear operations."""

import pytest
from .base_test_standalone_ledger_dump_restore_clear import (
    BaseTestStandaloneLedgerDumpRestoreClear
)
from pyledger import MemoryLedger


class TestDumpAndRestore(BaseTestStandaloneLedgerDumpRestoreClear):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
