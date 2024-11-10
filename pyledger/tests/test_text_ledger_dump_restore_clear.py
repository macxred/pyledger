"""Test suite for testing TextLedger dump, restore, and clear operations."""

import pytest
from .base_test_dump_restore_clear import BaseTestDumpRestoreClear
from pyledger import TextLedger


@pytest.mark.skip(reason="Delegation for accounting entities not yet implemented in TextLedger.")
class TestDumpAndRestore(BaseTestDumpRestoreClear):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)
