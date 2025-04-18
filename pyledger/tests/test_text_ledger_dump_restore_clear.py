"""Test suite for testing TextLedger dump, restore, and clear operations."""

import pytest
from .base_test_standalone_ledger_dump_restore_clear import (
    BaseTestStandaloneLedgerDumpRestoreClear
)
from pyledger import TextLedger


class TestDumpAndRestore(BaseTestStandaloneLedgerDumpRestoreClear):

    @pytest.fixture
    def engine(self, tmp_path):
        self.RECONCILIATION["source"] = "default.csv"
        self.RECONCILIATION["source"] = self.RECONCILIATION["source"].astype("string[python]")
        return TextLedger(tmp_path)
