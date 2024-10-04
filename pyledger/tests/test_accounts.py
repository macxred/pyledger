"""Test suite for accounts operations."""

import pytest
from .base_accounts import BaseTestAccounts
from pyledger import MemoryLedger


class TestTaxCodes(BaseTestAccounts):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
