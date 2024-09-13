"""Test suite for account chart operations"""

import pytest
from tests.base_accounts import BaseTestAccounts
from pyledger import MemoryLedger


class TestVatCodes(BaseTestAccounts):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
