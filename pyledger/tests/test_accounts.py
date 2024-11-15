"""Test suite for accounts operations."""

import pytest
from .base_test_accounts import BaseTestAccounts
from pyledger import MemoryLedger


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
