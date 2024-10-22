"""Test suite for TextLedger accounts operations."""

import pytest
from .base_accounts import BaseTestAccounts
from pyledger import TextLedger


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)
