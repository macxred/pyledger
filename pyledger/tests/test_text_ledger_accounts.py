"""Test suite for TextLedger accounts operations."""

import time
import pytest
from .base_accounts import BaseTestAccounts
from pyledger import TextLedger


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_accounts_invalidation(self, ledger):
        assert ledger._is_expired(ledger._accounts_time)
        ledger.accounts()
        assert not ledger._is_expired(ledger._accounts_time)
        ledger._invalidate_accounts()
        assert ledger._is_expired(ledger._accounts_time)

    def test_accounts_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path, cache_timeout=1)
        ledger.accounts()
        assert not ledger._is_expired(ledger._accounts_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._accounts_time)
