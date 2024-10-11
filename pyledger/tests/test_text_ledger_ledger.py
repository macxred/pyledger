"""Test suite for TextLedger ledger operations."""

import time
import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import TextLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path / "system")

    def test_ledger_cache_invalidation(self, ledger):
        assert not ledger._is_expired(ledger._ledger_cache_time)
        ledger._invalidate_ledger_cache()
        assert ledger._is_expired(ledger._ledger_cache_time)

    def test_ledger_cache_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path / "system", cache_timeout=1)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_cache_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._ledger_cache_time)
