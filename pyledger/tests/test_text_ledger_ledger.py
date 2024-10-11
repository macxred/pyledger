"""Test suite for TextLedger ledger operations."""

import time
import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import TextLedger
from pathlib import Path


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return TextLedger(root_path=Path.cwd() / "system")

    def test_ledger_cache_invalidation(self, ledger):
        assert not ledger._is_expired(ledger._ledger_cache_time)
        ledger._invalidate_ledger_cache()
        assert ledger._is_expired(ledger._ledger_cache_time)

    def test_ledger_cache_timeout(self):
        ledger = TextLedger(root_path=Path.cwd() / "system", cache_timeout=1)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_cache_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._ledger_cache_time)
