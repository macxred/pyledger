"""Test suite for TextLedger settings"""

import time
import pytest
from pyledger import TextLedger


@pytest.fixture
def ledger(tmp_path):
    return TextLedger(tmp_path)


def test_settings_invalidation(ledger):
    assert ledger._is_expired(ledger._settings_time)
    ledger.settings
    assert not ledger._is_expired(ledger._settings_time)
    ledger._invalidate_settings()
    assert ledger._is_expired(ledger._settings_time)


def test_settings_timeout(tmp_path):
    ledger = TextLedger(tmp_path, cache_timeout=1)
    ledger.settings
    assert not ledger._is_expired(ledger._settings_time)
    time.sleep(1)
    assert ledger._is_expired(ledger._settings_time)
