"""Test suite for TextLedger tax code operations"""

import time
import pytest
from .base_tax_codes import BaseTestTaxCodes
from pyledger import TextLedger


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_tax_code_invalidation(self, ledger):
        assert ledger._is_expired(ledger._tax_codes_time)
        ledger.tax_codes()
        assert not ledger._is_expired(ledger._tax_codes_time)
        ledger._invalidate_tax_codes()
        assert ledger._is_expired(ledger._tax_codes_time)

    def test_tax_code_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path, cache_timeout=1)
        ledger.tax_codes()
        assert not ledger._is_expired(ledger._tax_codes_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._tax_codes_time)
