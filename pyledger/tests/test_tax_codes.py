"""Test suite for tax code operations"""

import pytest
from .base_tax_codes import BaseTestTaxCodes
from pyledger import MemoryLedger


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
