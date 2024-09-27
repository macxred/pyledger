"""Test suite for VAT code operations"""

import pytest
from .base_vat_codes import BaseTestVatCode
from pyledger import MemoryLedger


class TestVatCodes(BaseTestVatCode):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
