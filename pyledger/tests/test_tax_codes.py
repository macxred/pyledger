"""Test suite for tax code operations"""

import pytest
from .base_test_tax_codes import BaseTestTaxCodes
from pyledger import MemoryLedger


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
