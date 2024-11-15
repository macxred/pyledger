"""Test suite for TextLedger tax code operations"""

import pytest
from .base_test_tax_codes import BaseTestTaxCodes
from pyledger import TextLedger


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
