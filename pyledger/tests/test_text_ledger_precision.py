"""Test suite for TextLedger precision()."""

import pytest
from .base_test_precision import BaseTestPrecision
from pyledger import TextLedger


class TestAssets(BaseTestPrecision):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)
