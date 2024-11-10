"""Test suite for TextLedger assets operations."""

import pytest
from .base_test_assets import BaseTestAssets
from pyledger import TextLedger


@pytest.mark.skip(reason="Delegation for accounting entities not yet implemented in TextLedger.")
class TestAssets(BaseTestAssets):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)
