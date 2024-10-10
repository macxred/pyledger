"""Test suite for TextLedger ledger operations."""

import pytest
from .base_test_ledger import BaseTestLedger
from pyledger import TextLedger
from pathlib import Path


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self):
        return TextLedger(root_path=Path.cwd() / "system")
