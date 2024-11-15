"""Test suite for TextLedger assets operations."""

import pytest
from .base_test_assets import BaseTestAssets
from pyledger import TextLedger


class TestAssets(BaseTestAssets):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)
