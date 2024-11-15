"""Test suite for assets operations."""

import pytest
from .base_test_assets import BaseTestAssets
from pyledger import MemoryLedger


class TestAssets(BaseTestAssets):

    @pytest.fixture
    def engine(self):
        return MemoryLedger()
