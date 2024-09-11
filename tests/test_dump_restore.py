"""This module contains the test suite for dump and restore operations using the MemoryLedger
implementation. It inherits the common account chart tests from the BaseTestDumpAndRestore
abstract base class.
"""

import pytest
from tests.base_test_dump_restore import BaseTestDumpAndRestore
from pyledger import MemoryLedger


class TestDumpAndRestore(BaseTestDumpAndRestore):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()
