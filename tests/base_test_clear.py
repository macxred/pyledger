"""This module provides an abstract base class for clearing ledger system.
It defines common test case that can be inherited and used by specific
ledger implementations. The actual ledger implementation must be provided
by subclasses through the abstract ledger fixture.
"""

import pytest
from abc import ABC


class BaseTestClear(ABC):

    @pytest.fixture
    def temp_ledger_dir(self, tmp_path):
        """Fixture that creates a temporary directory for ledger ZIP files."""
        temp_dir = tmp_path / "ledger_data"
        temp_dir.mkdir()
        zip_path = temp_dir / "ledger.zip"
        yield zip_path

    @pytest.fixture()
    def restore_initial_state(self, ledger, temp_ledger_dir):
        ledger.dump_to_zip(temp_ledger_dir)

        yield

        ledger.restore_from_zip(temp_ledger_dir)

    def test_restore(self, ledger, restore_initial_state):
        ledger.clear()
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"
        assert ledger.base_currency is None, "base_currency was not cleared"
        # TODO: Expand test logic to test price history, precision settings,
        # and FX adjustments when implemented
