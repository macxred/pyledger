"""This module provides an abstract base class for clearing ledger system.
It defines common test case that can be inherited and used by specific
ledger implementations. The actual ledger implementation must be provided
by subclasses through the abstract ledger fixture.
"""

import pytest
from abc import ABC


class BaseTestClear(ABC):

    @pytest.fixture()
    def restore_initial_state(self, ledger, tmp_path):
        ledger.dump_to_zip(tmp_path / "initial_ledger.zip")

        yield

        ledger.restore_from_zip(tmp_path / "initial_ledger.zip")

    def test_restore(self, ledger, restore_initial_state):
        ledger.clear()
        assert ledger.ledger().empty, "Ledger was not cleared"
        assert ledger.vat_codes().empty, "VAT codes were not cleared"
        assert ledger.account_chart().empty, "Account chart was not cleared"
        # TODO: Expand test logic to test price history, precision settings,
        # and FX adjustments when implemented
