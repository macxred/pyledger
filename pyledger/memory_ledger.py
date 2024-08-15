"""
This module defines the MemoryLedger class, which extends StandaloneLedger to create
a non-persistent ledger system. The MemoryLedger class stores accounting data as in-memory
DataFrame objects, making it ideal for scenarios where data persistence is unnecessary.
It is particularly useful for demonstrations and testing.
"""


import pandas as pd
from .standalone_ledger import StandaloneLedger


class MemoryLedger(StandaloneLedger):
    """An implementation of the StandaloneLedger class that operates as a non-persistent ledger system.
    This class works with data stored as in-memory DataFrame objects, using hard-coded settings.
    It is particularly useful for demonstration purposes and testing scenarios.

    Usage example:
        from pyledger import MemoryLedger
        ledger = MemoryLedger()
        add_ledger_entry("")
    """

    SETTINGS = {
        "base_currency": "CHF",
        "precision": {
            "CAD": 0.01,
            "CHF": 0.01,
            "EUR": 0.01,
            "GBP": 0.01,
            "HKD": 0.01,
            "USD": 0.01,
        },
    }

    def __init__(self) -> None:
        """Initialize the MemoryLedger with hard-coded settings"""
        super().__init__(
            settings=self.SETTINGS,
            accounts=None,
        )
        self._ledger = self.standardize_ledger(None)
        self._prices = self.standardize_prices(None)
        self._vat_codes = self.standardize_vat_codes(None)
