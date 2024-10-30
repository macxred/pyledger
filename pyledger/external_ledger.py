"""This module defines ExternalLedger, which extends LedgerEngine to interface
with external accounting systems."""

from pyledger.ledger_engine import LedgerEngine


class ExternalLedger(LedgerEngine):
    """ExternalLedger is designed to connect and synchronize with third-party accounting systems
    (such as CashCtrl or Proffix). It doesn't manage data directly but acts as a bridge between
    PyLedger's internal framework and external systems, ensuring consistent communication and data
    exchange. ExternalLedger subclasses implement the specific logic needed to interact with the
    chosen external system.

    This class provides a foundation for developing ledgers that rely on external data sources or
    accounting platforms without implementing any specific integration on its own.
    """

    def __init__(self):
        """Initialize the ExternalLedger."""
        super().__init__()
