"""Defines ExternalLedger, a base class for connectors to external accounting systems."""

from pyledger.ledger_engine import LedgerEngine


class ExternalLedger(LedgerEngine):
    """Base class for ledger systems that interface with external accounting systems.

    This class serves as a placeholder in the class hierarchy, representing
    ledger systems that delegate storage and accounting operations to external
    systems or services. It does not introduce any new properties or methods
    beyond those inherited from `LedgerEngine`.
    """
