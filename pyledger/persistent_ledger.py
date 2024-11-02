"""Defines PersistentLedger, a base class for persistent data storage."""

from pyledger.standalone_ledger import StandaloneLedger


class PersistentLedger(StandaloneLedger):
    """Base class for ledger systems with persistent storage.

    This class serves as a placeholder in the class hierarchy, representing
    ledger systems that integrate with persistent storage backends like files
    or databases. It does not introduce any new properties or methods beyond
    those inherited from `StandaloneLedger`.
    """
