"""This module Defines PersistentLedger, which extends StandaloneLedger to manage
persistent ledger data storages"""

from pyledger.standalone_ledger import StandaloneLedger


# TODO: replace with default constants
SETTINGS = {
    "reporting_currency": "CHF",
    "precision": {
        "CAD": 0.01,
        "CHF": 0.01,
        "EUR": 0.01,
        "GBP": 0.01,
        "HKD": 0.01,
        "USD": 0.01,
    },
}


class PersistentLedger(StandaloneLedger):
    """The PersistentLedger extends StandaloneLedger to manage the persistent storage of ledger
    data, ensuring that data is not lost between application runs.

    Subclasses of PersistentLedger can integrate with various storage backends, such as
    text files, databases, or other persistent storage solutions. The PersistentLedger
    class itself does not implement any specific storage mechanism but serves as a
    foundation for such systems.
    """

    def __init__(self):
        """Initialize the PersistentLedger with default settings."""
        super().__init__(settings=SETTINGS)
