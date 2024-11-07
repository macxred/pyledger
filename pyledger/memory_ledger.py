"""This module implements the MemoryLedger class."""

from .standalone_ledger import StandaloneLedger
from .storage_entity import DataFrameEntity, LedgerDataFrameEntity
from .constants import (
    ASSETS_SCHEMA,
    PRICE_SCHEMA,
    LEDGER_SCHEMA,
    ACCOUNT_SCHEMA,
    TAX_CODE_SCHEMA,
    FX_ADJUSTMENT_SCHEMA,
)


class MemoryLedger(StandaloneLedger):
    """
    MemoryLedger is a full-featured but non-persistent implementation of the
    abstract pyledger interface. It stores accounting data as DataFrames in
    memory and is particularly useful for demonstration and testing purposes.
    """

    _reporting_currency = None

    def __init__(self, reporting_currency: str = "USD") -> None:
        """Initialize the MemoryLedger and sets the reporting currency.

        Args:
            reporting_currency (str): The reporting currency. Defaults to "USD".
        """
        super().__init__()
        self._reporting_currency = reporting_currency
        self._assets = DataFrameEntity(ASSETS_SCHEMA)
        self._accounts = DataFrameEntity(ACCOUNT_SCHEMA)
        self._tax_codes = DataFrameEntity(TAX_CODE_SCHEMA)
        self._price_history = DataFrameEntity(PRICE_SCHEMA)
        self._revaluations = DataFrameEntity(FX_ADJUSTMENT_SCHEMA)
        self._ledger = LedgerDataFrameEntity(LEDGER_SCHEMA,
                                             prepare_for_mirroring=self.sanitize_ledger)

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self._reporting_currency = currency
