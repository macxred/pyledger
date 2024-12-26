"""This module implements the MemoryLedger class."""

from .standalone_ledger import StandaloneLedger
from .storage_entity import DataFrameEntity, LedgerDataFrameEntity
from .constants import (
    ASSETS_SCHEMA,
    COST_CENTER_SCHEMA,
    PRICE_SCHEMA,
    LEDGER_SCHEMA,
    ACCOUNT_SCHEMA,
    TAX_CODE_SCHEMA,
    REVALUATION_SCHEMA,
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
        self._accounts = DataFrameEntity(
            ACCOUNT_SCHEMA,
            on_change=self.serialized_ledger.cache_clear
        )
        self._tax_codes = DataFrameEntity(
            TAX_CODE_SCHEMA,
            on_change=self.serialized_ledger.cache_clear
        )
        self._price_history = DataFrameEntity(PRICE_SCHEMA)
        self._revaluations = DataFrameEntity(REVALUATION_SCHEMA)
        self._ledger = LedgerDataFrameEntity(
            LEDGER_SCHEMA,
            prepare_for_mirroring=self.sanitize_ledger,
            on_change=self.serialized_ledger.cache_clear
        )
        self._cost_centers = DataFrameEntity(COST_CENTER_SCHEMA)

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self._reporting_currency = currency
