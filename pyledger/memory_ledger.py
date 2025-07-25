"""This module implements the MemoryLedger class."""

from .standalone_ledger import StandaloneLedger
from .storage_entity import DataFrameEntity, JournalDataFrameEntity
from .constants import (
    ASSETS_SCHEMA,
    PROFIT_CENTER_SCHEMA,
    PRICE_SCHEMA,
    JOURNAL_SCHEMA,
    ACCOUNT_SCHEMA,
    RECONCILIATION_SCHEMA,
    TARGET_BALANCE_SCHEMA,
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
        self._assets = DataFrameEntity(
            ASSETS_SCHEMA,
        )

        def _clear_account_caches():
            self.serialized_ledger.cache_clear()
            self.account_currency.cache_clear()
        self._accounts = DataFrameEntity(
            ACCOUNT_SCHEMA,
            on_change=_clear_account_caches
        )
        self._tax_codes = DataFrameEntity(
            TAX_CODE_SCHEMA,
            on_change=self.serialized_ledger.cache_clear
        )
        self._price_history = DataFrameEntity(
            PRICE_SCHEMA,
            on_change=self.price.cache_clear
        )
        self._revaluations = DataFrameEntity(REVALUATION_SCHEMA)
        self._journal = JournalDataFrameEntity(
            JOURNAL_SCHEMA,
            prepare_for_mirroring=self.sanitize_journal,
            on_change=self.serialized_ledger.cache_clear
        )
        self._profit_centers = DataFrameEntity(PROFIT_CENTER_SCHEMA)
        self._reconciliation = DataFrameEntity(RECONCILIATION_SCHEMA)
        self._target_balance = DataFrameEntity(TARGET_BALANCE_SCHEMA)

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self._reporting_currency = currency
