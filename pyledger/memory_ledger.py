"""This module implements the MemoryLedger class."""

import datetime
import pandas as pd
from typing import List
from .standalone_ledger import StandaloneLedger
from .storage_entity import DataFrameEntity
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
        self._ledger = self.standardize_ledger(None)
        self._accounts = DataFrameEntity(ACCOUNT_SCHEMA)
        self._assets = DataFrameEntity(ASSETS_SCHEMA)
        self._price_history = DataFrameEntity(PRICE_SCHEMA)
        self._revaluations = DataFrameEntity(FX_ADJUSTMENT_SCHEMA)
        self._tax_codes = DataFrameEntity(TAX_CODE_SCHEMA)

    # ----------------------------------------------------------------------
    # Ledger

    def ledger(self) -> pd.DataFrame:
        return self.standardize_ledger(self._ledger.copy())

    def ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("ledger_entry is not implemented yet.")

    def add_ledger_entry(self, entry: pd.DataFrame) -> int:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        id = entry["id"].iat[0]
        if (self._ledger["id"] == id).any():
            raise ValueError(f"Ledger entry with id '{id}' already exists.")

        self._ledger = pd.concat([self._ledger, entry])
        return id

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        ledger_id = str(entry["id"].iat[0])
        if ledger_id not in self._ledger["id"].values:
            raise ValueError(f"Ledger entry with id '{ledger_id}' not found.")

        self._ledger = self.standardize_ledger(pd.concat(
            [self._ledger[self._ledger["id"] != ledger_id], entry],
        ))

    def delete_ledger_entries(self, ids: List[str] = [], allow_missing: bool = False) -> None:
        if not allow_missing:
            missing_ids = set(ids) - set(self._ledger["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        self._ledger = self._ledger[~self._ledger["id"].isin(ids)]

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self._reporting_currency = currency
