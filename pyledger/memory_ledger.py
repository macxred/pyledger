"""This module implements the MemoryLedger class."""

import datetime
import pandas as pd
from typing import List
from .standalone_ledger import StandaloneLedger


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
        self._prices = self.standardize_prices(None)
        self._tax_codes = self.standardize_tax_codes(None)
        self._accounts = self.standardize_accounts(None)
        self._assets = self.standardize_assets(None)
        # TODO: Similarly initialise assets when concept implemented

    # ----------------------------------------------------------------------
    # Tax Codes

    def tax_codes(self) -> pd.DataFrame:
        return self.standardize_tax_codes(self._tax_codes.copy())

    def add_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        if (self._tax_codes["id"] == id).any():
            raise ValueError(f"Tax code '{id}' already exists")

        new_tax_code = self.standardize_tax_codes(pd.DataFrame({
            "id": [id],
            "description": [description],
            "account": [account],
            "rate": [rate],
            "is_inclusive": [is_inclusive],
        }))
        self._tax_codes = pd.concat([self._tax_codes, new_tax_code])

    def modify_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        if (self._tax_codes["id"] == id).sum() != 1:
            raise ValueError(f"Tax code '{id}' not found or duplicated.")

        self._tax_codes.loc[
            self._tax_codes["id"] == id, ["rate", "account", "is_inclusive", "description"]
        ] = [rate, account, is_inclusive, description]
        self._tax_codes = self.standardize_tax_codes(self._tax_codes)

    def delete_tax_codes(
        self, codes: List[str] = [], allow_missing: bool = False
    ) -> None:
        if not allow_missing:
            missing = set(codes) - set(self._tax_codes["id"])
            if missing:
                raise ValueError(f"Tax code(s) '{', '.join(missing)}' not found.")

        self._tax_codes = self._tax_codes[~self._tax_codes["id"].isin(codes)]

    # ----------------------------------------------------------------------
    # Accounts

    def accounts(self) -> pd.DataFrame:
        return self.standardize_accounts(self._accounts.copy())

    def add_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        if (self._accounts["account"] == account).any():
            raise ValueError(f"Account '{account}' already exists")

        new_account = self.standardize_accounts(pd.DataFrame({
            "account": [account],
            "currency": [currency],
            "description": [description],
            "tax_code": [tax_code],
            "group": [group],
        }))
        self._accounts = pd.concat([self._accounts, new_account])

    def modify_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        if (self._accounts["account"] == account).sum() != 1:
            raise ValueError(f"Account '{account}' not found or duplicated.")

        self._accounts.loc[
            self._accounts["account"] == account,
            ["currency", "description", "tax_code", "group"]
        ] = [currency, description, tax_code, group]
        self._accounts = self.standardize_accounts(self._accounts)

    def delete_accounts(
        self, accounts: List[int] = [], allow_missing: bool = False
    ) -> None:
        if not allow_missing:
            missing = set(accounts) - set(self._accounts["account"])
            if missing:
                raise KeyError(f"Account(s) '{', '.join(missing)}' not found.")

        self._accounts = self._accounts[~self._accounts["account"].isin(accounts)]

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

    # ----------------------------------------------------------------------
    # Assets

    def assets(self) -> pd.DataFrame:
        return self.standardize_assets(self._assets.copy())

    def add_asset(self, ticker: str, increment: float, date: datetime.date = None) -> pd.DataFrame:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )
        if mask.any():
            raise ValueError("The asset with this unique combination already exists.")

        asset = self.standardize_assets(pd.DataFrame({
            "ticker": [ticker],
            "date": [date],
            "increment": [increment]
        }))
        self._assets = pd.concat([assets, asset], ignore_index=True)

    def modify_asset(self, ticker: str, increment: float, date: datetime.date = None) -> None:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )
        if not mask.any():
            raise ValueError(f"Asset with ticker '{ticker}' and date '{date}' not found.")

        assets.loc[mask, "increment"] = increment
        self._assets = self.standardize_assets(assets)

    def delete_asset(
        self, ticker: str, date: datetime.date = None, allow_missing: bool = False
    ) -> None:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )

        if mask.any():
            self._assets = assets[~mask].reset_index(drop=True)
        else:
            if not allow_missing:
                raise ValueError(f"Asset with ticker '{ticker}' and date '{date}' not found.")

    # ----------------------------------------------------------------------
    # Revaluations

    def revaluations(self) -> pd.DataFrame:
        return self._revaluations.copy()

    # ----------------------------------------------------------------------
    # Price

    def add_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_price is not implemented yet.")

    def modify_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_price is not implemented yet.")

    def delete_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_price is not implemented yet.")

    def price_history(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_history is not implemented yet.")

    def price_increment(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_increment is not implemented yet.")