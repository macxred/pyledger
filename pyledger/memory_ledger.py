"""This module implements the MemoryLedger class."""

import pandas as pd
from typing import List, Union
from .standalone_ledger import StandaloneLedger
from .constants import CURRENCY_PRECISION


class MemoryLedger(StandaloneLedger):
    """
    MemoryLedger is a full-featured but non-persistent implementation of the
    abstract pyledger interface. It stores accounting data as DataFrames in
    memory and is particularly useful for demonstration and testing purposes.
    """

    _base_currency = None
    _precision = CURRENCY_PRECISION

    def __init__(self, base_currency: str = "USD") -> None:
        """Initialize the MemoryLedger and sets the reporting currency.

        Args:
            base_currency (str): The reporting currency. Defaults to "USD".
        """
        self._base_currency = base_currency
        self._ledger = self.standardize_ledger(None)
        self._prices = self.standardize_prices(None)
        self._vat_codes = self.standardize_vat_codes(None)
        self._account_chart = self.standardize_account_chart(None)
        # TODO: Similarly initialise assets when concept implemented

    # ----------------------------------------------------------------------
    # VAT Codes

    def vat_codes(self) -> pd.DataFrame:
        return self.standardize_vat_codes(self._vat_codes)

    def add_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        if (self._vat_codes["id"] == code).any():
            raise ValueError(f"VAT code '{code}' already exists")

        new_vat_code = self.standardize_vat_codes(pd.DataFrame({
            "id": [code],
            "text": [text],
            "account": [account],
            "rate": [rate],
            "inclusive": [inclusive],
        }))
        self._vat_codes = pd.concat([self._vat_codes, new_vat_code])

    def modify_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        if (self._vat_codes["id"] == code).sum() != 1:
            raise ValueError(f"VAT code '{code}' not found or duplicated.")

        self._vat_codes.loc[
            self._vat_codes["id"] == code, ["rate", "account", "inclusive", "text"]
        ] = [rate, account, inclusive, text]
        self._vat_codes = self.standardize_vat_codes(self._vat_codes)

    def delete_vat_code(self, code: str, allow_missing: bool = False) -> None:
        if not allow_missing and code not in self._vat_codes["id"].values:
            raise ValueError(f"VAT code '{code}' not found in the memory.")
        self._vat_codes = self._vat_codes[self._vat_codes["id"] != code]

    # ----------------------------------------------------------------------
    # Accounts

    def account_chart(self) -> pd.DataFrame:
        return self.standardize_account_chart(self._account_chart)

    def add_account(
        self,
        account: int,
        currency: str,
        text: str,
        group: str,
        vat_code: str = None,
    ) -> None:
        if (self._account_chart["account"] == account).any():
            raise ValueError(f"Account '{account}' already exists")

        new_account = self.standardize_account_chart(pd.DataFrame({
            "account": [account],
            "currency": [currency],
            "text": [text],
            "vat_code": [vat_code],
            "group": [group],
        }))
        self._account_chart = pd.concat([self._account_chart, new_account])

    def modify_account(
        self,
        account: int,
        currency: str,
        text: str,
        group: str,
        vat_code: str = None,
    ) -> None:
        if (self._account_chart["account"] == account).sum() != 1:
            raise ValueError(f"Account '{account}' not found or duplicated.")

        self._account_chart.loc[
            self._account_chart["account"] == account,
            ["currency", "text", "vat_code", "group"]
        ] = [currency, text, vat_code, group]
        self._account_chart = self.standardize_account_chart(self._account_chart)

    def delete_accounts(
        self, accounts: Union[int, List[int]] = [], allow_missing: bool = False
    ) -> None:
        if isinstance(accounts, int):
            accounts = [accounts]

        if not allow_missing:
            missing = set(accounts) - set(self._account_chart["account"])
            if missing:
                raise KeyError(f"Account(s) '{', '.join(missing)}' not found.")

        self._account_chart = self._account_chart[~self._account_chart["account"].isin(accounts)]

    # ----------------------------------------------------------------------
    # Ledger

    def ledger(self) -> pd.DataFrame:
        return self.standardize_ledger(self._ledger)

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

    def delete_ledger_entry(self, ids: Union[str, List[str]], allow_missing: bool = False) -> None:
        if isinstance(ids, str):
            ids = [ids]

        if not allow_missing:
            missing_ids = set(ids) - set(self._ledger["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        self._ledger = self._ledger[~self._ledger["id"].isin(ids)]

    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
        # TODO: Refactor mirroring logic #25 issue
        target_df = self.standardize_ledger(target)

        if delete:
            self._ledger = target_df
        else:
            self._ledger = self.standardize_ledger(
                pd.concat([self._ledger, target_df]).drop_duplicates()
            )

    # ----------------------------------------------------------------------
    # Currency

    @property
    def base_currency(self):
        return self._base_currency

    @base_currency.setter
    def base_currency(self, currency):
        self._base_currency = currency
