"""This module implements the MemoryLedger class."""

import pandas as pd
from .standalone_ledger import StandaloneLedger


class MemoryLedger(StandaloneLedger):
    """
    MemoryLedger is a full-featured but non-persistent implementation of the
    abstract pyledger interface. It stores accounting data as DataFrames in
    memory and is particularly useful for demonstration and testing purposes.
    """

    PRECISION = {
        "AUD": 0.01,
        "CAD": 0.01,
        "CHF": 0.01,
        "EUR": 0.01,
        "GBP": 0.01,
        "JPY": 1.00,
        "NZD": 0.01,
        "NOK": 0.01,
        "SEK": 0.01,
        "USD": 0.01,
    }

    def __init__(self, base_currency: str = "USD") -> None:
        """Initialize the MemoryLedger and sets the reporting currency.

        Args:
            base_currency (str): The reporting currency. Defaults to "USD".
        """
        settings = {
            "precision": self.PRECISION,
            "base_currency": base_currency,
        }
        super().__init__(
            settings=settings,
            accounts=None,
        )
        self._ledger = self.standardize_ledger(None)
        self._prices = self.standardize_prices(None)
        self._vat_codes = self.standardize_vat_codes(None)

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
        if self._vat_codes["id"].isin([code]).any():
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
        if not self._vat_codes["id"].isin([code]).sum() == 1:
            raise ValueError(f"VAT code '{code}' not found (or duplicated).")

        self._vat_codes.loc[
            self._vat_codes["id"] == code, ["rate", "account", "inclusive", "text"]
        ] = [rate, account, inclusive, text]
        self._vat_codes = self.standardize_vat_codes(self._vat_codes)

    def delete_vat_code(self, code: str, allow_missing: bool = False) -> None:
        if not allow_missing and code not in self._vat_codes["id"].values:
            raise ValueError(f"VAT code '{code}' not found in the memory.")
        self._vat_codes = self._vat_codes[self._vat_codes["id"] != code]

    def mirror_vat_codes(self, target: pd.DataFrame, delete: bool = False):
        target_df = self.standardize_vat_codes(target)

        if target_df["id"].duplicated().any():
            raise ValueError("Duplicate VAT ids found in `target`.")

        if delete:
            self._vat_codes = target_df
        else:
            self._vat_codes = self.standardize_vat_codes(
                pd.concat([self._vat_codes, target_df])
                .drop_duplicates(subset=["id"], keep="last")
            )

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
        if self._account_chart["account"].isin([account]).any():
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
        if not self._account_chart["account"].isin([account]).sum() == 1:
            raise ValueError(f"Account '{account}' not found or have duplicates in the system.")

        self._account_chart.loc[
            self._account_chart["account"] == account,
            ["currency", "text", "vat_code", "group"]
        ] = [currency, text, vat_code, group]
        self._account_chart = self.standardize_account_chart(self._account_chart)

    def delete_account(self, account: str, allow_missing: bool = False) -> None:
        if not allow_missing and account not in self._account_chart["account"].values:
            raise KeyError(f"Account '{account}' not found in the account chart.")
        self._account_chart = self._account_chart[self._account_chart["account"] != account]

    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = False):
        target_df = self.standardize_account_chart(target)

        if target_df["account"].duplicated().any():
            raise ValueError("Duplicate accounts found in `target`.")

        if delete:
            self._account_chart = target_df
        else:
            self._account_chart = self.standardize_account_chart(
                pd.concat([self._account_chart, target_df])
                .drop_duplicates(subset=["account"], keep="last")
            )

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

        ledger_id = entry["id"].iat[0]
        if entry["id"].iat[0] in self._ledger["id"].values:
            raise ValueError(f"Ledger entry with id '{ledger_id}' already exists.")

        self._ledger = pd.concat([self._ledger, entry])
        return ledger_id

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

    def delete_ledger_entry(self, id: str) -> None:
        if id in self._ledger["id"].values:
            self._ledger = self._ledger[self._ledger["id"] != id]

    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
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
        return self._settings["base_currency"]

    @base_currency.setter
    def base_currency(self, currency):
        self._settings["base_currency"] = currency
