"""This module implements the MemoryLedger class."""

import pandas as pd
from .standalone_ledger import StandaloneLedger


class MemoryLedger(StandaloneLedger):
    """
    MemoryLedger is a full-featured but non-persistent implementation of the
    abstract pyledger interface. It stores accounting data as DataFrames in
    memory and is particularly useful for demonstration and testing purposes.
    """

    SETTINGS = {
        "base_currency": "CHF",
        "precision": {
            "CAD": 0.01,
            "CHF": 0.01,
            "EUR": 0.01,
            "GBP": 0.01,
            "HKD": 0.01,
            "USD": 0.01,
        },
    }

    def __init__(self) -> None:
        """Initialize the MemoryLedger with hard-coded settings"""
        super().__init__(
            settings=self.SETTINGS,
            accounts=None,
        )
        self._ledger = self.standardize_ledger(None)
        self._prices = self.standardize_prices(None)
        self._vat_codes = self.standardize_vat_codes(None)

    # ----------------------------------------------------------------------
    # VAT Codes

    def vat_codes(self) -> pd.DataFrame:
        """Retrieves VAT codes from the memory

        Returns:
            pd.DataFrame: A DataFrame with pyledger.VAT_CODE column schema.
        """
        return self.standardize_vat_codes(self._vat_codes)

    def add_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        """Adds a new VAT code to the memory.

        Args:
            code (str): The VAT code to be added.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool, optional): Determines whether the VAT is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            text (str, optional): Additional text or description associated with the VAT code.

        Raises:
            ValueError: If the VAT code already exists in the memory.
        """
        if self._vat_codes["id"].isin([code]).any():
            raise ValueError(f"VAT code '{code}' already exists")

        new_vat_code = pd.DataFrame({
            "id": [code],
            "text": [text],
            "account": [account],
            "rate": [rate],
            "inclusive": [inclusive],
        })
        self._vat_codes = self.standardize_vat_codes(
            pd.concat([self._vat_codes, new_vat_code])
        )

    def update_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        """Updates an existing VAT code with new parameters.

        Args:
            code (str): The VAT code to be updated.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool, optional): Determines whether the VAT is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            text (str, optional): Additional text or description associated with the VAT code.

        Raises:
            ValueError: If the VAT code is not found in the memory.
        """
        if not self._vat_codes["id"].isin([code]).any():
            raise ValueError(f"VAT code '{code}' not found.")

        self._vat_codes.loc[
            self._vat_codes["id"] == code,
            ["rate", "account", "inclusive", "text"],
        ] = [rate, account, inclusive, text]

    def delete_vat_code(self, code: str) -> None:
        """Deletes a VAT code from the memory.

        Args:
            code (str): The VAT code name to be deleted.
        """
        self._vat_codes = self._vat_codes[self._vat_codes["id"] != code]

    def mirror_vat_codes(self, target: pd.DataFrame, delete: bool = False):
        """Aligns VAT rates in the memory with the desired state provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame containing VAT rates in
                                         the pyledger.vat_codes format.
            delete (bool, optional): If True, deletes VAT codes on the remote account
                                     that are not present in target_state.
        """
        target_df = self.standardize_vat_codes(target)

        if delete:
            self._vat_codes = target_df
        else:
            self._vat_codes = self.standardize_vat_codes(
                pd.concat([self._vat_codes, target_df]).drop_duplicates()
            )

    # ----------------------------------------------------------------------
    # Accounts

    def account_chart(self) -> pd.DataFrame:
        """Retrieves the account chart.

        Returns:
            pd.DataFrame: A DataFrame with the account chart in pyledger format
        """
        return self.standardize_account_chart(self._account_chart)

    def add_account(
        self,
        account: str,
        currency: str,
        text: str,
        group: str,
        vat_code: str = None,
    ) -> None:
        """Adds a new account to the account chart.

        Args:
            account (str): The account number or identifier to be added.
            currency (str): The currency associated with the account.
            text (str): Description of the account.
            group (str): The category with which the account is associated.
            vat_code (str, optional): The VAT code to be applied to the account, if any.

        Raises:
            ValueError: If the `account` already exists.
        """
        if self._account_chart["account"].isin([account]).any():
            raise ValueError(f"Account '{account}' already exists")

        new_account = pd.DataFrame({
            "account": [account],
            "currency": [currency],
            "text": [text],
            "vat_code": [vat_code],
            "group": [group],
        })

        self._account_chart = self.standardize_account_chart(
            pd.concat([self._account_chart, new_account])
        )

    def modify_account(
        self,
        account: str,
        currency: str,
        text: str,
        group: str,
        vat_code: str = None,
    ) -> None:
        """Updates an existing account in the local account chart.

        Args:
            account (str): The account number or identifier to be updated.
            currency (str): The currency associated with the account.
            text (str): Description of the account.
            group (str): The category with which the account is associated.
            vat_code (str, optional): The VAT code to be applied to the account, if any.

        Raises:
            ValueError: If the `account` is not found.
        """
        if not self._account_chart["account"].isin([account]).any():
            raise ValueError(f"Account '{account}' not found")

        self._account_chart.loc[
            self._account_chart["account"] == account,
            ["currency", "text", "vat_code", "group"]
        ] = [currency, text, vat_code, group]

    def delete_account(self, account: str) -> None:
        """Remove an account from the account chart.

        Args:
            account (str): The number or identifier of the account to be deleted.
        """
        self._account_chart = self._account_chart[self._account_chart["account"] != account]

    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = False):
        """Synchronizes the account chart with a desired target state provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes accounts on the remote that are not
                                     present in the target DataFrame.
        """
        target_df = self.standardize_account_chart(target)

        if delete:
            self._account_chart = target_df
        else:
            self._account_chart = self.standardize_account_chart(
                pd.concat([self._account_chart, target_df]).drop_duplicates()
            )

    # ----------------------------------------------------------------------
    # Ledger

    def ledger(self) -> pd.DataFrame:
        """Retrieves all ledger entries.

        Returns:
            pd.DataFrame: A DataFrame with LedgerEngine.ledger() column schema.
        """
        return self.standardize_ledger(self._ledger)

    def add_ledger_entry(self, entry: pd.DataFrame) -> int:
        """Adds a new entry to the ledger.

        Args:
            entry (pd.DataFrame): DataFrame with ledger entry in pyledger schema.

        Returns:
            int: The Id of the created ledger entry.

        Raises:
            ValueError: If the entry does not have a unique 'id' across all rows.
            ValueError: If a ledger entry with the same 'id' already exists in the ledger.
        """
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        ledger_id = entry["id"].iat[0]
        if entry["id"].iat[0] in self._ledger["id"].values:
            raise ValueError(f"Ledger entry with id '{ledger_id}' already exists.")

        self._ledger = self.standardize_ledger(pd.concat([self._ledger, entry]))
        return ledger_id

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        """Modifies an existing entry in the ledger.

        Args:
            entry (pd.DataFrame): DataFrame with ledger entry in pyledger schema.

        Raises:
            ValueError: If the entry does not have a unique 'id' across all rows.
            ValueError: If the ledger entry with the specified 'id' does not exist in the ledger.
        """
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
        """Deletes an entry from the ledger.

        Args:
            id (str): The Id of the ledger entry to be deleted.
        """
        if id in self._ledger["id"].values:
            self._ledger = self._ledger[self._ledger["id"] != id]

    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
        """Synchronizes ledger entries with a desired target state provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame with ledger entries in the pyledger format.
            delete (bool, optional): If True, deletes ledger entries in the memory that are not
                                    present in the target DataFrame.
        """
        target_df = self.standardize_ledger(target)

        if delete:
            self._ledger = target_df
        else:
            self._ledger = self.standardize_ledger(
                pd.concat([self._ledger, target_df]).drop_duplicates()
            )
