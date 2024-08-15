"""
This module defines the MemoryLedger class, which extends StandaloneLedger to create
a non-persistent ledger system. The MemoryLedger class stores accounting data as in-memory
DataFrame objects, making it ideal for scenarios where data persistence is unnecessary.
It is particularly useful for demonstrations and testing.
"""

import pandas as pd
from .standalone_ledger import StandaloneLedger


class MemoryLedger(StandaloneLedger):
    """An implementation of the StandaloneLedger class that operates as a non-persistent
    ledger system. This class works with data stored as in-memory DataFrame objects,
    using hard-coded settings. It is particularly useful for demonstration purposes
    and testing scenarios.

    Usage example:
        from pyledger import MemoryLedger
        ledger = MemoryLedger()
        add_ledger_entry("")
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
        return self._vat_codes

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
                                  Defaults to "".
        """
        if code in self._vat_codes.index.values:
            raise ValueError(f"VAT code '{code}' already exists in the local ledger.")

        new_vat_code = StandaloneLedger.standardize_vat_codes(
            pd.DataFrame({
                "id": [code],
                "text": [text],
                "account": [account],
                "rate": [rate],
                "inclusive": [inclusive],
            })
        )
        self._vat_codes = pd.concat([self._vat_codes, new_vat_code])

    def update_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        """Updates an existing VAT code in the memory with new parameters.

        Args:
            code (str): The VAT code to be updated.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool, optional): Determines whether the VAT is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            text (str, optional): Additional text or description associated with the VAT code.
                                  Defaults to "".
        """
        if code not in self._vat_codes.index:
            raise ValueError(f"VAT code '{code}' not found.")

        self._vat_codes.loc[code] = {
            "rate": rate,
            "account": account,
            "inclusive": inclusive,
            "text": text,
        }

    def delete_vat_code(self, code: str, allow_missing: bool = False) -> None:
        """Deletes a VAT code from the memory.

        Args:
            code (str): The VAT code name to be deleted.
            allow_missing (bool, optional): If True, no error is raised if the VAT code is not
                                            found; if False, raises ValueError. Defaults to False.
        """
        if code in self._vat_codes.index.values:
            self._vat_codes = self._vat_codes[self._vat_codes.index != code]
        elif not allow_missing:
            raise ValueError(f"VAT code '{code}' not found.")

    # ----------------------------------------------------------------------
    # Accounts

    def account_chart(self) -> pd.DataFrame:
        """Retrieves the local account chart.

        Returns:
            pd.DataFrame: A DataFrame with the account chart in pyledger format,
                          including the 'account' as a column with its index values.
        """
        return self._account_chart.reset_index().rename(columns={"index": "account"})

    def add_account(
        self,
        account: str,
        currency: str,
        text: str,
        group: str,
        vat_code: str = None,
    ) -> None:
        """Adds a new account to the local account chart.

        Args:
            account (str): The account number or identifier to be added (used as the index).
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """
        if account in self._account_chart.index:
            raise ValueError(f"Account '{account}' already exists in the local ledger.")

        new_account = pd.DataFrame(
            {
                "currency": [currency],
                "text": [text],
                "vat_code": [vat_code],
                "group": [group],
            },
            index=[account],
        )
        self._account_chart = pd.concat([self._account_chart, new_account])

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
            account (str): The account number or identifier to be updated (used as the index).
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """
        if account not in self._account_chart.index:
            raise ValueError(f"Account '{account}' not found in the local ledger.")

        self._account_chart.loc[account] = {
            "currency": currency,
            "text": text,
            "vat_code": vat_code,
            "group": group,
        }

    def delete_account(self, account: str, allow_missing: bool = False) -> None:
        """Deletes an account from the local account chart.

        Args:
            account (str): The account number to be deleted (used as the index).
            allow_missing (bool, optional): If True, do not raise an error if the
                                            account is missing. Defaults to False.
        """
        if account not in self._account_chart.index:
            if allow_missing:
                return
            else:
                raise ValueError(f"Account '{account}' not found in the local ledger.")

        self._account_chart = self._account_chart.drop(account)
