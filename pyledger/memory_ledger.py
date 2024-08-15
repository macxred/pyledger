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
