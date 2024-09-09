"""This module implements the MemoryLedger class."""

import pandas as pd
from .standalone_ledger import StandaloneLedger
import zipfile


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
        """Initialize the MemoryLedger with hard-coded settings

        Args:
            base_currency (str): The base currency for the system. Defaults to "USD".
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
    # File Operations

    def restore(self, archive_path):
        """Restores the ledger, account chart, and VAT codes from a specified archive.

        This method extracts data from a zip archive and loads it into the ledger, account chart,
        and VAT codes. After the data is loaded, the extracted CSV files are removed to clean up
        the environment.

        Args:
            archive_path (str): The file path to the zip archive containing the ledger,
                                account chart, and VAT codes CSV files.

        Raises:
            FileNotFoundError: If the archive or any of the expected CSV files are not found.
        """
        required_files = {'ledger.csv', 'vat_codes.csv', 'accounts.csv'}

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            self._ledger = self.standardize_ledger(pd.read_csv(archive.open('ledger.csv')))
            self._vat_codes = self.standardize_vat_codes(pd.read_csv(archive.open('vat_codes.csv')))
            self._account_chart = self.standardize_account_chart(
                pd.read_csv(archive.open('accounts.csv'))
            )

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
        if not self._vat_codes["id"].isin([code]).any():
            raise ValueError(f"VAT code '{code}' not found.")

        self._vat_codes.loc[
            self._vat_codes["id"] == code, ["rate", "account", "inclusive", "text"]
        ] = [rate, account, inclusive, text]

        self._vat_codes = self.standardize_vat_codes(self._vat_codes)

    def delete_vat_code(self, code: str, allow_missing: bool = False) -> None:
        if not allow_missing and code not in self._vat_codes["id"].values:
            raise ValueError(f"VAT code '{code}' not found in the memory.")

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

        if target_df["id"].duplicated().any():
            raise ValueError("Duplicate VAT codes 'id' values found")

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
        if not self._account_chart["account"].isin([account]).any():
            raise ValueError(f"Account '{account}' not found")

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
        """Synchronizes the account chart with a desired target state provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes accounts on the remote that are not
                                     present in the target DataFrame.
        Raises:
            ValueError: If duplicate account charts are found in the target DataFrame.
        """
        target_df = self.standardize_account_chart(target)

        if target_df["account"].duplicated().any():
            raise ValueError("Duplicate account charts found")

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
