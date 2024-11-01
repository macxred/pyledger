"""This module defines Abstract base class for a double entry accounting system."""

from abc import ABC, abstractmethod
import datetime
import logging
import math
import zipfile
import json
from pathlib import Path
from typing import Dict, List
from consistent_df import enforce_schema, df_to_consistent_str, nest, unnest
import re
import numpy as np
import openpyxl
import pandas as pd
from .constants import (
    ASSETS_SCHEMA,
    PRICE_SCHEMA,
    LEDGER_SCHEMA,
    ACCOUNT_SCHEMA,
    TAX_CODE_SCHEMA,
    REVALUATION_SCHEMA,
)
from . import excel
from .helpers import represents_integer
from .time import parse_date_span


class LedgerEngine(ABC):
    """
    Abstract base class defining the core interface for managing a double entry
    accounting system for multiple currencies and arbitrary assets, including
    account chart, and vat or sales tax management.
    """

    _logger = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self):
        self._logger = logging.getLogger("ledger")

    # ----------------------------------------------------------------------
    # Settings

    @staticmethod
    def standardize_settings(settings: dict) -> dict:
        """Validates and standardizes the 'settings' dictionary. Ensures it
        contains items 'reporting_currency' and 'precision'.

        Example:
            settings = {
                'reporting_currency': 'USD',
                'precision': {
                    'CAD': 0.01, 'CHF': 0.01, 'EUR': 0.01,
                    'GBP': 0.01, 'HKD': 0.01, 'USD': 0.01
                }
            }
            LedgerEngine.standardize_settings(settings)

        Args:
            settings (dict): The settings dictionary to be standardized.

        Returns:
            dict: The standardized settings dictionary.

        Raises:
            ValueError: If 'settings' is not a dictionary, or if 'reporting_currency'
                        is missing/not a string, or if 'precision' is missing/not a
                        dictionary, or if any key/value in 'precision' is of invalid
                        type.

        Note:
            Modifies 'precision' to include the 'reporting_currency' key.
        """
        if not isinstance(settings, dict):
            raise ValueError("'settings' must be a dict.")

        # Check for 'reporting_currency'
        if (
            "reporting_currency" not in settings
            or not isinstance(settings["reporting_currency"], str)
        ):
            raise ValueError("Missing/invalid 'reporting_currency' in settings.")

        return settings

    # ----------------------------------------------------------------------
    # Revaluation

    @classmethod
    def standardize_revaluations(cls, df: pd.DataFrame,
                                 keep_extra_columns: bool = False) -> pd.DataFrame:
        """Validates and standardizes the 'revaluations' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the revaluations.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.
        Returns:
            pd.DataFrame: The standardized revaluations DataFrame.

        Raises:
            ValueError: If required columns are missing or if data types are incorrect.
        """
        df = enforce_schema(df, REVALUATION_SCHEMA, keep_extra_columns=keep_extra_columns)
        return df

    @abstractmethod
    def revaluations(self) -> pd.DataFrame:
        """Retrieves all revaluation entries.

        Returns:
            pd.DataFrame with columns specified in REVALUATION_SCHEMA.
        """

    @abstractmethod
    def add_revaluation(
        self,
        date: datetime.date,
        account: str,
        credit: int,
        description: str,
        debit: int = None,
        price: float = None
    ) -> None:
        """Adds a new revaluation.

        The unique identifier is a combination of `date` and `account`.

        Args:
            date (datetime.date): Date of the revaluation.
            account (str): Account (e.g. "1020") or range of accounts
                          (e.g. "1020:1099") to revalue.
            credit (int, optional): Credit amount for the revaluation.
                                    If None, uses the value of `debit`.
            debit (int, optional): Debit amount for the revaluation.
            description (str): Description of the revaluation.
            price (float, optional): Price associated with the revaluation.
        """

    @abstractmethod
    def modify_revaluation(
        self,
        date: datetime.date,
        account: str,
        credit: int,
        description: str,
        debit: int = None,
        price: float = None
    ) -> None:
        """Updates an existing revaluation.

        The unique identifier is a combination of `date` and `account`.

        Args:
            date (datetime.date): Date of the revaluation.
            account (str): Account (e.g. "1020") or range of accounts
                          (e.g. "1020:1099") to revalue.
            credit (int, optional): Credit amount for the revaluation.
                                    If None, uses the value of `debit`.
            debit (int, optional): Debit amount for the revaluation.
            description (str): Description of the revaluation.
            price (float, optional): Price associated with the revaluation.
        """

    @abstractmethod
    def delete_revaluations(
        self,
        account: str,
        date: datetime.date,
        allow_missing: bool = False
    ) -> None:
        """Removes revaluations.

        The unique identifier is a combination of `date` and `account`.

        Args: accounts (str): Account or account range of revaluations to remove.
        date (datetime.date | None): The date of the revaluations to remove.
            allow_missing (bool, optional): If True, no error is raised if a revaluation entry is
                                            not found. Defaults to False.
        """

    def mirror_revaluations(self, target: pd.DataFrame, delete: bool = False) -> dict:
        """Aligns revaluations with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame adhering to REVALUATION_SCHEMA.
            delete (bool, optional): If True, deletes existing revaluations that are not
                                     present in the target data.

        Returns:
            dict: A dictionary containing statistics about the mirroring process:
                - pre-existing (int): The number of revaluations present before mirroring.
                - targeted (int): The number of revaluations in the target data.
                - added (int): The number of revaluations added by the mirroring method.
                - deleted (int): The number of deleted revaluations.
                - updated (int): The number of revaluations modified during mirroring.
        """

    # ----------------------------------------------------------------------
    # File Operations

    def export_account_sheets(self, file: str, root: str = None) -> None:
        """Export ledger as Excel file, with separate sheets containing
        transactions for each account.

        Args:
            file (str): The path where the Excel file will be saved.
            root (str, optional): Root directory for document paths. If not None, valid document
                                  paths are formatted as hyperlinks to the file in the root folder.
                                  Defaults to None.
        """
        out = {}
        for account in self.accounts().index.sort_values():
            df = self._fetch_account_history(account)
            out[str(account)] = df.drop(columns="account")
        excel.write_sheets(out, path=file)

        if root is not None:
            # Add link to open documents locally
            root_dir = Path(root).expanduser()
            workbook = openpyxl.load_workbook(file)
            for sheet in workbook.worksheets:
                for cell in sheet["K"]:
                    # Column K corresponds to 'documents'
                    if cell.value is not None:
                        document = root_dir.joinpath(cell.value)
                        if document.is_file():
                            cell.hyperlink = f"file://{str(document)}"
                            cell.style = "Hyperlink"
            workbook.save(file)

    def dump_to_zip(self, archive_path: str):
        """Dump entire ledger system into a ZIP archive.

        Save all data and settings of an accounting system into a ZIP archive.
        Each component of the ledger system (accounts, tax_codes, ledger entries,
        settings, etc.) is stored as an individual file inside the ZIP archive
        for modular restoration and analysis.

        Args:
            archive_path (str): The file path of the ZIP archive.
        """
        with zipfile.ZipFile(archive_path, 'w') as archive:
            settings = {}
            settings["REPORTING_CURRENCY"] = self.reporting_currency
            archive.writestr('settings.json', json.dumps(settings))
            archive.writestr('ledger.csv', self.ledger().to_csv(index=False))
            archive.writestr('tax_codes.csv', self.tax_codes().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts().to_csv(index=False))
            archive.writestr('assets.csv', self.assets().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        """Restore ledger system from a ZIP archive.

        Restores a dumped ledger system from a ZIP archive.
        Extracts the accounts, tax codes, ledger entries, reporting currency, etc.,
        from the ZIP archive and passes the extracted data to the `restore` method.

        Args:
            archive_path (str): The file path of the ZIP archive to restore.
        """
        required_files = {'ledger.csv', 'tax_codes.csv', 'accounts.csv', 'settings.json'}

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            ledger = pd.read_csv(archive.open('ledger.csv'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            tax_codes = pd.read_csv(archive.open('tax_codes.csv'))
            assets = pd.read_csv(archive.open('assets.csv'))
            self.restore(
                settings=settings,
                ledger=ledger,
                tax_codes=tax_codes,
                accounts=accounts,
                assets=assets
            )

    def restore(
        self,
        settings: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        ledger: pd.DataFrame | None = None,
        assets: pd.DataFrame | None = None,
    ):
        """Replaces the entire ledger system with data provided as arguments.

        Args:
            settings (dict | None): System settings. If `None`, settings remains unchanged.
            tax_codes (pd.DataFrame | None): Tax codes of the restored ledger system.
                If `None`, tax codes remain unchanged.
            accounts (pd.DataFrame | None): Accounts of the restored ledger system.
                If `None`, accounts remain unchanged.
            ledger (pd.DataFrame | None): Ledger entries of the restored system.
                If `None`, ledger remains unchanged.
            assets (pd.DataFrame | None): Assets entries of the restored system.
                If `None`, assets remains unchanged.
        """
        if settings is not None and "REPORTING_CURRENCY" in settings:
            self.reporting_currency = settings["REPORTING_CURRENCY"]
        if assets is not None:
            self.mirror_assets(assets, delete=True)
        if tax_codes is not None:
            self.mirror_tax_codes(tax_codes, delete=True)
        if accounts is not None:
            self.mirror_accounts(accounts, delete=True)
        if ledger is not None:
            self.mirror_ledger(ledger, delete=True)
        # TODO: Implement price history and revaluation restoration logic

    def clear(self):
        """Clear all data from the ledger system.

        This method removes all entries from the ledger, tax codes, accounts, etc.
        restoring the system to a pristine state.
        """
        self.mirror_ledger(None, delete=True)
        self.mirror_tax_codes(None, delete=True)
        self.mirror_accounts(None, delete=True)
        self.mirror_assets(None, delete=True)
        # TODO: Implement price history and revaluation clearing logic

    # ----------------------------------------------------------------------
    # Tax rates

    @abstractmethod
    def tax_codes(self) -> pd.DataFrame:
        """Retrieves all tax definitions.

        Returns:
            pd.DataFrame: DataFrame with columns `id` (str), `account` (int), `rate` (float),
                          `is_inclusive` (bool), `description` (str).
        """

    @abstractmethod
    def add_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
    ) -> None:
        """Append a tax code to the list of available tax_codes.

        Args:
            id (str): Identifier for the tax definition.
            rate (float): The tax code to apply.
            account (str): Account to which the tax code is applicable.
            is_inclusive (bool, optional): Specifies whether the tax amount is included in the
                                           transaction amount. Defaults to True.
        """

    @abstractmethod
    def modify_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        """
        Update an existing tax code.

        Args:
            id (str): Tax code to update.
            rate (float): Tax rate (from 0 to 1).
            account (str): Account identifier for the tax code.
            is_inclusive (bool, optional): If True, tax is 'NET' (default), else 'GROSS'.
            description (str, optional): Description for the tax code.
        """

    @abstractmethod
    def delete_tax_codes(self, codes: List[str] = [], allow_missing: bool = False) -> None:
        """Removes tax code definitions.

        Args:
            codes (List[str]): Tax codes to be removed.
            allow_missing (bool, optional): If True, no error is raised if the tax code is not
                                            found.
        """

    def mirror_tax_codes(self, target: pd.DataFrame, delete: bool = False):
        """Aligns tax codes with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with tax codes in the pyledger.tax_codes format.
            delete (bool, optional): If True, deletes existing tax codes that are
                                     not present in the target data.

        Returns:
            dict: A dictionary containing statistics about the mirroring process:
                - pre-existing (int): The number of tax codes present before mirroring.
                - targeted (int): The number of tax codes in the target data.
                - added (int): The number of tax codes added by the mirroring method.
                - deleted (int): The number of deleted tax codes.
                - updated (int): The number of tax codes modified during mirroring.
        """
        target_df = self.standardize_tax_codes(target)
        current_state = self.tax_codes()

        # Delete superfluous tax codes on remote
        if delete:
            to_delete = set(current_state["id"]).difference(set(target_df["id"]))
            self.delete_tax_codes(to_delete)

        # Create new tax codes on remote
        ids = set(target_df["id"]).difference(set(current_state["id"]))
        to_add = target_df.loc[target_df["id"].isin(ids)]
        for row in to_add.to_dict("records"):
            self.add_tax_code(
                id=row["id"],
                description=row["description"],
                account=row["account"],
                rate=row["rate"],
                is_inclusive=row["is_inclusive"],
            )

        # Update modified tax codes on remote
        both = set(target_df["id"]).intersection(set(current_state["id"]))
        left = target_df.loc[target_df["id"].isin(both)]
        right = current_state.loc[current_state["id"].isin(both)]
        merged = pd.merge(left, right, how="outer", indicator=True)
        to_update = merged[merged["_merge"] == "left_only"]
        for row in to_update.to_dict("records"):
            self.modify_tax_code(
                id=row["id"],
                description=row["description"],
                account=row["account"],
                rate=row["rate"],
                is_inclusive=row["is_inclusive"],
            )

        # return number of elements found, targeted, changed:
        stats = {
            "pre-existing": len(current_state),
            "targeted": len(target_df),
            "added": len(to_add),
            "deleted": len(to_delete) if delete else 0,
            "updated": len(to_update),
        }

        return stats

    @classmethod
    def standardize_tax_codes(cls, df: pd.DataFrame,
                              keep_extra_columns: bool = False) -> pd.DataFrame:
        """Validates and standardizes the 'tax_codes' DataFrame to ensure it contains
        the required columns, correct data types, and logical consistency in the data.

        Args:
            df (pd.DataFrame): The DataFrame representing the tax codes.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.

        Returns:
            pd.DataFrame: The standardized tax codes DataFrame.

        Raises:
            ValueError: If required columns are missing, if data types are incorrect,
                        or if logical inconsistencies are found (e.g., non-zero rates
                        without defined accounts).
        """
        # Enforce data frame schema
        df = enforce_schema(df, TAX_CODE_SCHEMA, keep_extra_columns=keep_extra_columns)

        # Ensure account is defined if rate is other than zero
        missing = list(df["id"][(df["rate"] != 0) & df["account"].isna()])
        if len(missing) > 0:
            # TODO: Drop entries with missing account with a warning, rather than raising an error
            raise ValueError(f"Account must be defined for non-zero rate in tax_codes: {missing}.")

        return df

    # ----------------------------------------------------------------------
    # Accounts

    @abstractmethod
    def accounts(self) -> pd.DataFrame:
        """Retrieves a data frame with all account definitions.

        Returns:
            pd.DataFrame: DataFrame with columns `account` (int), `description` (str),
                          `currency` (str), `tax_code` (str or None). `None` implies
                          tax code is never applicable. If set, tax code is sometimes applicable,
                          and transactions on this account must explicitly state
                          a `tax_code`. The value in the accounts serves as default
                          for new transactions.
        """

    @classmethod
    def standardize_accounts(cls, df: pd.DataFrame,
                             keep_extra_columns: bool = True) -> pd.DataFrame:
        """Validates and standardizes the 'accounts' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the accounts.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.

        Returns:
            pd.DataFrame: The standardized accounts DataFrame.

        Raises:
            ValueError: If required columns are missing, or if 'account' column
                        contains NaN values, or if data types are incorrect.
        """
        df = enforce_schema(df, ACCOUNT_SCHEMA, keep_extra_columns=keep_extra_columns)
        # Ensure required values are present
        if df["account"].isna().any():
            # TODO: Drop entries with missing accounts with a warning, rather than raising an error
            raise ValueError("Missing 'account' values in accounts.")

        return df

    def account_currency(self, account: int) -> str:
        accounts = self.accounts()
        if not int(account) in accounts["account"].values:
            raise ValueError(f"Account {account} is not defined.")
        return accounts.loc[accounts["account"] == account, "currency"].values[0]

    @abstractmethod
    def add_account(
        self, account: int, description: str, currency: str, tax_code: bool = False
    ) -> None:
        """Appends an account to the accounts.

        Args:
            account (int): Unique identifier for the account.
            description (str): Description of the account.
            currency (str): Currency of the account.
            tax_code (bool, optional): Indicates if tax is applicable. Defaults to False.
        """

    @abstractmethod
    def modify_account(self, account: int, new_data: dict) -> None:
        """Modifies an existing account definition.

        Args:
            account (int): Account to be modified.
            new_data (dict): Fields to be overwritten. Keys typically include
                             `description` (str), `currency` (str), or `tax_code` (str).
        """

    @abstractmethod
    def delete_accounts(self, accounts: List[int] = [], allow_missing: bool = False) -> None:
        """Removes an account from the account definitions.

        Args:
            accounts (List[int]): The numbers of the accounts to be deleted.
            allow_missing (bool, optional): If True, no error is raised if the account is
                                            not found.
        """

    def mirror_accounts(self, target: pd.DataFrame, delete: bool = False):
        """Aligns the accounts with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes existing accounts that are not
                                     present in the target data.

        Returns:
            dict: A dictionary containing statistics about the mirroring process:
                - pre-existing (int): The number of accounts present before mirroring.
                - targeted (int): The number of accounts in the target data.
                - added (int): The number of accounts added by the mirroring method.
                - deleted (int): The number of accounts codes.
                - updated (int): The number of accounts modified during mirroring.
        """
        if target is not None:
            target = target.copy()
        target_df = self.standardize_accounts(target)
        current_state = self.accounts()

        # Delete superfluous accounts on remote
        if delete:
            to_delete = set(current_state["account"]).difference(set(target_df["account"]))
            self.delete_accounts(to_delete)

        # Create new accounts on remote
        accounts = set(target_df["account"]).difference(set(current_state["account"]))
        to_add = target_df.loc[target_df["account"].isin(accounts)]
        for row in to_add.to_dict("records"):
            self.add_account(
                account=row["account"],
                currency=row["currency"],
                description=row["description"],
                tax_code=row["tax_code"],
                group=row["group"],
            )

        # Update modified accounts on remote
        both = set(target_df["account"]).intersection(set(current_state["account"]))
        left = target_df.loc[target_df["account"].isin(both)]
        right = current_state.loc[current_state["account"].isin(both)]
        merged = pd.merge(left, right, how="outer", indicator=True)
        to_update = merged[merged["_merge"] == "left_only"]
        for row in to_update.to_dict("records"):
            self.modify_account(
                account=row["account"],
                currency=row["currency"],
                description=row["description"],
                tax_code=row["tax_code"],
                group=row["group"],
            )

        # return number of elements found, targeted, changed:
        stats = {
            "pre-existing": len(current_state),
            "targeted": len(target_df),
            "added": len(to_add),
            "deleted": len(to_delete) if delete else 0,
            "updated": len(to_update),
        }
        return stats

    @abstractmethod
    def _single_account_balance(self, account: int, date: datetime.date = None) -> dict:
        """Retrieve the balance of a single account up to a specified date.

        Args:
            account (int): Account number.
            date (datetime.date, optional): Date for which to retrieve the balance.
                                            Defaults to None.

        Returns:
            dict: Dictionary containing the balance of the account in various currencies.
        """

    def _account_balance_list(self, accounts: list[int], date: datetime.date = None) -> dict:
        result = {}
        for account in accounts:
            account_balance = self._single_account_balance(account, date=date)
            for currency, value in account_balance.items():
                result[currency] = result.get(currency, 0) + value
        return result

    def _account_balance_range(
        self, accounts: dict[str, list[int]], date: datetime.date = None
    ) -> dict:
        result = {}
        add = self._account_balance_list(accounts["add"], date=date)
        subtract = self._account_balance_list(accounts["subtract"], date=date)
        for currency, value in add.items():
            result[currency] = result.get(currency, 0) + value
        for currency, value in subtract.items():
            result[currency] = result.get(currency, 0) - value
        return result

    def account_balance(
        self, account: int | str | dict, date: datetime.date = None
    ) -> dict:
        """Balance of a single account or a list of accounts.

        Args:
            account (int, str, dict): The account(s) to be evaluated. Can be a
            sequence of accounts separated by a column, e.g. "1000:1999", in
            which case the combined balance of all accounts within the
            specified range is returned. Multiple accounts and/or account
            sequences can be separated by a plus or minus sign,
            e.g. "1000+1020:1025", in which case the combined balance of all
            accounts is returned, or "1020:1025-1000", in which case the
            balance of account 1000 is deducted from the combined balance of
            accounts 1020:1025.
            date (datetime.date, optional): The date as of which the account
                                            balance is calculated. Defaults to None.

        Returns:
            dict: Dictionary containing the balance of the account(s) in various currencies.
        """
        start, end = parse_date_span(date)
        if start is None:
            # Account balance per a single point in time
            if represents_integer(account):
                result = self._single_account_balance(account=abs(int(account)), date=end)
                if int(account) < 0:
                    result = {key: -1.0 * val for key, val in result.items()}
            elif (
                isinstance(account, dict)
                and ("add" in account.keys())
                and ("subtract" in account.keys())
            ):
                result = self._account_balance_range(accounts=account, date=end)
            elif isinstance(account, str):
                accounts = self.account_range(account)
                result = self._account_balance_range(accounts=accounts, date=end)
            else:
                raise ValueError(
                    f"Account(s) '{account}' of type {type(account).__name__} not identifiable."
                )
        else:
            # Account balance over a period
            at_start = self.account_balance(
                account=account,
                date=start - datetime.timedelta(days=1)
            )
            at_end = self.account_balance(account=account, date=end)
            result = {
                currency: at_end.get(currency, 0) - at_start.get(currency, 0)
                for currency in (at_start | at_end).keys()
            }

        result = {
            ticker: self.round_to_precision(value, ticker=ticker, date=date)
            for ticker, value in result.items()
        }

        return result

    def account_history(
        self, account: int | str | dict, period: datetime.date = None
    ) -> pd.DataFrame:
        """Transaction and balance history of an account or a list of accounts.

        Args:
            account (int, str, dict): The account(s) to be evaluated. Can be a
            sequence of accounts separated by a column, e.g. "1000:1999", in
            which case the combined balance of all accounts within the
            specified range is returned. Multiple accounts and/or account
            sequences can be separated by a plus or minus sign,
            e.g. "1000+1020:1025", in which case the combined balance of all
            accounts is returned, or "1020:1025-1000", in which case the
            balance of account 1000 is deducted from the combined balance of
            accounts 1020:1025.
            period (datetime.date, optional): The date as of which the account balance
                                              is calculated. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the transaction and balance
                          history of the account(s).
        """
        start, end = parse_date_span(period)
        # Account balance per a single point in time
        if represents_integer(account):
            account = int(account)
            if account not in self.accounts()[["account"]].values:
                raise ValueError(f"No account matching '{account}'.")
            out = self._fetch_account_history(account, start=start, end=end)
        elif (
            isinstance(account, dict)
            and ("add" in account.keys())
            and ("subtract" in account.keys())
        ):
            accounts = list(set(account["add"]) - set(account["subtract"]))
            out = self._fetch_account_history(accounts, start=start, end=end)
        elif isinstance(account, str):
            accounts = self.account_range(account)
            accounts = list(set(accounts["add"]) - set(accounts["subtract"]))
            out = self._fetch_account_history(accounts, start=start, end=end)
        elif isinstance(account, list):
            not_integer = [i for i in account if not represents_integer(i)]
            if any(not_integer):
                raise ValueError(f"Non-integer list elements in `account`: {not_integer}.")
            accounts = account = [int(i) for i in account]
            out = self._fetch_account_history(accounts, start=start, end=end)
        else:
            raise ValueError(
                f"Account(s) '{account}' of type {type(account).__name__} not identifiable."
            )

        return out

    def _fetch_account_history(
        self, account: int | list[int], start: datetime.date = None, end: datetime.date = None
    ) -> pd.DataFrame:
        """Fetch transaction history of a list of accounts and compute balance.

        Args:
            account (int, list[int]): The account or list of accounts to fetch the history for.
            start (datetime.date, optional): Start date for the history. Defaults to None.
            end (datetime.date, optional): End date for the history. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the transaction history of the account(s).
        """
        ledger = self.serialized_ledger()
        if isinstance(account, list):
            filter = ledger["account"].isin(account)
        else:
            filter = ledger["account"] == account
        if end is not None:
            filter = filter & (ledger["date"] <= end)
        df = ledger.loc[filter, :]
        df = df.sort_values("date")
        df["balance"] = df["amount"].cumsum()
        df["report_balance"] = df["report_amount"].cumsum()
        cols = [col for col in LEDGER_SCHEMA["column"] if col in df.columns]
        if start is not None:
            df = df.loc[df["date"] >= start, :]
        df = df.reset_index(drop=True)
        return df[cols]

    def account_range(self, range: int | str) -> dict:
        """Determine the account range for a given input.

        Args:
            range (int, str): The account range input.

        Returns:
            dict: Dictionary with 'add' and 'subtract' lists of accounts.
        """
        add = []
        subtract = []
        if represents_integer(range):
            account = int(range)
            if abs(account) in self.accounts()["account"].values:
                if account >= 0:
                    add = [account]
                else:
                    subtract = [abs(account)]
        elif isinstance(range, float):
            raise ValueError(f"`range` {range} is not an integer value.")
        elif isinstance(range, str):
            is_addition = True
            for element in re.split(r"(-|\+)", range.strip()):
                accounts = []
                if element.strip() == "":
                    pass
                elif element.strip() == "+":
                    is_addition = True
                elif element.strip() == "-":
                    is_addition = False
                elif re.fullmatch("^[^:]*[:][^:]*$", element):
                    # `element` contains exactly one colon (':')
                    sequence = element.split(":")
                    first = int(sequence[0].strip())
                    last = int(sequence[1].strip())
                    accounts = self.accounts()
                    in_range = (accounts["account"] >= first) & (accounts["account"] <= last)
                    accounts = list(accounts["account"][in_range])
                elif re.match("[0-9]*", element.strip()):
                    accounts = [int(element.strip())]
                else:
                    raise ValueError(f"Accounts '{element}' not identifiable.")
                if len(accounts) > 0:
                    if is_addition:
                        add += accounts
                    else:
                        subtract += accounts
        else:
            raise ValueError(
                f"Expecting int, float, or str `range`, not {type(range).__name__} {range}."
            )
        if (len(add) == 0) and (len(subtract) == 0):
            raise ValueError(f"No account matching '{range}'.")
        return {"add": add, "subtract": subtract}

    def sanitize_accounts(self, accounts: pd.DataFrame) -> pd.DataFrame:
        """Discards inconsistent entries in the accounts.

        Args:
            accounts (pd.DataFrame): Accounts as a DataFrame.

        Returns:
            pd.DataFrame: DataFrame with sanitized accounts.
        """
        accounts = self.st  # noqa: F841

    # ----------------------------------------------------------------------
    # Ledger

    @abstractmethod
    def ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame representing all ledger transactions.

        Returns:
            pd.DataFrame: DataFrame with columns `id` (str), `date` (datetime),
            `document` (str), `amounts` (DataFrame). `amounts` is a nested
            pd.DataFrame with columns `account` (int), `contra`
            (int or None), `currency` (str), `amount` (float),
            `report_amount` (float or None), and `tax_code` (str).
        """

    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with a long representation of all ledger transactions.

        Simple transactions with a credit and debit account are represented twice,
        one with 'account' corresponding to the credit account and one with
        account corresponding to the debit account.

        Returns:
            pd.DataFrame: DataFrame with columns `id` (str), `date` (datetime),
                         `account` (int), `contra` (int or None), `currency` (str),
                         `amount` (float), `report_amount` (float or None),
                         `tax_code` (str), and `document` (str).
        """
        return self.serialize_ledger(self.ledger())

    @abstractmethod
    def ledger_entry(self, id: str) -> dict:
        """Retrieves a specific ledger entry by its ID.

        Args:
            id (str): The unique identifier for the ledger entry.

        Returns:
            dict: A dictionary representing the ledger entry.
        """

    @abstractmethod
    def add_ledger_entry(
        self, date: datetime.date, document: str, amounts: pd.DataFrame
    ) -> None:
        """Adds a new posting to the ledger.

        Args:
            date (datetime.date): Date of the transaction.
            document (str): Reference to the associated document.
            amounts (pd.DataFrame): DataFrame detailing transaction amounts.
            Columns include: `account` (int), `contra` (int, optional),
            `currency` (str), `amount` (float), `report_amount` (float, optional),
            `tax_code` (str, optional).
        """

    @abstractmethod
    def modify_ledger_entry(self, id: str, new_data: dict) -> None:
        """Modifies an existing ledger entry.

        Args:
            id (str): Unique identifier of the ledger entry to be modified.
            new_data (dict): Fields to be overwritten in the ledger entry.
                             Keys typically include `date` (datetime.date),
                             `document` (str), or `amounts` (pd.DataFrame).
        """

    @abstractmethod
    def delete_ledger_entries(self, ids: List[str] = []) -> None:
        """Deletes a ledger entry by its ID.

        Args:
            ids (List[str]): The ids of the ledger entries to be deleted.
        """

    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
        """Aligns ledger entries with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with ledger entries in pyledger format.
            delete (bool, optional): If True, deletes existing ledger that are not
                                     present in the target data.

        Returns:
            dict: A dictionary containing statistics about the mirroring process:
                - pre-existing (int): The number of transactions present before mirroring.
                - targeted (int): The number of transactions in the target data.
                - added (int): The number of new transactions that were added.
                - deleted (int): The number of deleted transactions.
        """
        # Standardize data frame schema, discard incoherent entries with a warning
        target = self.standardize_ledger(target)
        target = self.sanitize_ledger(target)

        # Nest to create one row per transaction, add unique string identifier
        def process_ledger(df: pd.DataFrame) -> pd.DataFrame:
            df = nest(
                df,
                columns=[col for col in df.columns if col not in ["id", "date"]],
                key="txn",
            )
            df["txn_str"] = [
                f"{str(date)},{df_to_consistent_str(txn)}"
                for date, txn in zip(df["date"], df["txn"])
            ]
            return df

        remote = process_ledger(self.ledger())
        target = process_ledger(target)
        if target["id"].duplicated().any():
            # We expect nesting to combine all rows with the same
            raise ValueError("Non-unique dates in `target` transactions.")

        # Count occurrences of each unique transaction in target and remote,
        # find number of additions and deletions for each unique transaction
        count = pd.DataFrame({
            "remote": remote["txn_str"].value_counts(),
            "target": target["txn_str"].value_counts(),
        })
        count = count.fillna(0).reset_index(names="txn_str")
        count["n_add"] = (count["target"] - count["remote"]).clip(lower=0).astype(int)
        count["n_delete"] = (count["remote"] - count["target"]).clip(lower=0).astype(int)

        # Delete unneeded transactions on remote
        if delete and any(count["n_delete"] > 0):
            ids = [
                id
                for txn_str, n in zip(count["txn_str"], count["n_delete"])
                if n > 0
                for id in remote.loc[remote["txn_str"] == txn_str, "id"]
                .tail(n=n)
                .values
            ]
            self.delete_ledger_entries(ids)

        # Add missing transactions to remote
        for txn_str, n in zip(count["txn_str"], count["n_add"]):
            if n > 0:
                txn = unnest(
                    target.loc[target["txn_str"] == txn_str, :].head(1), "txn"
                )
                txn.drop(columns="txn_str", inplace=True)
                if txn["id"].dropna().nunique() > 0:
                    id = txn["id"].dropna().unique()[0]
                else:
                    id = txn["description"].iat[0]
                for _ in range(n):
                    try:
                        self.add_ledger_entry(txn)
                    except Exception as e:
                        raise Exception(
                            f"Error while adding ledger entry {id}: {e}"
                        ) from e

        # return number of elements found, targeted, changed:
        stats = {
            "pre-existing": int(count["remote"].sum()),
            "targeted": int(count["target"].sum()),
            "added": count["n_add"].sum(),
            "deleted": count["n_delete"].sum() if delete else 0,
        }
        return stats

    def sanitize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Discards inconsistent ledger entries and inconsistent tax codes.

        Logs a warning for each discarded entry with reason for dropping.

        Args:
            ledger (pd.DataFrame): Ledger data as a DataFrame.

        Returns:
            pd.DataFrame: DataFrame with sanitized ledger entries.
        """
        # Discard undefined tax codes
        ledger["tax_code"] = ledger["tax_code"].str.strip()
        invalid = ledger["tax_code"].notna() & ~ledger["tax_code"].isin(self.tax_codes()["id"])
        if invalid.any():
            df = ledger.loc[invalid, ["id", "tax_code"]]
            df = df.groupby("id").agg({"tax_code": lambda x: x.unique()})
            for id, codes in zip(df["id"].values, df["tax_code"]):
                if len(codes) > 1:
                    self._logger.warning(
                        f"Discard unknown tax codes {', '.join([f'{x}' for x in codes])} at '{id}'."
                    )
                else:
                    self._logger.warning(f"Discard unknown tax code '{codes[0]}' at '{id}'.")
            ledger.loc[invalid, "tax_code"] = None

        # Collect postings to be discarded as pd.DataFrame with columns 'id' and
        # "description". The latter specifies the reason(s) why the entry is discarded.
        to_discard = []  # List[pd.DataFrame['id', "description"]]

        # 1. Discard journal entries with non-unique dates
        grouped = ledger[["date", "id"]].groupby("id")
        df = grouped.filter(lambda x: x["date"].nunique() > 1)
        if df.shape[0] > 0:
            df = df.drop_duplicates()
            df["description"] = df["date"].astype(pd.StringDtype())
            df = df.groupby("id").agg({"description": lambda x: f"multiple dates {', '.join(x)}"})
            to_discard.append(df.reset_index()[["id", "description"]])

        # 2. Discard journal entries with undefined accounts
        valid_accounts = self.accounts()["account"].values
        invalid_accounts = []
        for col in ["account", "contra"]:
            invalid = ledger[col].notna() & ~ledger[col].isin(valid_accounts)
            if invalid.any():
                df = ledger.loc[invalid, ["id", col]]
                df = df.rename(columns={col: "account"})
                invalid_accounts.append(df)
        if len(invalid_accounts) > 0:
            df = pd.concat(invalid_accounts)
            df = df.groupby("id").agg({"account": lambda x: x.unique()})
            df["description"] = [
                f"Accounts {', '.join([str(y) for y in x])} not defined"
                if len(x) > 1
                else f"Account {x[0]} not defined"
                for x in df["account"]
            ]
            to_discard.append(df.reset_index()[["id", "description"]])

        # 2. Discard journal entries without amount or balance or with both
        if "target_balance" in ledger.columns:
            invalid = ledger["amount"].isna() & ledger["target_balance"].isna()
        else:
            invalid = ledger["amount"].isna()
        if invalid.any():
            df = pd.DataFrame(
                {"id": ledger.loc[invalid, "id"].unique(), "description": "amount missing"}
            )
            to_discard.append(df)
        if "target_balance" in ledger.columns:
            invalid = ledger["amount"].notna() & ledger["target_balance"].notna()
            if invalid.any():
                df = pd.DataFrame({
                    "id": ledger.loc[invalid, "id"].unique(),
                    "description": "both amount and target amount defined"
                })
                to_discard.append(df)

        # Discard journal postings identified above
        if len(to_discard) > 0:
            df = pd.concat(to_discard)
            df = df.groupby("id").agg({"description": lambda x: ", ".join(x)})
            for id, description in zip(df.index, df["description"]):
                self._logger.warning(f"Discard ledger entry '{id}': {description}.")
            ledger = ledger.loc[~ledger["id"].isin(df.index), :]

        return ledger

    @staticmethod
    def standardize_ledger_columns(df: pd.DataFrame = None,
                                   keep_extra_columns: bool = True) -> pd.DataFrame:
        """Standardizes and enforces type consistency for the ledger DataFrame.

        Ensures that the required columns are present in the ledger DataFrame,
        adds any missing optional columns with None values, and enforces
        specific data types for each column.

        Args:
            ledger (pd.DataFrame, optional): A data frame with ledger transactions
                                             Defaults to None.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.

        Returns:
            pd.DataFrame: A standardized DataFrame with both the required and
            optional columns with enforced data types.

        Raises:
            ValueError: If required columns are missing from the ledger DataFrame.
        """
        # Enforce data frame schema
        df = enforce_schema(df, LEDGER_SCHEMA, sort_columns=False,
                            keep_extra_columns=keep_extra_columns)

        # Add id column if missing: Entries without a date share id of the last entry with a date
        if "id" not in df.columns or df["id"].isna().any():
            id_type = LEDGER_SCHEMA.loc[LEDGER_SCHEMA['column'] == 'id', 'dtype'].item()
            df["id"] = df["date"].notna().cumsum().astype(id_type)

        # Enforce column data types
        date_type = LEDGER_SCHEMA.loc[LEDGER_SCHEMA['column'] == 'date', 'dtype'].item()
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(date_type)

        return df

    def standardize_ledger(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert ledger entries to a canonical representation.

        This method converts ledger entries into a standardized format. It
        ensures uniformity where transactions can be defined in various
        equivalent ways, allowing for easy identification of equivalent
        entries.

        Args:
            df (pd.DataFrame): A data frame with ledger transactions.

        Returns:
            pd.DataFrame: A DataFrame with ledger entries in canonical form.

        Notes:
            - The method removes redundant 'report_amount' values for
            transactions in the reporting currency.
            - It fills missing dates in collective transactions with dates from
            other line items in the same collective transaction.
        """
        df = self.standardize_ledger_columns(df).copy()

        # Fill missing (NA) dates
        df["date"] = df.groupby("id")["date"].ffill()
        df["date"] = df.groupby("id")["date"].bfill()

        # Drop redundant report_amount for transactions in reporting currency
        set_na = (
            (df["currency"] == self.reporting_currency)
            & (df["report_amount"].isna() | (df["report_amount"] == df["amount"]))
        )
        df.loc[set_na, "report_amount"] = pd.NA

        # Remove leading and trailing spaces in strings, convert -0.0 to 0.0
        for col in df.columns:
            if pd.StringDtype.is_dtype(df[col]):
                df[col] = df[col].str.strip()
            elif pd.Float64Dtype.is_dtype(df[col]):
                df.loc[df[col].notna() & (df[col] == 0), col] = 0.0

        return df

    def serialize_ledger(self, df: pd.DataFrame) -> pd.DataFrame:
        """Serializes the ledger into a long format.

        Simple journal postings with both credit and debit entries appear twice,
        one with account in the resulting data frame corresponding to the credit
        account and one corresponding to the debit account.

        Args:
            df (pd.DataFrame): DataFrame containing the original ledger entries in wide format.

        Returns:
            pd.DataFrame: Serialized DataFrame in long format.
        """
        # Create separate DataFrames for credit and debit accounts
        credit = df[LEDGER_SCHEMA["column"]]
        debit = credit.copy()
        debit["amount"] *= -1.0
        debit["report_amount"] *= -1.0
        debit["account"] = df["contra"]
        debit["contra"] = df["account"]
        # Combine credit and debit entries
        result = pd.concat([
            credit.loc[credit["account"].notna()],
            debit.loc[debit["account"].notna()]
        ])
        return result[LEDGER_SCHEMA["column"]]

    def txn_to_str(self, df: pd.DataFrame) -> Dict[str, str]:
        """Create a consistent, unique representation of ledger transactions.

        Converts transactions into a dict of CSV-like string representation.
        The result can be used to compare transactions.

        Args:
            df (pd.DataFrame): DataFrame containing ledger transactions.

        Returns:
            Dict[str, str]: A dictionary where keys are ledger 'id's and values are
            unique string representations of the transactions.
        """
        df = self.standardize_ledger(df)
        df = nest(df, columns=[col for col in df.columns if col not in ["id", "date"]], key="txn")
        if df['id'].duplicated().any():
            raise ValueError("Some collective transaction(s) have non-unique date.")

        result = {
            str(id): f"{str(date.strftime("%Y-%m-%d"))}\n{df_to_consistent_str(txn)}"
            for id, date, txn in zip(df["id"], df["date"], df["txn"])
        }
        return result

    # ----------------------------------------------------------------------
    # Currency

    @property
    @abstractmethod
    def reporting_currency(self) -> str:
        """Reporting currency of the ledger system."""

    @reporting_currency.setter
    @abstractmethod
    def reporting_currency(self, currency):
        """Set the reporting currency of the ledger system."""

    @abstractmethod
    def precision(
        self, ticker: str, date: datetime.date = None
    ) -> float:
        """Returns the smallest increment for quotation of prices of a given asset or currency.

        This is the precision, to which prices should be rounded.

        Args:
            ticker (str): Identifier of the currency or asset.
            date (datetime.date, optional): Date for which to retrieve the precision.
                                            Defaults to today's date.

        Returns:
            float: The smallest price increment.
        """

    def round_to_precision(
        self,
        amount: float | List[float],
        ticker: str | List[str],
        date: datetime.date = None
    ) -> float | list:
        """
        Round amounts to the precision of the specified ticker (currency or asset).

        Args:
            amount (float, List[float]): Value(s) to be rounded.
            ticker (str, List[str]): Ticker symbol(s) of the currency or asset.
                If amount and ticker are both vectors, they must be of same length.
            date (datetime.date, optional): Date for precision determination. Defaults to
                                            today's date.

        Returns:
            float or list: Rounded amount(s), adjusted to the specified ticker's precision.

        Raises:
            ValueError: If the lengths of `amount` and `ticker` do not match, or if input
                        lists have zero length.
        """
        def round_scalar(amount: float, ticker: str, date: datetime.date = None) -> float:
            """Round a scaler to the precision of the specified ticker."""
            precision = self.precision(ticker=ticker, date=date)
            result = round(amount / precision, 0) * precision
            return round(result, -1 * math.floor(math.log10(precision)))

        if np.isscalar(amount) and np.isscalar(ticker):
            result = round_scalar(amount=amount, ticker=ticker, date=date)
        elif np.isscalar(amount):
            result = [None if pd.isna(tck)
                      else round_scalar(amount=amount, ticker=tck, date=date)
                      for tck in ticker]
        elif np.isscalar(ticker):
            result = [None if pd.isna(amt)
                      else round_scalar(amount=amt, ticker=ticker, date=date)
                      for amt in amount]
        else:
            # amount and ticker are both array-like
            if len(amount) != len(ticker):
                raise ValueError("Amount and ticker lists must be of the same length")
            result = [
                None if pd.isna(amt) or pd.isna(tck)
                else round_scalar(amount=amt, ticker=tck, date=date)
                for amt, tck in zip(amount, ticker)
            ]
        return result

    @abstractmethod
    def price_history(self) -> pd.DataFrame:
        """Retrieves a data frame with all price definitions.

        Returns:
            pd.DataFrame: DataFrame with columns `ticker` (str), `date` (datetime.date),
                          `currency` (str), and `price` (float). Tickers can be arbitrarily
                          chosen and can represent anything, including foreign currencies,
                          securities, commodities, or inventory. Price observations are uniquely
                          defined by a date/ticker/currency triple. Prices can be defined in
                          any other currency and are applied up to the subsequent price
                          definition for the same ticker/currency pair.
        """

    @abstractmethod
    def price(
        self, ticker: str, date: datetime.date, currency: str = None
    ) -> tuple[str, float]:
        """Retrieve price for a given ticker as of a specified date.

        If no price is available on the exact date, return latest price
        observation prior to the specified date.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date for which the price is required.
            currency (str, optional): Currency in which the price is desired.

        Returns:
            tuple: (currency, price) where 'currency' is a string indicating the currency
                   of the price, and 'price' is a float representing the asset's price as
                   of the specified date.
        """

    @abstractmethod
    def add_price(
        self, ticker: str, date: datetime.date, currency: str, price: float, overwrite: bool = False
    ) -> None:
        """Appends a price to the price history.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date on which the price is recorded.
            currency (str): Currency in which the price is quoted.
            price (float): Value of the asset as of the given date.
        """

    @abstractmethod
    def modify_price(
        self, ticker: str, date: datetime.date, currency: str, price: float, overwrite: bool = False
    ) -> None:
        """Modifies an observation in the price history.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date on which the price is recorded.
            currency (str): Currency in which the price is quoted.
            price (float): Value of the asset as of the given date.
        """

    @abstractmethod
    def delete_price(self, ticker: str, date: datetime.date, currency: str = None) -> None:
        """Removes a price definition from the history.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date on which the price is recorded.
            currency (str, optional): Currency in which the price is quoted. `None` indicates that
                                      price definitions for this ticker in all currencies
                                      should be removed.
        """

    @classmethod
    def standardize_price_df(cls, df: pd.DataFrame,
                             keep_extra_columns: bool = True) -> pd.DataFrame:
        """Validates and standardizes the 'prices' DataFrame to ensure it contains
        the required columns, correct data types, and no missing values in key fields.

        Args:
            df (pd.DataFrame): The DataFrame representing the prices.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.

        Returns:
            pd.DataFrame: The standardized prices DataFrame.

        Raises:
            ValueError: If required columns are missing, if there are missing values
                        in required columns, or if data types are incorrect.
        """
        df = enforce_schema(df, PRICE_SCHEMA, keep_extra_columns=keep_extra_columns)
        return df

    @classmethod
    def standardize_prices(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Store prices in nested dict: Each prices[ticker][currency] is a DataFrame with
        columns 'date' and 'price' that is sorted by 'date'.
        """
        df = cls.standardize_price_df(df)
        result = {}
        for (ticker, currency), group in df.groupby(["ticker", "currency"]):
            group = group[["date", "price"]].sort_values("date")
            group = group.reset_index(drop=True)
            if ticker not in result.keys():
                result[ticker] = {}
            result[ticker][currency] = group
        return result

    # ----------------------------------------------------------------------
    # Assets

    @classmethod
    def standardize_assets(
        cls,
        df: pd.DataFrame,
        keep_extra_columns: bool = False
    ) -> pd.DataFrame:
        """Validates and standardizes the 'assets' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the assets.
            keep_extra_columns (bool): If True, columns that do not appear in the data frame
                                       schema are left in the resulting DataFrame.
        Returns:
            pd.DataFrame: The standardized revaluations DataFrame.

        Raises:
            ValueError: If required columns are missing or if data types are incorrect.
        """
        df = enforce_schema(df, ASSETS_SCHEMA, keep_extra_columns=keep_extra_columns)
        return df

    @abstractmethod
    def assets(self) -> pd.DataFrame:
        """Retrieves all asset entries.

        Returns:
            pd.DataFrame with columns specified in ASSETS_SCHEMA.
        """

    @abstractmethod
    def add_asset(
        self,
        ticker: str,
        increment: float,
        date: pd.Timestamp = None
    ) -> None:
        """Adds a new asset entry.

        The unique identifier is a combination of `ticker` and `date`.

        Args:
            ticker (str): Identifier for the asset (e.g., stock ticker).
            increment (float): Increment value representing the asset unit.
            date (pd.Timestamp, optional): Date of the asset entry.
        """

    @abstractmethod
    def modify_asset(
        self,
        ticker: str,
        increment: float,
        date: pd.Timestamp = None
    ) -> None:
        """Updates an existing asset entry.

        The unique identifier is a combination of `ticker` and `date`.

        Args:
            ticker (str): Identifier for the asset.
            increment (float): Increment value for the asset.
            date (pd.Timestamp, optional): Date of the asset entry.
        """

    @abstractmethod
    def delete_asset(
        self,
        ticker: str,
        date: pd.Timestamp = None,
        allow_missing: bool = False
    ) -> None:
        """Removes asset entries.

        The unique identifier is a combination of `ticker` and `date`.

        Args:
            ticker (str): Ticker to remove.
            date (pd.Timestamp, optional): Date of the asset entry.
            allow_missing (bool, optional): If True, no error is raised if an asset entry is
                                            not found. Defaults to False.
        """

    def mirror_assets(self, target: pd.DataFrame, delete: bool = False) -> dict:
        """Aligns assets with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with assets in the ASSETS_SCHEMA format.
            delete (bool, optional): If True, deletes existing assets that are not present
                                    in the target data.

        Returns:
            dict: A dictionary containing statistics about the mirroring process:
                - pre-existing (int): The number of assets present before mirroring.
                - targeted (int): The number of assets in the target data.
                - added (int): The number of assets added by the mirroring method.
                - deleted (int): The number of deleted assets.
                - updated (int): The number of assets modified during mirroring.
        """
        current_state = self.assets()
        target = self.prepare_assets_for_mirroring(target)

        # Perform an outer merge to identify differences
        merged = current_state.merge(
            target, on=["ticker", "date"], how="outer", suffixes=('_left', '_right'),
            indicator=True
        )

        # Handle deletions
        to_delete = []
        if delete:
            to_delete = merged[merged["_merge"] == "left_only"]
            for _, row in to_delete.iterrows():
                self.delete_asset(ticker=row["ticker"], date=row["date"])

        # Handle additions
        to_add = merged[merged["_merge"] == "right_only"]
        for _, row in to_add.iterrows():
            self.add_asset(
                ticker=row["ticker"], increment=row["increment_right"], date=row["date"]
            )

        # Handle updates
        to_update = merged[
            (merged["_merge"] == "both") & (merged["increment_left"] != merged["increment_right"])
        ]
        for _, row in to_update.iterrows():
            self.modify_asset(
                ticker=row["ticker"], increment=row["increment_right"], date=row["date"]
            )

        return {
            "pre-existing": len(current_state),
            "targeted": len(target),
            "added": len(to_add),
            "deleted": len(to_delete) if delete else 0,
            "updated": len(to_update)
        }

    def prepare_assets_for_mirroring(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aligns incoming asset data with the current system's constraints.

        Invoked as the initial step in the mirroring process, this method prepares
        asset data for integration into the current system. It adapts the incoming
        data to specific storage requirements and aligns it with existing data, making
        it easy to identify entries that need to be added, modified, or removed.

        By default, this method returns the data unchanged. Subclasses may override
        it to apply class-specific adaptations as required.

        Args:
            df (pd.DataFrame): Incoming asset data.

        Returns:
            pd.DataFrame: Adjusted data ready for synchronization with the current system.
        """
        return self.standardize_assets(df)
