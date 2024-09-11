"""This module defines Abstract base class defining the core interface for
managing a ledger system, including account chart, VAT management, and arbitrary
assets or currencies.
"""

from abc import ABC, abstractmethod
import datetime
import logging
import math
import zipfile
import json
from pathlib import Path
from typing import List
from consistent_df import enforce_dtypes
import re
import numpy as np
import openpyxl
import pandas as pd
from .constants import (
    REQUIRED_ACCOUNT_COLUMNS,
    OPTIONAL_ACCOUNT_COLUMNS,
    REQUIRED_FX_ADJUSTMENT_COLUMNS,
    REQUIRED_LEDGER_COLUMNS,
    OPTIONAL_LEDGER_COLUMNS,
    LEDGER_COLUMN_SHORTCUTS,
    LEDGER_COLUMN_SEQUENCE,
    REQUIRED_PRICE_COLUMNS,
    REQUIRED_VAT_CODE_COLUMNS,
    OPTIONAL_VAT_CODE_COLUMNS,
)
from . import excel
from .helpers import represents_integer
from .time import parse_date_span


class LedgerEngine(ABC):
    """Abstract base class defining the core interface for managing a ledger system,
    including account chart, VAT management, and arbitrary assets or currencies.
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
        contains items 'base_currency' and 'precision'.

        Example:
            settings = {
                'base_currency': 'USD',
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
            ValueError: If 'settings' is not a dictionary, or if 'base_currency'
                        is missing/not a string, or if 'precision' is missing/not a
                        dictionary, or if any key/value in 'precision' is of invalid
                        type.

        Note:
            Modifies 'precision' to include the 'base_currency' key.
        """
        if not isinstance(settings, dict):
            raise ValueError("'settings' must be a dict.")

        # Check for 'base_currency'
        if "base_currency" not in settings or not isinstance(settings["base_currency"], str):
            raise ValueError("Missing/invalid 'base_currency' in settings.")

        # Check for 'precision'
        if "precision" not in settings or not isinstance(settings["precision"], dict):
            raise ValueError("Missing/invalid 'precision'.")

        # Validate contents of 'precision'
        for key, value in settings["precision"].items():
            if not isinstance(key, str) or not isinstance(value, (float, int)):
                raise ValueError("Invalid types in 'precision'.")

        settings["precision"]["base_currency"] = settings["precision"][settings["base_currency"]]

        return settings

    # ----------------------------------------------------------------------
    # FX Adjustments

    @classmethod
    def standardize_fx_adjustments(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validates and standardizes the 'fx_adjustments' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the FX adjustments.

        Returns:
            pd.DataFrame: The standardized FX adjustments DataFrame.

        Raises:
            ValueError: If required columns are missing or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=REQUIRED_FX_ADJUSTMENT_COLUMNS.keys())

        # Ensure required columns and values are present
        missing = set(REQUIRED_FX_ADJUSTMENT_COLUMNS.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} are missing.")

        df = enforce_dtypes(df, REQUIRED_FX_ADJUSTMENT_COLUMNS)
        return df.sort_values("date")

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
        for account in self.account_chart().index.sort_values():
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
        Each component of the ledger system (accounts, vat_codes, ledger entries,
        settings, etc.) is stored as an individual file inside the ZIP archive
        for modular restoration and analysis.

        Args:
            archive_path (str): The file path of the ZIP archive.
        """
        with zipfile.ZipFile(archive_path, 'w') as archive:
            settings = {"base_currency": self.base_currency}
            archive.writestr('settings.json', json.dumps(settings))
            archive.writestr('ledger.csv', self.ledger().to_csv(index=False))
            archive.writestr('vat_codes.csv', self.vat_codes().to_csv(index=False))
            archive.writestr('accounts.csv', self.account_chart().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        """Restore ledger system from a ZIP archive.

        Restores a dumped ledger system from a ZIP archive.
        Extracts the account chart, vat codes, ledger entries, base_currency, etc.,
        from the ZIP archive and passes the extracted data to the `restore` method.

        Args:
            archive_path (str): The file path of the ZIP archive to restore.
        """
        required_files = {'ledger.csv', 'vat_codes.csv', 'accounts.csv', 'settings.json'}

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            base_currency = settings.get("base_currency")
            ledger = pd.read_csv(archive.open('ledger.csv'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            vat_codes = pd.read_csv(archive.open('vat_codes.csv'))

            # Restore the ledger system using the extracted data
            self.restore(
                base_currency=base_currency, ledger=ledger, vat_codes=vat_codes, accounts=accounts
            )

    def restore(
        self,
        base_currency: str | None = None,
        vat_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        ledger: pd.DataFrame | None = None,
    ):
        """Replaces the entire ledger system with data provided as arguments.

        Args:
            base_currency (str | None): Reporting currency. If `None`,
                                        the reporting currency remains unchanged.
            vat_codes (pd.DataFrame | None): VAT codes of the restored ledger system.
                If `None`, VAT codes remain unchanged.
            accounts (pd.DataFrame | None): Accounts of the restored ledger system.
                If `None`, accounts remain unchanged.
            ledger (pd.DataFrame | None): Ledger entries of the restored system.
                If `None`, ledger remains unchanged.
        """
        if base_currency is not None:
            self.base_currency = base_currency
        if vat_codes is not None:
            self.mirror_vat_codes(vat_codes, delete=True)
        if accounts is not None:
            self.mirror_account_chart(accounts, delete=True)
        if ledger is not None:
            self.mirror_ledger(ledger, delete=True)
        # TODO: Implement price history, precision settings,
        # and FX adjustments restoration logic

    def clear(self):
        """Clear all data from the ledger system.

        This method removes all entries from the ledger, VAT codes, account chart,
        base_currency, etc. restoring the system to a pristine state.
        It is designed to be flexible and adapt to the clearing process requirements.
        """
        self.mirror_ledger(None, delete=True)
        self.mirror_vat_codes(None, delete=True)
        self.mirror_account_chart(None, delete=True)
        # TODO: Implement price history, precision settings, and FX adjustments clearing logic

    # ----------------------------------------------------------------------
    # VAT Codes

    @abstractmethod
    def vat_codes(self) -> pd.DataFrame:
        """Retrieves all vat definitions.

        Returns:
            pd.DataFrame: DataFrame with columns `code` (str), `date` (datetime.date),
                          `rate` (float), `included` (bool), `account` (int).
        """

    @abstractmethod
    def add_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        date: datetime.date = None
    ) -> None:
        """Append a vat code to the list of available vat_codes.

        Args:
            code (str): Identifier for the vat definition.
            rate (float): The VAT rate to apply.
            account (str): Account to which the VAT is applicable.
            inclusive (bool, optional): Specifies whether the VAT amount is included in the
                                        transaction amount. Defaults to True.
            date (datetime.date, optional): Date from which onward the vat definition is
                                            applied. The definition is valid until the same
                                            vat code is re-defined on a subsequent date.
                                            Defaults to None.
        """

    @abstractmethod
    def modify_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ) -> None:
        """
        Update an existing VAT code.

        Args:
            code (str): VAT code to update.
            rate (float): VAT rate (0 to 1).
            account (str): Account identifier for the VAT.
            inclusive (bool, optional): If True, VAT is 'NET' (default), else 'GROSS'.
            text (str, optional): Description for the VAT code.
        """

    @abstractmethod
    def delete_vat_code(self, code: str, allow_missing: bool = False) -> None:
        """Removes a vat definition.

        Args:
            code (str): Vat code to be removed.
            allow_missing (bool, optional): If True, no error is raised if the VAT code is not
                                            found; if False, raises error. Defaults to False.
        """

    @abstractmethod
    def mirror_vat_codes(self, target: pd.DataFrame, delete: bool = False):
        """Aligns VAT rates with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame containing VAT rates in the pyledger format.
            delete (bool, optional): If True, deletes existing VAT codes that are
                                     not present in the target data.
        """

    @classmethod
    def standardize_vat_codes(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validates and standardizes the 'vat_codes' DataFrame to ensure it contains
        the required columns, correct data types, and logical consistency in the data.

        Args:
            df (pd.DataFrame): The DataFrame representing the VAT codes.

        Returns:
            pd.DataFrame: The standardized VAT codes DataFrame.

        Raises:
            ValueError: If required columns are missing, if data types are incorrect,
                        or if logical inconsistencies are found (e.g., non-zero rates
                        without defined accounts).
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=REQUIRED_VAT_CODE_COLUMNS.keys())

        # Ensure required columns and values are present
        missing = set(REQUIRED_VAT_CODE_COLUMNS.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} are missing.")

        # Add missing columns
        for col in OPTIONAL_VAT_CODE_COLUMNS.keys():
            if col not in df.columns:
                df[col] = None

        # Enforce data types
        df = enforce_dtypes(df, {**REQUIRED_VAT_CODE_COLUMNS, **OPTIONAL_VAT_CODE_COLUMNS})
        inception = pd.Timestamp("1900-01-01")
        df["date"] = pd.to_datetime(df["date"]).fillna(inception)

        # Ensure account is defined if rate is other than zero
        missing = list(df["id"][(df["rate"] != 0) & df["account"].isna()])
        if len(missing) > 0:
            raise ValueError(f"Account must be defined for non-zero rate in vat_codes: {missing}.")

        return df

    # ----------------------------------------------------------------------
    # Account chart

    @abstractmethod
    def account_chart(self) -> pd.DataFrame:
        """Retrieves a data frame with all account definitions.

        Returns:
            pd.DataFrame: DataFrame with columns `account` (int), `description` (str),
                          `currency` (str), `vat_code` (str or None). `None` implies
                          VAT is never applicable. If set, VAT is sometimes applicable,
                          and transactions on this account must explicitly state
                          a `vat_code`. The value in the account chart serves as default
                          for new transactions.
        """

    @classmethod
    def standardize_account_chart(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validates and standardizes the 'account_chart' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the account chart.

        Returns:
            pd.DataFrame: The standardized account chart DataFrame.

        Raises:
            ValueError: If required columns are missing, or if 'account' column
                        contains NaN values, or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=REQUIRED_ACCOUNT_COLUMNS.keys())

        # Ensure required columns and values are present
        missing = set(REQUIRED_ACCOUNT_COLUMNS.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns missing in account chart: {missing}.")
        if df["account"].isna().any():
            raise ValueError("Missing 'account' values in account chart.")

        # Add missing columns
        for col in OPTIONAL_ACCOUNT_COLUMNS.keys():
            if col not in df.columns:
                df[col] = None

        return enforce_dtypes(df, {**REQUIRED_ACCOUNT_COLUMNS, **OPTIONAL_ACCOUNT_COLUMNS})

    def account_currency(self, account: int) -> str:
        account_chart = self.account_chart()
        if not int(account) in account_chart["account"].values:
            raise ValueError(f"Account {account} is not defined.")
        return account_chart.loc[account_chart["account"] == account, "currency"].values[0]

    @abstractmethod
    def add_account(
        self, account: int, text: str, currency: str, vat_code: bool = False
    ) -> None:
        """Appends an account to the account chart.

        Args:
            account (int): Unique identifier for the account.
            text (str): Description of the account.
            currency (str): Currency of the account.
            vat_code (bool, optional): Indicates if VAT is applicable. Defaults to False.
        """

    @abstractmethod
    def modify_account(self, account: int, new_data: dict) -> None:
        """Modifies an existing account definition.

        Args:
            account (int): Account to be modified.
            new_data (dict): Fields to be overwritten. Keys typically include
                             `description` (str), `currency` (str), or `vat_code` (str).
        """

    @abstractmethod
    def delete_account(self, account: int, allow_missing: bool = False) -> None:
        """Removes an account from the account chart.

        Args:
            account (int): The account to be removed.
            allow_missing (bool, optional): If True, no error is raised if the account is
                                            not found; if False raises error if
                                            the account is missing. Defaults to False.
        """

    @abstractmethod
    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = False):
        """Aligns the account chart with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes existing accounts that are not
                                     present in the target data.
        """

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
            if account not in self.account_chart()[["account"]].values:
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
        df["base_currency_balance"] = df["base_currency_amount"].cumsum()
        cols = [col for col in LEDGER_COLUMN_SEQUENCE if col in df.columns]
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
            if abs(account) in self.account_chart()["account"].values:
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
                    chart = self.account_chart()
                    in_range = (chart["account"] >= first) & (chart["account"] <= last)
                    accounts = list(chart["account"][in_range])
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

    def sanitize_account_chart(self, accounts: pd.DataFrame) -> pd.DataFrame:
        """Discards inconsistent entries in the account chart.

        Args:
            accounts (pd.DataFrame): Account chart as a DataFrame.

        Returns:
            pd.DataFrame: DataFrame with sanitized account chart.
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
            pd.DataFrame with columns `account` (int), `counter_account`
            (int or None), `currency` (str), `amount` (float),
            `base_currency_amount` (float or None), and `vat_code` (str).
        """

    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with a long representation of all ledger transactions.

        Simple transactions with a credit and debit account are represented twice,
        one with 'account' corresponding to the credit account and one with
        account corresponding to the debit account.

        Returns:
            pd.DataFrame: DataFrame with columns `id` (str), `date` (datetime),
                         `account` (int), `counter_account` (int or None), `currency` (str),
                         `amount` (float), `base_currency_amount` (float or None),
                         `vat_code` (str), and `document` (str).
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
            Columns include: `account` (int), `counter_account` (int, optional),
            `currency` (str), `amount` (float), `base_currency_amount` (float, optional),
            `vat_code` (str, optional).
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
    def delete_ledger_entry(self, id: str) -> None:
        """Deletes a ledger entry by its ID.

        Args:
            id (str): The unique identifier of the ledger entry to delete.
        """

    @abstractmethod
    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
        """Aligns ledger entries with a desired target state.

        Args:
            target (pd.DataFrame): DataFrame with ledger entries in the pyledger format.
            delete (bool, optional): If True, deletes existing ledger that are not
                                     present in the target data.
        """

    def sanitize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Discards inconsistent ledger entries and inconsistent vat codes.

        Logs a warning for each discarded entry with reason for dropping.

        Args:
            ledger (pd.DataFrame): Ledger data as a DataFrame.

        Returns:
            pd.DataFrame: DataFrame with sanitized ledger entries.
        """
        # Discard undefined VAT codes
        ledger["vat_code"] = ledger["vat_code"].str.strip()
        invalid = ledger["vat_code"].notna() & ~ledger["vat_code"].isin(self.vat_codes()["id"])
        if invalid.any():
            df = ledger.loc[invalid, ["id", "vat_code"]]
            df = df.groupby("id").agg({"vat_code": lambda x: x.unique()})
            for id, codes in zip(df["id"].values, df["vat_code"]):
                if len(codes) > 1:
                    self._logger.warning(
                        f"Discard unknown VAT codes {', '.join([f'{x}' for x in codes])} at '{id}'."
                    )
                else:
                    self._logger.warning(f"Discard unknown VAT code '{codes[0]}' at '{id}'.")
            ledger.loc[invalid, "vat_code"] = None

        # Collect postings to be discarded as pd.DataFrame with columns 'id' and
        # 'text'. The latter specifies the reason(s) why the entry is discarded.
        to_discard = []  # List[pd.DataFrame['id', 'text']]

        # 1. Discard journal entries with non-unique dates
        grouped = ledger[["date", "id"]].groupby("id")
        df = grouped.filter(lambda x: x["date"].nunique() > 1)
        if df.shape[0] > 0:
            df = df.drop_duplicates()
            df["text"] = df["date"].astype(pd.StringDtype())
            df = df.groupby("id").agg({"text": lambda x: f"multiple dates {', '.join(x)}"})
            to_discard.append(df.reset_index()[["id", "text"]])

        # 2. Discard journal entries with undefined accounts
        valid_accounts = self.account_chart()["account"].values
        invalid_accounts = []
        for col in ["account", "counter_account"]:
            invalid = ledger[col].notna() & ~ledger[col].isin(valid_accounts)
            if invalid.any():
                df = ledger.loc[invalid, ["id", col]]
                df = df.rename(columns={col: "account"})
                invalid_accounts.append(df)
        if len(invalid_accounts) > 0:
            df = pd.concat(invalid_accounts)
            df = df.groupby("id").agg({"account": lambda x: x.unique()})
            df["text"] = [
                f"Accounts {', '.join([str(y) for y in x])} not defined"
                if len(x) > 1
                else f"Account {x[0]} not defined"
                for x in df["account"]
            ]
            to_discard.append(df.reset_index()[["id", "text"]])

        # 2. Discard journal entries without amount or balance or with both
        if "target_balance" in ledger.columns:
            invalid = ledger["amount"].isna() & ledger["target_balance"].isna()
        else:
            invalid = ledger["amount"].isna()
        if invalid.any():
            df = pd.DataFrame({"id": ledger.loc[invalid, "id"].unique(), "text": "amount missing"})
            to_discard.append(df)
        if "target_balance" in ledger.columns:
            invalid = ledger["amount"].notna() & ledger["target_balance"].notna()
            if invalid.any():
                df = pd.DataFrame({
                    "id": ledger.loc[invalid, "id"].unique(),
                    "text": "both amount and target amount defined"
                })
                to_discard.append(df)

        # Discard journal postings identified above
        if len(to_discard) > 0:
            df = pd.concat(to_discard)
            df = df.groupby("id").agg({"text": lambda x: ", ".join(x)})
            for id, text in zip(df.index, df["text"]):
                self._logger.warning(f"Discard ledger entry '{id}': {text}.")
            ledger = ledger.loc[~ledger["id"].isin(df.index), :]
        # # Add account chart VAT codes for 'account' and 'counter_account'
        # account_chart = self.account_chart[['account', 'vat_code']]
        # df = df.merge(account_chart, on='account', how='left',
        #               suffixes=('', '_account'))
        # df = df.merge(account_chart, left_on='account',
        #               right_on='counter_account', how='left',
        #               suffixes=('', '_counter_account'))

        # # Check whether vat_codes match account definition
        # failed = (df['vat_code_account'].notna() &
        #           df['vat_code_counter_account'].notna())
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries where both "
        #                   f"account and counter_account have VAT codes in the "
        #                   f"account chart: {df.index[failed]}.", UserWarning)

        # failed = (df['vat_code'].notna()
        #           & df['vat_code_account'].isna()
        #           & df['vat_code_counter_account'].isna())
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries with VAT code "
        #                   f"where neither account and counter_account have VAT "
        #                   f"codes in the account chart: {df.index[failed]}.",
        #                   UserWarning)

        # failed = (df['vat_code'].isna()
        #           & (df['vat_code_account'].notna()
        #              | df['vat_code_counter_account'].notna()))
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries without VAT "
        #                   f"code where either account and counter_account "
        #                   f"requires a VAT code: {df.index[failed]}.",
        #                   UserWarning)

        return ledger

    @staticmethod
    def standardize_ledger_columns(ledger: pd.DataFrame = None) -> pd.DataFrame:
        """Standardizes and enforces type consistency for the ledger DataFrame.

        Ensures that the required columns are present in the ledger DataFrame,
        adds any missing optional columns with None values, and enforces
        specific data types for each column.

        Args:
            ledger (pd.DataFrame, optional): A data frame with ledger transactions
                                             Defaults to None.

        Returns:
            pd.DataFrame: A standardized DataFrame with both the required and
            optional columns with enforced data types.

        Raises:
            ValueError: If required columns are missing from the ledger DataFrame.
        """
        if ledger is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=REQUIRED_LEDGER_COLUMNS.keys())
        else:
            df = ledger.copy()
            # Standardize column names
            df = df.rename(columns=LEDGER_COLUMN_SHORTCUTS)

        # In empty DataFrames, add required columns if not present
        if isinstance(ledger, pd.DataFrame) and len(ledger) == 0:
            for col in REQUIRED_LEDGER_COLUMNS.keys():
                if col not in df.columns:
                    df[col] = None

        # Ensure all required columns are present
        missing = set(REQUIRED_LEDGER_COLUMNS.keys()) - set(df.columns)
        if len(missing) > 0:
            raise ValueError(f"Missing required columns: {missing}")

        # Add optional columns if not present
        if "id" not in df.columns:
            df["id"] = df["date"].notna().cumsum().astype(pd.StringDtype())
        for col in OPTIONAL_LEDGER_COLUMNS.keys():
            if col not in df.columns:
                df[col] = None

        # Enforce column data types
        df = enforce_dtypes(df, {**REQUIRED_LEDGER_COLUMNS, **OPTIONAL_LEDGER_COLUMNS})
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Order columns based on 'LEDGER_COLUMN_SEQUENCE'
        col_order = LEDGER_COLUMN_SEQUENCE
        cols = (
            [col for col in col_order if col in df.columns]
            + [col for col in df.columns if col not in col_order]
        )
        df = df[cols]

        return df

    def standardize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Convert ledger entries to a canonical representation.

        This method converts ledger entries into a standardized format. It
        ensures uniformity where transactions can be defined in various
        equivalent ways, allowing for easy identification of equivalent
        entries.

        Args:
            ledger (pd.DataFrame): A data frame with ledger transactions.

        Returns:
            pd.DataFrame: A DataFrame with ledger entries in canonical form.

        Notes:
            - The method removes redundant 'base_currency_amount' values for
            transactions in the base currency.
            - It fills missing dates in collective transactions with dates from
            other line items in the same collective transaction.
        """
        df = self.standardize_ledger_columns(ledger)

        # Fill missing (NA) dates
        df["date"] = df.groupby("id")["date"].ffill()
        df["date"] = df.groupby("id")["date"].bfill()

        # Drop redundant base_currency_amount for transactions in base currency
        set_na = (
            (df["currency"] == self.base_currency)
            & (df["base_currency_amount"].isna() | (df["base_currency_amount"] == df["amount"]))
        )
        df.loc[set_na, "base_currency_amount"] = pd.NA

        # Remove leading and trailing spaces in strings, convert -0.0 to 0.0
        for col in df.columns:
            if pd.StringDtype.is_dtype(df[col]):
                df[col] = df[col].str.strip()
            elif pd.Float64Dtype.is_dtype(df[col]):
                df[col] = np.where(df[col].notna() & (df[col] == 0), 0.0, df[col])

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
        cols = [
            "id", "date", "account", "counter_account", "currency", "amount",
            "base_currency_amount", "vat_code", "text", "document",
        ]
        credit = df[cols]
        debit = credit.copy()
        debit["amount"] *= -1.0
        debit["base_currency_amount"] *= -1.0
        debit["account"] = df["counter_account"]
        debit["counter_account"] = df["account"]
        # Combine credit and debit entries
        result = pd.concat([
            credit.loc[credit["account"].notna()],
            debit.loc[debit["account"].notna()]
        ])
        return result[cols]

    # ----------------------------------------------------------------------
    # Currency

    @property
    @abstractmethod
    def base_currency(self) -> str:
        """Reporting currency of the ledger system."""

    @base_currency.setter
    @abstractmethod
    def base_currency(self, currency):
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
            overwrite (bool, optional): Overwrite an existing price definition with the same ticker,
                                        date, and currency if one exists. Defaults to False.
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
    def standardize_price_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validates and standardizes the 'prices' DataFrame to ensure it contains
        the required columns, correct data types, and no missing values in key fields.

        Args:
            df (pd.DataFrame): The DataFrame representing the prices.

        Returns:
            pd.DataFrame: The standardized prices DataFrame.

        Raises:
            ValueError: If required columns are missing, if there are missing values
                        in required columns, or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=REQUIRED_PRICE_COLUMNS.keys())

        # Check for missing required columns
        missing = set(REQUIRED_PRICE_COLUMNS.keys()) - set(df.columns)
        if len(missing) > 0:
            raise ValueError(f"Required columns {missing} missing.")

        # Check for missing values in required columns
        has_missing_value = [
            column for column in REQUIRED_PRICE_COLUMNS.keys() if df[column].isnull().any()
        ]
        if len(has_missing_value) > 0:
            raise ValueError(f"Missing values in column {has_missing_value}.")

        # Enforce data types
        df = enforce_dtypes(df, REQUIRED_PRICE_COLUMNS)

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
