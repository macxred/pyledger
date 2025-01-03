"""This module defines Abstract base class for a double entry accounting system."""

from abc import ABC, abstractmethod
import datetime
import logging
import math
import zipfile
import json
from pathlib import Path
from typing import Dict, List
from consistent_df import enforce_schema, df_to_consistent_str, nest
import re
import numpy as np
import openpyxl
import pandas as pd
from .decorators import timed_cache
from .constants import (
    ACCOUNT_SCHEMA,
    ASSETS_SCHEMA,
    LEDGER_SCHEMA,
    PRICE_SCHEMA,
    REVALUATION_SCHEMA,
    TAX_CODE_SCHEMA,
    DEFAULT_ASSETS,
)
from .storage_entity import AccountingEntity
from . import excel
from .helpers import first_elements_as_str, represents_integer
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
    # Storage entities

    @property
    def accounts(self) -> AccountingEntity:
        return self._accounts

    @property
    def assets(self) -> AccountingEntity:
        return self._assets

    @property
    def ledger(self) -> AccountingEntity:
        return self._ledger

    @property
    def revaluations(self) -> AccountingEntity:
        return self._revaluations

    @property
    def tax_codes(self) -> AccountingEntity:
        return self._tax_codes

    @property
    def price_history(self) -> AccountingEntity:
        return self._price_history

    @property
    def profit_centers(self) -> AccountingEntity:
        return self._profit_centers

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
        for account in self.accounts.list().index.sort_values():
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
            archive.writestr('settings.json', json.dumps(self.settings_list()))
            archive.writestr('ledger.csv', self.ledger.list().to_csv(index=False))
            archive.writestr('assets.csv', self.assets.list().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts.list().to_csv(index=False))
            archive.writestr('tax_codes.csv', self.tax_codes.list().to_csv(index=False))
            archive.writestr('revaluations.csv', self.revaluations.list().to_csv(index=False))
            archive.writestr('price_history.csv', self.price_history.list().to_csv(index=False))
            archive.writestr('profit_centers.csv', self.profit_centers.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        """Restore ledger system from a ZIP archive.

        Restores a dumped ledger system from a ZIP archive.
        Extracts the accounts, tax codes, ledger entries, reporting currency, etc.,
        from the ZIP archive and passes the extracted data to the `restore` method.

        Args:
            archive_path (str): The file path of the ZIP archive to restore.
        """
        required_files = {
            'ledger.csv', 'tax_codes.csv', 'accounts.csv', 'settings.json', 'assets.csv',
            'price_history.csv', 'revaluations.csv', 'profit_centers.csv'
        }

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
            price_history = pd.read_csv(archive.open('price_history.csv'))
            revaluations = pd.read_csv(archive.open('revaluations.csv'))
            profit_centers = pd.read_csv(archive.open('profit_centers.csv'))
            self.restore(
                settings=settings,
                ledger=ledger,
                tax_codes=tax_codes,
                accounts=accounts,
                assets=assets,
                price_history=price_history,
                revaluations=revaluations,
                profit_centers=profit_centers
            )

    def restore(
        self,
        settings: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        ledger: pd.DataFrame | None = None,
        assets: pd.DataFrame | None = None,
        price_history: pd.DataFrame | None = None,
        revaluations: pd.DataFrame | None = None,
        profit_centers: pd.DataFrame | None = None,
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
                If `None`, assets remain unchanged.
            price_history (pd.DataFrame | None): Price history of the restored system.
                If `None`, price history remains unchanged.
            revaluations (pd.DataFrame | None): Revaluations of the restored system.
                If `None`, revaluations remain unchanged.
            profit_centers (pd.DataFrame | None): Profit centers of the restored system.
                If `None`, profit centers remain unchanged.
        """
        if settings is not None:
            self.settings_modify(settings)
        if assets is not None:
            self.assets.mirror(assets, delete=True)
        if price_history is not None:
            self.price_history.mirror(price_history, delete=True)
        if revaluations is not None:
            self.revaluations.mirror(revaluations, delete=True)
        if tax_codes is not None:
            self.tax_codes.mirror(tax_codes, delete=True)
        if accounts is not None:
            self.accounts.mirror(accounts, delete=True)
        if profit_centers is not None:
            self.profit_centers.mirror(profit_centers, delete=True)
        if ledger is not None:
            self.ledger.mirror(ledger, delete=True)

    def clear(self):
        """Clear all data from the ledger system.

        This method removes all entries from the ledger, tax codes, accounts, etc.
        restoring the system to a pristine state.
        """
        self.ledger.mirror(None, delete=True)
        self.tax_codes.mirror(None, delete=True)
        self.accounts.mirror(None, delete=True)
        self.assets.mirror(None, delete=True)
        self.price_history.mirror(None, delete=True)
        self.revaluations.mirror(None, delete=True)
        self.profit_centers.mirror(None, delete=True)

    # ----------------------------------------------------------------------
    # Settings

    def settings_list(self) -> dict:
        """Return a dict with all settings."""
        return {"REPORTING_CURRENCY": self.reporting_currency}

    def settings_modify(self, settings: dict = {}):
        """Modify provided settings."""
        if "REPORTING_CURRENCY" in settings:
            self.reporting_currency = settings["REPORTING_CURRENCY"]

    # ----------------------------------------------------------------------
    # Tax rates

    def sanitize_tax_codes(self, df: pd.DataFrame, accounts: pd.DataFrame = None) -> pd.DataFrame:
        """Discard incoherent tax code data.

        Discard tax code entries that have invalid rates, missing account references
        for non-zero rates, or references to non-existent accounts. Removed entries
        are logged as warnings.

        If no accounts DataFrame is provided, it will be fetched via the `accounts.list()` method,
        as the validity of tax codes depends on the existence of these accounts.

        Args:
            df (pd.DataFrame): The raw tax_codes DataFrame to validate.
            accounts (pd.DataFrame, optional): Accounts DataFrame for reference validation.

        Returns:
            pd.DataFrame: The sanitized DataFrame with valid tax code entries.
        """
        df = enforce_schema(df, schema=TAX_CODE_SCHEMA, keep_extra_columns=True)

        # Validate rates (must be between 0 and 1)
        invalid_rates = (df["rate"] < 0) | (df["rate"] > 1)
        if invalid_rates.any():
            invalid = df.loc[invalid_rates, "id"].tolist()
            self._logger.warning(
                f"Discarding {len(invalid)} tax codes with invalid rates: "
                f"{first_elements_as_str(invalid)}."
            )
            df = df.loc[~invalid_rates]

        # Use current accounts if none provided
        if accounts is None:
            accounts = self.sanitize_accounts(self.accounts.list(), tax_codes=df)

        # Ensure account/contra is defined for non-zero rates
        missing_accounts = (df["rate"] != 0) & df["account"].isna() & df["contra"].isna()
        if missing_accounts.any():
            invalid = df.loc[missing_accounts, "id"].tolist()
            self._logger.warning(
                f"Discarding {len(invalid)} tax codes with non-zero rates and missing "
                f"accounts/contra: {first_elements_as_str(invalid)}."
            )
            df = df.loc[~missing_accounts]

        # Validate referenced accounts
        valid_accounts = set(accounts["account"])
        invalid_accounts_mask = df["account"].notna() & ~df["account"].isin(valid_accounts)
        invalid_contra_mask = df["contra"].notna() & ~df["contra"].isin(valid_accounts)
        invalid_mask = invalid_accounts_mask | invalid_contra_mask
        if invalid_mask.any():
            invalid = df.loc[invalid_mask, "id"].tolist()
            self._logger.warning(
                f"Discarding {len(invalid)} tax codes with non-existent accounts: "
                f"{first_elements_as_str(invalid)}."
            )
            df = df.loc[~invalid_mask]

        return df.reset_index(drop=True)

    # ----------------------------------------------------------------------
    # Accounts

    def sanitize_accounts(self, df: pd.DataFrame, tax_codes: pd.DataFrame = None) -> pd.DataFrame:
        """Discard incoherent account data.

        Discard accounts with invalid currencies, and set invalid tax code references
        to NA. Removed or modified entries are logged as warnings.

        If no tax_codes DataFrame is provided, it will be fetched via the `tax_codes.list()` method,
        as the validity of accounts may depend on referenced tax codes.

        Args:
            df (pd.DataFrame): The raw accounts DataFrame to validate.
            tax_codes (pd.DataFrame, optional): Tax codes DataFrame for reference validation.

        Returns:
            pd.DataFrame: The sanitized DataFrame with valid account entries.
        """
        df = enforce_schema(df, ACCOUNT_SCHEMA, keep_extra_columns=True)

        def validate_currency() -> pd.Series:
            """Validate that each account's currency is supported using the precision() method."""
            def is_valid(row):
                try:
                    self.precision(ticker=row["currency"])
                    return True
                except ValueError:
                    return False
            return df.apply(is_valid, axis=1)

        # Validate currencies
        valid_currency_mask = validate_currency()
        if not valid_currency_mask.all():
            invalid = df.loc[~valid_currency_mask, "account"].tolist()
            self._logger.warning(
                f"Discarding {len(invalid)} accounts with invalid currencies: "
                f"{first_elements_as_str(invalid)}."
            )
            df = df.loc[valid_currency_mask]

        # Validate tax codes
        if tax_codes is None:
            tax_codes = self.sanitize_tax_codes(self.tax_codes.list(), accounts=df)
        valid_tax_codes = set(tax_codes["id"])

        invalid_tax_code_mask = df["tax_code"].notna() & ~df["tax_code"].isin(valid_tax_codes)
        if invalid_tax_code_mask.any():
            invalid = df.loc[invalid_tax_code_mask, "account"].tolist()
            self._logger.warning(
                f"Found {len(invalid)} accounts with invalid tax code references: "
                f"{first_elements_as_str(invalid)}. Setting 'tax_code' to NA for these entries."
            )
            df.loc[invalid_tax_code_mask, "tax_code"] = pd.NA

        return df.reset_index(drop=True)

    def sanitized_accounts_tax_codes(self):
        """Orchestrates the sanitization of interdependent accounts and tax codes DataFrames.

        This method handles their mutual dependencies in a multi-step process:
        1. Sanitize accounts with no confirmed tax_codes, setting invalid ones to NA.
        2. Sanitize tax_codes using the partially sanitized accounts.
        3. Re-sanitize accounts now that we have a validated tax_codes DataFrame, ensuring
           accounts reference only valid, sanitized tax codes.

        Logs warnings for each discarded or adjusted entry. This stepwise process ensures
        both DataFrames end up coherent with each other.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: The sanitized accounts and tax_codes DataFrames.
        """
        # Step 1: Partial accounts sanitization
        raw_accounts = self.accounts.list()
        raw_tax_codes = self.tax_codes.list()
        accounts_df_step1 = self.sanitize_accounts(raw_accounts, tax_codes=raw_tax_codes)

        # Step 2: Sanitize tax_codes using partially sanitized accounts
        tax_codes_df = self.sanitize_tax_codes(raw_tax_codes, accounts=accounts_df_step1)

        # Step 3: Re-validate accounts with the now fully validated tax_codes
        accounts_df_final = self.sanitize_accounts(raw_accounts, tax_codes=tax_codes_df)

        return accounts_df_final, tax_codes_df

    def account_currency(self, account: int) -> str:
        accounts = self.accounts.list()
        if not int(account) in accounts["account"].values:
            raise ValueError(f"Account {account} is not defined.")
        return accounts.loc[accounts["account"] == account, "currency"].values[0]

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

        # Type consistent return value Dict[str, float]
        def _standardize_currency(ticker: str, x: float) -> float:
            result = float(self.round_to_precision(x, ticker=ticker, date=date))
            if result == -0.0:
                result = 0.0
            return result
        result = {k: _standardize_currency(k, v) for k, v in result.items()}
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
            if account not in self.accounts.list()[["account"]].values:
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
            filter = filter & (ledger["date"] <= pd.to_datetime(end))
        df = ledger.loc[filter, :]
        df = df.sort_values("date")
        df.insert(df.columns.get_loc("amount") + 1, "balance", df["amount"].cumsum())
        df.insert(df.columns.get_loc("report_amount") + 1,
                  "report_balance", df["report_amount"].cumsum())
        if start is not None:
            df = df.loc[df["date"] >= pd.to_datetime(start), :]
        return df.reset_index(drop=True)

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
            if abs(account) in self.accounts.list()["account"].values:
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
                    accounts = self.accounts.list()
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

    # ----------------------------------------------------------------------
    # Ledger

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
        return self.serialize_ledger(self.ledger.list())

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
        invalid = ledger["tax_code"].notna() & ~ledger["tax_code"].isin(self.tax_codes.list()["id"])
        if invalid.any():
            df = ledger.loc[invalid, ["id", "tax_code"]]
            df = df.groupby("id").agg({"tax_code": lambda x: x.unique()})
            for id, codes in df.iterrows():
                if len(codes) > 1:
                    self._logger.warning(
                        f"Discard unknown tax codes {', '.join([f'{x}' for x in codes])} at '{id}'."
                    )
                else:
                    self._logger.warning(f"Discard unknown tax code '{codes.iloc[0]}' at '{id}'.")
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
        valid_accounts = self.accounts.list()["account"].values
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
        df = self.ledger.standardize(df)
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

    def report_amount(
        self, amount: list[float], currency: list[str], date: list[datetime.date]
    ) -> list[float]:
        """Converts a list of amounts in various currencies to the reporting currency.

        Args:
            amount (list[float]): List of amounts in the respective currencies.
            currency (list[str]): List of currency codes corresponding to the amounts.
            date (list[datetime.date]): List of dates for the currency conversion.

        Returns:
            list[float]: List of amounts converted to the reporting currency, rounded
                to the appropriate precision.

        Raises:
            ValueError: If the lengths of `amount`, `currency`, and `date` are not equal.
        """

        reporting_currency = self.reporting_currency
        if not (len(amount) == len(currency) == len(date)):
            raise ValueError("Vectors 'amount', 'currency', and 'date' must have the same length.")
        result = [
            self.round_to_precision(
                a * self.price(t, date=d, currency=reporting_currency)[1],
                reporting_currency, date=d)
            for a, t, d in zip(amount, currency, date)]
        return result

    # ----------------------------------------------------------------------
    # Price

    def sanitize_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard incoherent price data.

        Discard price entries referencing tickers or currencies not defined as
        assets. Removed entries are logged as warnings.

        Args:
            df (pd.DataFrame): The input DataFrame with price data to validate.

        Returns:
            pd.DataFrame: The sanitized DataFrame with valid price entries.
        """
        df = enforce_schema(df, PRICE_SCHEMA, keep_extra_columns=True)

        # The `precision` method validates the existence of a valid asset definition
        # and relies on sanitized data, serving as a centralized validation point.
        def invalid_asset_reference(ticker: pd.Series, date: pd.Series) -> pd.Series:
            """Validate specified column values for asset references."""
            def is_valid(ticker, date):
                try:
                    self.precision(ticker=ticker, date=date)
                    return False
                except ValueError:
                    return True
            return pd.Series([is_valid(ticker=t, date=d) for t, d in zip(ticker, date)])

        invalid_tickers_mask = invalid_asset_reference(df["ticker"], df["date"])
        invalid_currencies_mask = invalid_asset_reference(df["currency"], df["date"])

        if invalid_tickers_mask.any():
            invalid_tickers = df.loc[invalid_tickers_mask, "ticker"].unique().tolist()
            self._logger.warning(
                f"Discard {len(invalid_tickers)} price entries with invalid "
                f"tickers: {first_elements_as_str(invalid_tickers)}."
            )
        if invalid_currencies_mask.any():
            invalid_currencies = df.loc[invalid_currencies_mask, "currency"].unique().tolist()
            self._logger.warning(
                f"Discard {len(invalid_currencies)} price entries with invalid "
                f"currencies: {first_elements_as_str(invalid_currencies)}."
            )

        invalid_mask = invalid_tickers_mask | invalid_currencies_mask
        df = df[~invalid_mask].reset_index(drop=True)
        return df

    def price(
        self,
        ticker: str,
        date: datetime.date = None,
        currency: str | None = None
    ) -> tuple[str, float]:
        """Retrieve price for a given ticker as of a specified date. If no price is available
        on the exact date, return latest price observation prior to the specified date.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date for which the price is required.
            currency (str, optional): Currency in which the price is desired.

        Returns:
            tuple: (currency, price) where 'currency' is a string indicating the currency
            of the price, and 'price' is a float representing the asset's price as of the
            specified date.
        """
        if currency is not None and str(ticker) == str(currency):
            return (currency, 1.0)

        if date is None:
            date = datetime.date.today()
        elif not isinstance(date, datetime.date):
            date = pd.to_datetime(date).date()

        if ticker not in self._prices_as_dict_of_df:
            raise ValueError(f"No price data available for '{ticker}'.")

        if currency is None:
            # Assuming the first currency is the default if none specified
            currency = next(iter(self._prices_as_dict_of_df[ticker]))

        if currency not in self._prices_as_dict_of_df[ticker]:
            raise ValueError(f"No {currency} prices available for '{ticker}'.")

        prc = self._prices_as_dict_of_df[ticker][currency]
        prc = prc.loc[prc["date"].dt.normalize() <= pd.Timestamp(date), "price"]

        if prc.empty:
            raise ValueError(f"No {currency} prices available for '{ticker}' before {date}.")

        return (currency, prc.iloc[-1].item())

    @property
    @timed_cache(15)
    def _prices_as_dict_of_df(self) -> Dict[str, pd.DataFrame]:
        """Organizes price data by ticker and currency for quick access.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: Maps each asset ticker to
            a nested dictionary of DataFrames by currency, with its
            `price` history sorted by `date` with `NaT` values first.
        """
        result = {}
        prices = self.sanitize_prices(self.price_history.list()).groupby(["ticker", "currency"])
        for (ticker, currency), group in prices:
            group = group[["date", "price"]].sort_values("date", na_position="first")
            group = group.reset_index(drop=True)
            if ticker not in result.keys():
                result[ticker] = {}
            result[ticker][currency] = group
        return result

    # ----------------------------------------------------------------------
    # Assets

    def sanitize_assets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard incoherent asset data.

        Discard entries with negative increment and log removed entries as
        warnings.

        Args:
            df (pd.DataFrame): The input DataFrame with assets to validate.

        Returns:
            pd.DataFrame: The sanitized DataFrame with valid asset entries.
        """
        df = enforce_schema(df, ASSETS_SCHEMA, keep_extra_columns=True)

        # Validate increment > 0
        invalid_increment_mask = df["increment"] <= 0
        if invalid_increment_mask.any():
            id_columns = ASSETS_SCHEMA.query("id == True")["column"].tolist()
            invalid_assets = df.loc[invalid_increment_mask, id_columns].to_dict("records")
            self._logger.warning(
                f"Discarding assets with non-positive increments: "
                f"{first_elements_as_str(invalid_assets)}."
            )
            df = df[~invalid_increment_mask].reset_index(drop=True)

        return df

    @property
    @timed_cache(15)
    def _assets_as_dict_of_df(self) -> Dict[str, pd.DataFrame]:
        """Organize assets by ticker for quick access.

        Splits assets by ticker for efficient lookup of increments by ticker
        and date.

        Returns:
            Dict[str, pd.DataFrame]: Maps each asset ticker to a DataFrame of
            its `increment` history, sorted by `date` with `NaT` values first.
        """
        return {
            ticker: (
                group[["date", "increment"]]
                .sort_values("date", na_position="first")
                .reset_index(drop=True)
            )
            for ticker, group in self.sanitize_assets(self.assets.list()).groupby("ticker")
        }

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        """Returns the smallest price increment of an asset or currency.

        This is the precision, to which prices should be rounded.

        Args:
            ticker (str): Identifier of the currency or asset.
            date (datetime.date, optional): Date for which to retrieve the precision.
                                            Defaults to today's date.

        Returns:
            float: The smallest price increment.
        """

        if ticker == "reporting_currency":
            ticker = self.reporting_currency

        if date is None:
            date = datetime.date.today()
        elif not isinstance(date, datetime.date):
            date = pd.to_datetime(date).date()

        asset = self._assets_as_dict_of_df.get(ticker)
        if asset is None:
            # Asset is not defined by the user, fall back to hard-coded defaults
            increment = DEFAULT_ASSETS.loc[DEFAULT_ASSETS["ticker"] == ticker, "increment"]
            if len(increment) < 1:
                raise ValueError(f"No asset definition available for ticker '{ticker}'.")
            if len(increment) > 1:
                raise ValueError(f"Multiple default definitions for asset '{ticker}'.")
            return increment.item()
        else:
            mask = asset["date"].isna() | (asset["date"] <= pd.Timestamp(date))
            if not mask.any():
                raise ValueError(
                    f"No asset definition available for '{ticker}' on or before {date}."
                )
            return asset.loc[mask[mask].index[-1], "increment"].item()

    # ----------------------------------------------------------------------
    # Revaluations

    def sanitize_revaluations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Discard incoherent revaluation data.

        Discard revaluation entries with invalid dates, missing credit/debit fields,
        invalid accounts, or missing price definitions for required currencies.
        Removed entries are logged as warnings.

        Args:
            df (pd.DataFrame): The input DataFrame with revaluation data to validate.

        Returns:
            pd.DataFrame: The sanitized DataFrame with valid revaluation entries.
        """
        # Enforce schema
        df = enforce_schema(df, REVALUATION_SCHEMA, keep_extra_columns=True)
        id_columns = REVALUATION_SCHEMA.query("id == True")["column"].tolist()

        # Validate date: discard rows with invalid dates (NaT)
        invalid_date_mask = df["date"].isna()
        if invalid_date_mask.any():
            invalid = df.loc[invalid_date_mask, id_columns].to_dict(orient='records')
            self._logger.warning(
                f"Discarding {len(invalid)} revaluation rows with invalid dates: "
                f"{first_elements_as_str(invalid)}"
            )
            df = df.loc[~invalid_date_mask]

        # TODO: use sanitized accounts
        valid_accounts = set(self.accounts.list()["account"])

        # Ensure at least one of credit or debit is specified
        both_missing_mask = df["credit"].isna() & df["debit"].isna()
        if both_missing_mask.any():
            invalid = df.loc[both_missing_mask, id_columns].to_dict(orient='records')
            self._logger.warning(
                f"Discarding {len(invalid)} revaluations with no credit nor debit specified: "
                f"{first_elements_as_str(invalid)}"
            )
            df = df.loc[~both_missing_mask]

        # Ensure non-missing credit and debit accounts exist in the accounts entity
        invalid_credit_mask = ~df["credit"].isna() & ~df["credit"].isin(valid_accounts)
        invalid_debit_mask = ~df["debit"].isna() & ~df["debit"].isin(valid_accounts)
        invalid_account_mask = invalid_credit_mask | invalid_debit_mask
        if invalid_account_mask.any():
            invalid = df.loc[invalid_account_mask, id_columns].to_dict(orient='records')
            self._logger.warning(
                f"Discarding {len(invalid)} revaluations with non-existent "
                f"credit or debit accounts: {first_elements_as_str(invalid)}"
            )
            df = df.loc[~invalid_account_mask]

        def validate_account_prices(accounts: pd.Series, dates: pd.Series) -> pd.Series:
            """
            Validate that for each row's accounts, all required price definitions are available.

            For each row, this checks all associated accounts and uses `price()` to ensure
            that a conversion rate exists for their currencies. Rows lacking a required
            price definition are marked invalid.
            """
            valid_list = []
            for acc, d in zip(accounts, dates):
                accounts_range = self.account_range(acc)
                accounts_set = set(accounts_range["add"]) - set(accounts_range["subtract"])
                # Assume this row is valid until a missing price definition is found
                all_valid = True
                for a in accounts_set:
                    acc_curr = self.account_currency(a)
                    if acc_curr != self.reporting_currency:
                        try:
                            self.price(ticker=acc_curr, date=d, currency=self.reporting_currency)
                        except Exception:
                            # If a price definition is missing or any error occurs, mark invalid
                            all_valid = False
                            break
                valid_list.append(all_valid)
            return pd.Series(valid_list, index=accounts.index)

        currency_validation_mask = validate_account_prices(df["account"], df["date"])
        if not currency_validation_mask.all():
            invalid = df.loc[~currency_validation_mask, id_columns].to_dict(orient='records')
            self._logger.warning(
                f"Discarding {len(invalid)} revaluation rows with no price definition "
                f"for required currencies: {first_elements_as_str(invalid)}"
            )
            df = df.loc[currency_validation_mask]

        return df.reset_index(drop=True)
