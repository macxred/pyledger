"""This module defines Abstract base class for a double entry accounting system."""

from abc import ABC, abstractmethod
from collections import Counter
import datetime
import logging
import math
import zipfile
import json
from pathlib import Path
from typing import Dict, Literal
from consistent_df import enforce_schema, df_to_consistent_str, nest
import re
import numpy as np
import openpyxl
import pandas as pd
import polars as pl
from .decorators import timed_cache
from .constants import (
    ACCOUNT_BALANCE_SCHEMA,
    ACCOUNT_SCHEMA,
    ACCOUNT_HISTORY_SCHEMA,
    ACCOUNT_SHEET_REPORT_CONFIG_SCHEMA,
    ASSETS_SCHEMA,
    DEFAULT_DATE,
    JOURNAL_SCHEMA,
    PRICE_SCHEMA,
    PROFIT_CENTER_SCHEMA,
    RECONCILIATION_SCHEMA,
    REVALUATION_SCHEMA,
    TARGET_BALANCE_SCHEMA,
    TAX_CODE_SCHEMA,
    DEFAULT_ASSETS,
    AGGREGATED_BALANCE_SCHEMA
)
from .storage_entity import AccountingEntity
from . import excel
from .helpers import first_elements_as_str, prune_path, represents_integer
from .time import parse_date_span
from .typst import (
    df_to_typst,
    format_threshold,
    format_typst_text,
    format_typst_links,
)


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
    def journal(self) -> AccountingEntity:
        return self._journal

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
        Each component of the ledger system (accounts, tax_codes, journal entries,
        configuration, etc.) is stored as an individual file inside the ZIP archive
        for modular restoration and analysis.

        Args:
            archive_path (str): The file path of the ZIP archive.
        """
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('configuration.json', json.dumps(self.configuration_list()))
            archive.writestr('journal.csv', self.journal.list().to_csv(index=False))
            archive.writestr('assets.csv', self.assets.list().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts.list().to_csv(index=False))
            archive.writestr('tax_codes.csv', self.tax_codes.list().to_csv(index=False))
            archive.writestr('revaluations.csv', self.revaluations.list().to_csv(index=False))
            archive.writestr('price_history.csv', self.price_history.list().to_csv(index=False))
            archive.writestr('profit_centers.csv', self.profit_centers.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        """Restore ledger system from a ZIP archive.

        Restores a dumped ledger system from a ZIP archive.
        Extracts the accounts, tax codes, journal entries, reporting currency, etc.,
        from the ZIP archive and passes the extracted data to the `restore` method.

        Args:
            archive_path (str): The file path of the ZIP archive to restore.
        """
        required_files = {
            'journal.csv', 'tax_codes.csv', 'accounts.csv', 'configuration.json', 'assets.csv',
            'price_history.csv', 'revaluations.csv', 'profit_centers.csv'
        }

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            configuration = json.loads(archive.open('configuration.json').read().decode('utf-8'))
            journal = pd.read_csv(archive.open('journal.csv'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            tax_codes = pd.read_csv(archive.open('tax_codes.csv'))
            assets = pd.read_csv(archive.open('assets.csv'))
            price_history = pd.read_csv(archive.open('price_history.csv'))
            revaluations = pd.read_csv(archive.open('revaluations.csv'))
            profit_centers = pd.read_csv(archive.open('profit_centers.csv'))
            self.restore(
                configuration=configuration,
                journal=journal,
                tax_codes=tax_codes,
                accounts=accounts,
                assets=assets,
                price_history=price_history,
                revaluations=revaluations,
                profit_centers=profit_centers
            )

    def restore(
        self,
        configuration: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        journal: pd.DataFrame | None = None,
        assets: pd.DataFrame | None = None,
        price_history: pd.DataFrame | None = None,
        revaluations: pd.DataFrame | None = None,
        profit_centers: pd.DataFrame | None = None,
    ):
        """Replaces the entire ledger system with data provided as arguments.

        Args:
            configuration (dict | None): System configuration.
                If `None`, configuration remains unchanged.
            tax_codes (pd.DataFrame | None): Tax codes of the restored ledger system.
                If `None`, tax codes remain unchanged.
            accounts (pd.DataFrame | None): Accounts of the restored ledger system.
                If `None`, accounts remain unchanged.
            journal (pd.DataFrame | None): Journal entries of the restored system.
                If `None`, journal remains unchanged.
            assets (pd.DataFrame | None): Assets entries of the restored system.
                If `None`, assets remain unchanged.
            price_history (pd.DataFrame | None): Price history of the restored system.
                If `None`, price history remains unchanged.
            revaluations (pd.DataFrame | None): Revaluations of the restored system.
                If `None`, revaluations remain unchanged.
            profit_centers (pd.DataFrame | None): Profit centers of the restored system.
                If `None`, profit centers remain unchanged.
        """
        if configuration is not None:
            self.configuration_modify(configuration)
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
        if journal is not None:
            self.journal.mirror(journal, delete=True)

    def clear(self):
        """Clear all data from the ledger system.

        This method removes all entries from the journal, tax codes, accounts, etc.
        restoring the system to a pristine state.
        """
        self.journal.mirror(None, delete=True)
        self.tax_codes.mirror(None, delete=True)
        self.accounts.mirror(None, delete=True)
        self.assets.mirror(None, delete=True)
        self.price_history.mirror(None, delete=True)
        self.revaluations.mirror(None, delete=True)
        self.profit_centers.mirror(None, delete=True)

    # ----------------------------------------------------------------------
    # Configuration

    @staticmethod
    def standardize_configuration(configuration: dict) -> dict:
        """Validates and standardizes the 'configuration' dictionary. Ensures it
        contains items 'reporting_currency' and 'precision'.

        Example:
            configuration = {
                'reporting_currency': 'USD',
                'precision': {
                    'CAD': 0.01, 'CHF': 0.01, 'EUR': 0.01,
                    'GBP': 0.01, 'HKD': 0.01, 'USD': 0.01
                }
            }
            LedgerEngine.standardize_configuration(configuration)

        Args:
            configuration (dict): The configuration dictionary to be standardized.

        Returns:
            dict: The standardized configuration dictionary.

        Raises:
            ValueError: If 'configuration' is not a dictionary, or if 'reporting_currency'
                        is missing/not a string, or if 'precision' is missing/not a
                        dictionary, or if any key/value in 'precision' is of invalid
                        type.

        Note:
            Modifies 'precision' to include the 'reporting_currency' key.
        """
        if not isinstance(configuration, dict):
            raise ValueError("'configuration' must be a dict.")

        # Check for 'reporting_currency'
        if (
            "reporting_currency" not in configuration
            or not isinstance(configuration["reporting_currency"], str)
        ):
            raise ValueError("Missing/invalid 'reporting_currency' in configuration.")

        return configuration

    def configuration_list(self) -> dict:
        """Return a dict with the configuration."""
        return {"REPORTING_CURRENCY": self.reporting_currency}

    def configuration_modify(self, configuration: dict = {}):
        """Modify provided configuration."""
        if "REPORTING_CURRENCY" in configuration:
            self.reporting_currency = configuration["REPORTING_CURRENCY"]

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

    @staticmethod
    def account_multipliers(accounts: dict) -> dict[str, int]:
        """
        Compute net multipliers for accounts based on their frequency in the
        'add' and 'subtract' lists. Each occurrence in 'add' contributes +1,
        each in 'subtract' contributes -1.

        Args:
            accounts (dict): Dictionary with two keys:
                - 'add': list of accounts to include positively
                - 'subtract': list of accounts to include negatively

        Returns:
            dict[str, int]: Mapping of account identifier to net multiplier.
        """
        add_counts = Counter(accounts.get("add", []))
        sub_counts = Counter(accounts.get("subtract", []))
        return {
            acc: add_counts.get(acc, 0) - sub_counts.get(acc, 0)
            for acc in set(add_counts) | set(sub_counts)
        }

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

        dates = pl.Series(name="date", values=[None] * len(df))
        precision = self.precision_vectorized(
            currencies=df["currency"], dates=dates, allow_missing=True
        )
        valid_currency_mask = ~precision.is_null().to_numpy()
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
        """Sanitize accounts and tax codes

        Accounts and tax_codes are interdependent. This method validates both
        entities, navigating their mutual dependencies in a multi-step process:
        1. Sanitize accounts with raw tax_codes.
        2. Sanitize tax_codes using the partially sanitized accounts.
        3. Re-sanitize accounts with the validated tax_codes.

        Logs warnings for each discarded or adjusted entry.

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

    @timed_cache(120)
    def account_currency(self, account: int) -> str:
        accounts = self.accounts.list()
        if not int(account) in accounts["account"].values:
            raise ValueError(f"Account {account} is not defined.")
        return accounts.loc[accounts["account"] == account, "currency"].values[0]

    def individual_account_balances(
        self,
        accounts: str | int | dict[str, list[int]] | list[int] | None,
        period: str | datetime.date | None = None,
        profit_centers: list[str] | str | None = None,
    ) -> pd.DataFrame:
        """Calculate balances individually for a single account
        or for each account in a list or range, returning one row per account.

        Args:
            accounts (str | int | dict[str, list[int]] | list[int] | None):
                The range of accounts to be evaluated. See `parse_account_range()` for possible
                formats. If None, the balance of all accounts is returned.
            period (datetime.date | str | None):
                The time period for which account balances are calculated. See `parse_date_span`
                for possible values. If a single date, the balance up to that date is returned.
                If None, the balance is calculated from all available accounting data.
            profit_centers (list[str], str, optional): If not None, the result is calculated only
                           from ledger entries assigned to the specified profit centers.

        Returns:
            pd.DataFrame: A data frame with ACCOUNT_BALANCE_SCHEMA, providing account and
                reporting currency balances as separate rows for each account.
        """
        # Gather account list
        df = self.accounts.list()[["group", "description", "account", "currency"]]
        if accounts is not None:
            account_dict = self.parse_account_range(accounts)
            account_list = list(set(account_dict["add"]) - set(account_dict["subtract"]))
            df = df.loc[df["account"].isin(account_list)]

        df["period"] = period
        df["profit_center"] = [profit_centers] * len(df)
        df.reset_index(drop=True, inplace=True)
        balances = self.account_balances(df)
        df[["balance", "report_balance"]] = balances[["balance", "report_balance"]]
        df["balance"] = df.apply(lambda row: row["balance"].get(row["currency"], 0.0), axis=1)

        return enforce_schema(df, ACCOUNT_BALANCE_SCHEMA).sort_values("account")

    def account_balances(
        self, df: pd.DataFrame, reporting_currency_only: bool = False
    ) -> pd.DataFrame:
        """Calculate account balances from a DataFrame of flexible query specifications,
        returning a result of the same length with the most efficient method.

        Enriches the DataFrame with new column(s):
        - `report_balance`: Balance in the reporting currency.
        - `balance`: Dictionary containing the balance of the account(s) in all currencies
        in which transactions were recorded. Keys denote currencies and values the balance.
        The absence of a currency is interpreted as a zero balance.

        Args:
            df (pd.DataFrame): Input DataFrame with rows specifying balance queries.
                Expected columns include:
                - 'account': An account identifier, list, or range.
                See `parse_account_range()` for supported formats.
                - 'period' (optional): A cutoff date or date span.
                See `parse_date_span()` for supported formats.
                - 'profit_center' (optional): Profit center filter.
                See `parse_profit_center_range()` for supported formats.
            reporting_currency_only (bool, optional): If True, omits the `balance` column
                and includes only the `report_balance` column. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame of the same length, enriched with:
                - 'report_balance': Amount in the reporting currency.
                - 'balance': Dictionary of currency-wise balances
                (excluded if `reporting_currency_only` is True).
        """
        balances = [
            self._account_balance(account=acct, period=prd, profit_centers=pc)
            for prd, acct, pc in zip(df["period"], df["account"], df["profit_center"])
        ]
        report_balances = [r.pop("reporting_currency", 0.0) for r in balances]
        result = pd.DataFrame({"report_balance": report_balances})

        if not reporting_currency_only:
            result["balance"] = balances

        return result

    def aggregate_account_balances(self, df: pd.DataFrame = None, n: int = 1) -> pd.DataFrame:
        """
        Aggregates account balances by account groups

        Prunes the group path to a specified depth and updates the description
        with the next segment in the path when available, otherwise falling
        back to the original account description. Finally, sums the
        report_balance of all within unique combinations of account group
        and description.

        Parameters:
            df (pd.DataFrame): A DataFrame in LEDGER_ENGINE.ACCOUNT_BALANCE_SCHEMA.
            n (int): Number of leading segments to preserve in the group path.

        Returns:
            pd.DataFrame: Aggregated account balances with the
                          LEDGER_ENGINE.AGGREGATED_BALANCE_SCHEMA schema.
        """
        groups = [prune_path(g, d, n=n) for g, d in zip(df["group"], df["description"])]
        df[["group", "description"]] = pd.DataFrame(groups, index=df.index)
        grouped = df.groupby(["group", "description"], dropna=False, sort=False)["report_balance"]
        return enforce_schema(grouped.sum().reset_index(), AGGREGATED_BALANCE_SCHEMA)

    def account_history(
        self,
        account: int | str | dict,
        period: datetime.date = None,
        profit_centers: list[str] | str = None,
        drop: bool = False
    ) -> pd.DataFrame:
        """
        Return the transaction history for the specified account(s).

        Fetches all transactions in chronological order for a single account,
        an account range (e.g., "1000:1999"), or multiple/ranges combined
        (e.g., "1000+1020:1025" or "1020:1025-1000") within the given period.
        The resulting DataFrame includes `balance` (transaction currency) and
        `report_balance` (reporting currency).

        Args:
            account (int | str | dict): Account(s) to retrieve. Accepts:
                - Single account (e.g., 1020)
                - Account range (e.g., "1000:1999")
                - Combinations using "+" or "-" (e.g., "1000+1020:1025")
                See `parse_account_range` for details.
            period (datetime.date | str | int, optional): Period or date for the
                transactions. Can be specified as a year ("2024"), month ("2024-01"),
                quarter ("2024-Q1"), or start-end tuple. Defaults to None.
                See `parse_date_span` for details.
            profit_centers (list[str] | str, optional): Filter results by these
                profit center(s). Defaults to None.
            drop (bool, optional): If True, drops redundant information:
                - Columns containing only NA values
                - The "account" column if a single account is queried
                - "report_amount" and "report_balance" if only reporting currency
                accounts are queried
                Defaults to False.

        Returns:
            pd.DataFrame: Transaction history in JOURNAL_SCHEMA with additional
            `balance` and `report_balance` columns, potentially excluding
            columns if `drop` is True.
        """
        start, end = parse_date_span(period)
        accounts = self.parse_account_range(account)
        accounts = list(set(accounts["add"]) - set(accounts["subtract"]))
        df = self._fetch_account_history(
            accounts, start=start, end=end, profit_centers=profit_centers
        )
        df = enforce_schema(df, schema=ACCOUNT_HISTORY_SCHEMA)

        if drop:
            if len(accounts) == 1:
                df = df.drop(columns=["account"])
            accounts_df = self.accounts.list().query("account in @accounts")
            if (
                accounts_df["currency"].nunique() == 1
                and accounts_df["currency"].iloc[0] == self.reporting_currency
            ):
                df = df.drop(columns=["report_amount", "report_balance"])
            mandatory = ACCOUNT_HISTORY_SCHEMA.query("mandatory == True")["column"].tolist()
            remove = [col for col in df.columns.difference(mandatory) if df[col].isna().all()]
            df = df.drop(columns=remove)

        return df

    def _fetch_account_history(
        self, account: int | list[int], start: datetime.date = None, end: datetime.date = None,
        profit_centers: list[str] | str = None
    ) -> pd.DataFrame:
        """Fetch transaction history of a list of accounts and compute balance.

        Args:
            account (int, list[int]): The account or list of accounts to fetch the history for.
            start (datetime.date, optional): Start date for the history. Defaults to None.
            end (datetime.date, optional): End date for the history. Defaults to None.
            profit_centers: (list[str], str): Filter for journal entries. If not None, the result is
                                              calculated only from journal entries assigned to one
                                              of the profit centers in the filter.

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
        if profit_centers is not None:
            if isinstance(profit_centers, str):
                profit_centers = [profit_centers]
            valid_profit_centers = set(self.profit_centers.list()["profit_center"])
            invalid_profit_centers = set(profit_centers) - valid_profit_centers
            if invalid_profit_centers:
                raise ValueError(
                    f"Profit centers: {', '.join(invalid_profit_centers)} do not exist."
                )
            filter = filter & (ledger["profit_center"].isin(profit_centers))
        df = ledger.loc[filter, :]
        df = df.sort_values("date")
        df.insert(df.columns.get_loc("amount") + 1, "balance", df["amount"].cumsum())
        df.insert(df.columns.get_loc("report_amount") + 1,
                  "report_balance", df["report_amount"].cumsum())
        if start is not None:
            df = df.loc[df["date"] >= pd.to_datetime(start), :]
        return df.reset_index(drop=True)

    def parse_account_range(
        self, range: str | int | dict[str, list[int]] | list[int]
    ) -> dict:
        """Convert an account range into a standard format.

        Args:
            range (str | int | dict[str, list[int]] | list[int]): The account(s) to be evaluated.
                - **str**: Can be a sequence of accounts formatted as follows:
                    - A colon (`:`) defines a range, e.g., `"1000:1999"`, which includes all
                    accounts within the specified range.
                    - A plus (`+`) adds multiple accounts or ranges, e.g., `"1000+1020:1025"`,
                    which includes `1000` and all accounts from `1020` to `1025`.
                    - A minus (`-`) excludes accounts or ranges, e.g., `"1020:1030-1022"`,
                    where `1022` is excluded from the selection. In `account_balance`,
                    the minus sign is used to subtract the balance of accounts or ranges,
                    e.g., `"-2020:2030"` returns the balance of accounts `2020` to `2030`
                    multiplied by `-1`, or `"1020:1030-2020:2030"` subtracts the balance
                    of accounts `2020` to `2030` from the balance of accounts `1020` to `1030`.
                - **int**: A single numeric account number to add (positive number)
                    or subtract (negative number).
                - **dict[str, list[int]]**: A dictionary with `"add"` and `"subtract"` keys,
                    each containing a list of account numbers to be included or excluded.
                    Same as the return value.
                - **list[int]**: A list of account numbers to use, same as `"add"` key
                    in the return value.

        Returns:
            dict: A dictionary with the following structure:
                - `"add"` (list[int]): Accounts to be included.
                - `"subtract"` (list[int]): Accounts to be excluded or,
                  for `account_balance`, subtracted.

        Raises:
            ValueError: If the input format is invalid or no matching accounts are found.
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
        elif isinstance(range, dict):
            if not ("add" in range and "subtract" in range):
                raise ValueError("Dict must have 'add' and 'subtract' keys.")
            # Ensure values are lists
            add = list(range.get("add", []))
            subtract = list(range.get("subtract", []))
            if not all(isinstance(i, int) for i in add + subtract):
                raise ValueError("Both 'add' and 'subtract' must contain only integers.")
        elif isinstance(range, list):
            if not all(isinstance(i, int) for i in range):
                raise ValueError("List elements must all be integers.")
            add = range
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
                f"Expecting int, str, dict, or list for range, not {type(range).__name__}."
            )
        if not add and not subtract:
            raise ValueError(f"No account matching '{range}'.")
        return {"add": add, "subtract": subtract}

    # ----------------------------------------------------------------------
    # Journal

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
        return self.serialize_ledger(self.journal.list())

    def sanitize_journal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Discard incoherent journal data.

        This method discards ledger transactions - entries in the journal data
        frame with the same 'id' - that:
        1. Do not balance to zero.
        2. Span multiple dates.
        3. Have neither 'account' nor 'contra' specified.
        4. Omit a profit center when profit centers exist in the system.
        5. Reference an invalid currency, account, contra account, or profit
           center.
        6. Do not match the currency of a referenced account or contra account,
           and the account or contra account is not denominated in
           reporting currency.
        7. Lack a valid price reference when 'report_amount' is missing and
           the entry is in a non-reporting currency.

        Also, undefined tax code references are removed from journal entries.

        A warning specifying the reason is logged for each discarded entry.

        Args:
            df (pd.DataFrame): Journal data to sanitize.

        Returns:
            pd.DataFrame: Sanitized journal data containing only valid entries.
        """

        df = enforce_schema(df, JOURNAL_SCHEMA, keep_extra_columns=True)

        invalid_ids = set()
        invalid_ids = self._invalid_multidate_txns(df, invalid_ids)
        self._invalid_tax_codes(df, invalid_ids)
        invalid_ids = self._invalid_accounts(df, invalid_ids)
        precision = self.precision_vectorized(df["currency"], dates=df["date"], allow_missing=True)
        invalid_ids = self._invalid_assets(df, invalid_ids, precision)
        invalid_ids = self._invalid_currency(df, invalid_ids)
        invalid_ids = self._invalid_prices(df, invalid_ids)
        invalid_ids = self._invalid_profit_centers(df, invalid_ids)
        df["report_amount"] = self._fill_report_amounts(df, invalid_ids, precision)
        invalid_ids = self._unbalanced_report_amounts(df, invalid_ids)

        return df.query("id not in @invalid_ids").reset_index(drop=True)

    def _invalid_multidate_txns(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark transactions where a single 'id' spans multiple distinct 'date' values."""
        invalid_date = df.groupby("id")["date"].transform("nunique") > 1
        new_invalid_ids = set(df.loc[invalid_date, "id"]) - invalid_ids
        if new_invalid_ids:
            self._logger.warning(
                f"Discarding {len(new_invalid_ids)} journal entries where a single 'id' "
                f"has more than one 'date': {first_elements_as_str(new_invalid_ids)}"
            )
            invalid_ids = invalid_ids.union(new_invalid_ids)

        return invalid_ids

    def _invalid_tax_codes(self, df: pd.DataFrame, invalid_ids: set) -> None:
        """Drop undefined 'tax_code' references."""
        _, tax_codes = self.sanitized_accounts_tax_codes()
        invalid_tax_code_mask = ~df["tax_code"].isna() & ~df["tax_code"].isin(set(tax_codes["id"]))
        new_invalid_ids = set(df.loc[invalid_tax_code_mask, "id"]) - invalid_ids
        if new_invalid_ids:
            self._logger.warning(
                f"Setting 'tax_code' to 'NA' for {len(new_invalid_ids)} journal entries "
                f"with invalid tax codes: {first_elements_as_str(new_invalid_ids)}"
            )
        df.loc[invalid_tax_code_mask, "tax_code"] = pd.NA

    def _invalid_accounts(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """
        Mark transactions with invalid accounts:
        - both 'account' and 'contra' are missing, or
        - a referenced account is not defined."""
        accounts, _ = self.sanitized_accounts_tax_codes()
        accounts_set = set(accounts["account"])

        # Transactions missing both 'account' and 'contra'
        missing_mask = df["account"].isna() & df["contra"].isna()
        missing_ids = set(df.loc[missing_mask, "id"]) - invalid_ids
        if missing_ids:
            self._logger.warning(
                f"Discarding {len(missing_ids)} journal entries with neither 'account' "
                f"nor 'contra' specified: {first_elements_as_str(missing_ids)}"
            )
            invalid_ids = invalid_ids.union(missing_ids)

        # Transactions with invalid account or contra references
        invalid_account_mask = ~df["account"].isna() & ~df["account"].isin(accounts_set)
        invalid_contra_mask = ~df["contra"].isna() & ~df["contra"].isin(accounts_set)
        ref_mask = invalid_account_mask | invalid_contra_mask
        ref_ids = set(df.loc[ref_mask, "id"]) - invalid_ids - missing_ids
        if ref_ids:
            self._logger.warning(
                f"Discarding {len(ref_ids)} journal entries with invalid account "
                f"or contra references: {first_elements_as_str(ref_ids)}"
            )
            invalid_ids = invalid_ids.union(ref_ids)

        return invalid_ids

    def _invalid_assets(self, df: pd.DataFrame, invalid_ids: set, precision: pl.Series) -> set:
        """Mark transactions with invalid asset references."""
        null_mask = precision.is_null().to_numpy()
        new_invalid_ids = set(df.loc[null_mask, "id"]) - invalid_ids

        if new_invalid_ids:
            self._logger.warning(
                f"Discarding {len(new_invalid_ids)} journal entries with invalid currency: "
                f"{first_elements_as_str(new_invalid_ids)}"
            )
            invalid_ids = invalid_ids.union(new_invalid_ids)

        return invalid_ids

    def _invalid_currency(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark transactions with currency mismatched to account or contra account."""
        reporting_currency = self.reporting_currency

        def is_invalid(row):
            if row["id"] in invalid_ids:
                return True
            if "amount" in df.columns and row["amount"] == 0:
                return False
            if pd.notna(row["account"]):
                account_currency = self.account_currency(row["account"])
                if ((account_currency != reporting_currency)
                        and (row["currency"] != account_currency)):
                    return True
            if pd.notna(row["contra"]):
                contra_currency = self.account_currency(row["contra"])
                if ((contra_currency != reporting_currency)
                        and (row["currency"] != contra_currency)):
                    return True
            return False

        invalid_currency = df.apply(is_invalid, axis=1, result_type='reduce')
        new_invalid_ids = set(df.loc[invalid_currency, "id"]) - invalid_ids
        if new_invalid_ids:
            self._logger.warning(
                f"Discarding {len(new_invalid_ids)} journal entries with mismatched transaction "
                f"currency: {first_elements_as_str(new_invalid_ids)}"
            )
            invalid_ids = invalid_ids.union(new_invalid_ids)

        return invalid_ids

    def _invalid_prices(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark transactions with missing price references."""
        reporting_currency = self.reporting_currency

        def is_invalid(row):
            if row["id"] in invalid_ids:
                return True
            if (
                row.get("currency") == reporting_currency
                or ("report_amount" in df.columns and pd.notna(row.get("report_amount")))
            ):
                return False
            try:
                self.price(
                    ticker=row["currency"], date=row["date"], currency=reporting_currency
                )
                return False
            except ValueError:
                return True

        invalid_price = df.apply(is_invalid, axis=1, result_type='reduce')
        new_invalid_ids = set(df.loc[invalid_price, "id"]) - invalid_ids
        if new_invalid_ids:
            self._logger.warning(
                f"Discarding {len(new_invalid_ids)} journal entries with invalid price: "
                f"{first_elements_as_str(new_invalid_ids)}"
            )
            invalid_ids = invalid_ids.union(new_invalid_ids)

        return invalid_ids

    def _invalid_profit_centers(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark transactions with missing or invalid profit center references."""
        profit_centers = set(self.profit_centers.list()["profit_center"])
        if profit_centers:
            invalid_mask = df["profit_center"].isna() | ~df["profit_center"].isin(profit_centers)
        else:
            invalid_mask = df["profit_center"].notna()
        new_invalid_ids = set(df.loc[invalid_mask, "id"]) - invalid_ids
        if new_invalid_ids:
            if profit_centers:
                self._logger.warning(
                    f"Discarding {len(new_invalid_ids)} journal entries with missing or invalid "
                    f"profit center: {first_elements_as_str(new_invalid_ids)}"
                )
            else:
                self._logger.warning(
                    f"Discarding {len(new_invalid_ids)} journal entries assigned to a "
                    f"profit center, while no profit centers are defined: "
                    f"{first_elements_as_str(new_invalid_ids)}"
                )
            invalid_ids = invalid_ids.union(new_invalid_ids)
        return invalid_ids

    @staticmethod
    def amount_multiplier(df: pd.DataFrame) -> np.ndarray:
        """Return an array of multipliers (0, 1, or -1) based on the presence of
        'account' and 'contra' columns in the DataFrame.

        Rules:
        - If both 'account' and 'contra' are present or both are missing -> 0
        - If only 'account' is present -> 1
        - If only 'contra' is present -> -1
        """
        return np.where(
            df["account"].isna() == df["contra"].isna(),
            0,
            np.where(df["account"].notna(), 1, -1)
        )

    def _fill_report_amounts(
        self, df: pd.DataFrame, invalid_ids: set, precision: pd.Series
    ) -> pd.Series:
        """Fill missing report amounts with default values.

        Replaces NA report amounts by converting the amount in transaction
        currency into the reporting currency. Ensures that transactions with a single
        non-reporting currency that are balanced in their original currency are also balanced in
        reporting currency.
        """
        report_amount = df["report_amount"].copy()
        na_mask = report_amount.isna() & ~df["id"].isin(invalid_ids)
        report_amount.loc[na_mask] = self.report_amount(
            amount=df.loc[na_mask, "amount"],
            currency=df.loc[na_mask, "currency"],
            date=df.loc[na_mask, "date"],
        )

        # Identify transactions with a single non-reporting currency that are
        # 1. balanced in in their original currency,
        # 2. not balanced in reporting currency,
        # 3. for which all original report amounts were missing,
        # 4. which have at least two rows with exactly one of account or contra account specified.
        tolerance = self.precision_vectorized(["reporting_currency"], [None])[0] / 2
        multiplier = self.amount_multiplier(df)
        grouped = pd.DataFrame({
            "id": df["id"],
            "currency": df["currency"],
            "amount": df["amount"] * multiplier,
            "report_amount": report_amount * multiplier,
            "single_account_row": multiplier != 0,
            "original_report_amount_missing": df["report_amount"].isna(),
            "precision": precision,
        })
        grouped = grouped.groupby("id").agg(
            nunique_currency=("currency", "nunique"),
            first_currency=("currency", "first"),
            first_precision=("precision", "first"),
            all_na_report_balance=("original_report_amount_missing", "all"),
            n_single_account_rows=("single_account_row", "sum"),
            net_amount=("amount", "sum"),
            net_report_amount=("report_amount", "sum"),
        )
        auto_balance_ids = set(grouped.index[
            (grouped["nunique_currency"] == 1)
            & (grouped["first_currency"] != self.reporting_currency)
            & (grouped["all_na_report_balance"])
            & (grouped["n_single_account_rows"] >= 2)
            & (grouped["net_amount"].abs() <= grouped["first_precision"] / 2)
            & (grouped["net_report_amount"].abs() > tolerance)
        ])
        # Ensure transactions with a single non-reporting currency that are balanced in their
        # original currency are also balanced in reporting currency.
        for txn_id in auto_balance_ids.difference(invalid_ids):
            txn_mask = (df["id"] == txn_id) & (multiplier != 0)
            first_txn_row = np.flatnonzero(txn_mask)[0]
            txn_multiplier = multiplier[txn_mask]
            fx_rate = self.price(
                date=df["date"].iloc[first_txn_row],
                ticker=df["currency"].iloc[first_txn_row],
                currency=self.reporting_currency,
            )[1]
            # Adjust sign for 'amount' and 'report_amount' where only 'contra' is specified
            unrounded_values = df.loc[txn_mask, "amount"] * txn_multiplier * fx_rate
            rounded_values = report_amount[txn_mask] * txn_multiplier
            increment = self.precision_vectorized(["reporting_currency"], [None])[0]
            while True:
                errors = rounded_values - unrounded_values
                # Exit if within tolerance
                if abs(rounded_values.sum()) <= tolerance:
                    break
                # Reduce the row with the largest positive error
                if rounded_values.sum() > tolerance:
                    idx_largest_err = errors.idxmax()
                    rounded_values[idx_largest_err] -= increment
                # Increase the row with the largest negative error
                elif rounded_values.sum() < -tolerance:
                    idx_smallest_err = errors.idxmin()
                    rounded_values[idx_smallest_err] += increment
            # Restore original sign where only 'contra' is specified
            report_amount[txn_mask] = rounded_values * txn_multiplier
        return report_amount

    def _unbalanced_report_amounts(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark transactions whose total amounts do not balance to zero as invalid."""
        net_amount = (df["report_amount"] * self.amount_multiplier(df)).groupby(df["id"]).sum()
        precision = self.precision_vectorized(["reporting_currency"], [None])[0] / 2
        unbalanced = abs(net_amount) > precision
        if unbalanced.any():
            unbalanced_ids = set(unbalanced.index[unbalanced])
            self._logger.warning(
                f"Discarding {len(unbalanced_ids)} journal entries where "
                f"amounts do not balance to zero: {first_elements_as_str(unbalanced_ids)}"
            )
            invalid_ids = invalid_ids.union(unbalanced_ids)
        return invalid_ids

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
        credit = df[JOURNAL_SCHEMA["column"]]
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
        return result[JOURNAL_SCHEMA["column"]]

    def txn_to_str(self, df: pd.DataFrame) -> Dict[str, str]:
        """Create a consistent, unique representation of journal entries.

        Converts transactions into a dict of CSV-like string representation.
        The result can be used to compare transactions.

        Args:
            df (pd.DataFrame): DataFrame containing journal entries.

        Returns:
            Dict[str, str]: A dictionary where keys are journal 'id's and values are
            unique string representations of the transactions.
        """
        df = self.journal.standardize(df)
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
        amount: float | list[float],
        ticker: str | list[str],
        date: datetime.date = None
    ) -> float | list[float]:
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
        is_scalar = np.isscalar(amount) and np.isscalar(ticker)

        if np.isscalar(amount):
            amounts = [amount] * len(ticker) if not np.isscalar(ticker) else [amount]
        else:
            amounts = list(amount)

        if np.isscalar(ticker):
            tickers = [ticker] * len(amounts)
        else:
            tickers = list(ticker)

        if len(amounts) != len(tickers):
            raise ValueError("Amount and ticker lists must be of the same length")

        date = date or datetime.date.today()
        dates = [date] * len(tickers)

        # Get precision vector
        precision = self.precision_vectorized(
            currencies=tickers, dates=dates, allow_missing=True
        )

        result = []
        for amt, prec in zip(amounts, precision):
            if prec is None or pd.isna(amt):
                result.append(None)
            else:
                scaled = round(amt / prec, 0) * prec
                rounded = round(scaled, -1 * math.floor(math.log10(prec)))
                result.append(rounded)

        return result[0] if is_scalar else result

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

        if not (len(amount) == len(currency) == len(date)):
            raise ValueError("Vectors 'amount', 'currency', and 'date' must have the same length.")

        reporting_currency = self.reporting_currency
        prices = [
            self.price(src_currency, date=dt, currency=reporting_currency)[1]
            for src_currency, dt in zip(currency, date)
        ]
        amounts = [
            amt * rate if amt is not None and rate is not None else None
            for amt, rate in zip(amount, prices)
        ]
        if isinstance(date, pd.Series):
            date = date.iloc[0] if not date.empty else None
        else:
            date = date[0] if date and pd.notna(date[0]) else None
        return self.round_to_precision(amounts, [reporting_currency] * len(amounts), date)

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

        # The `precision_vectorized` method validates the existence of a valid asset definition
        # and relies on sanitized data, serving as a centralized validation point.
        ticker_precision = self.precision_vectorized(
            currencies=df["ticker"], dates=df["date"], allow_missing=True,
        )
        currency_precision = self.precision_vectorized(
            currencies=df["currency"], dates=df["date"], allow_missing=True,
        )
        invalid_tickers_mask = ticker_precision.is_null().to_numpy()
        invalid_currencies_mask = currency_precision.is_null().to_numpy()

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
        df = df.loc[~invalid_mask].reset_index(drop=True)
        return df

    @timed_cache(120)
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
    @timed_cache(120)
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
    @timed_cache(120)
    def _assets_as_df(self) -> pl.DataFrame:
        """
        Returns a clean, unified DataFrame of asset definitions for precision lookups.

        Merges default and user-defined assets, fills missing dates with a fallback default date,
        removes duplicates on ('ticker', 'date') keeping the first occurrence, and sorts
        values by ticker and date.

        Returns:
            pl.DataFrame: A Polars DataFrame with columns:
                - 'ticker' (str): Asset ticker symbol.
                - 'date' (date): Effective date (or fallback default for timeless entries).
                - 'increment' (float): Precision increment for the asset.
        """
        combined = (
            pl.concat([
                pl.from_pandas(self.sanitize_assets(self.assets.list())),
                pl.from_pandas(DEFAULT_ASSETS),
            ])
            .with_columns([
                pl.col("ticker").cast(pl.Utf8),
                pl.col("date").fill_null(DEFAULT_DATE).cast(pl.Date),
            ])
            .unique(subset=["ticker", "date"], keep="first")
            .sort(["ticker", "date"])
        )
        return combined

    def precision_vectorized(
        self, currencies: pl.Series, dates: pl.Series, allow_missing: bool = False
    ) -> pl.Series:
        """
        Returns the smallest price increment (precision) for each (currency, date) pair.

        Falls back to the earliest known date for timeless assets. Raises on unresolved
        lookups unless `allow_missing=True`.

        Args:
            currencies (pl.Series): Currency or asset tickers, e.g. 'USD', 'BTC'.
            dates (pl.Series): Corresponding datetime.date values, same length as `currencies`.
            allow_missing (bool): If True, missing entries return null instead of raising.

        Raises:
            ValueError: If no precision is found and `allow_missing` is False.

        Returns:
            pl.Series: Precision increments (float or null), one per (currency, date) input.
        """
        currencies = pl.Series(currencies) if not isinstance(currencies, pl.Series) else currencies
        dates = pl.Series(dates) if not isinstance(dates, pl.Series) else dates

        tickers = [
            self.reporting_currency if c == "reporting_currency" else c
            for c in currencies.to_list()
        ]
        lookup_df = pl.DataFrame({
            "ticker": pl.Series(tickers, dtype=pl.Utf8),
            "date": pl.Series(dates, dtype=pl.Date).fill_null(DEFAULT_DATE)
        })
        joined = lookup_df.join_asof(
            self._assets_as_df,
            by="ticker",
            on="date",
            strategy="backward",
            check_sortedness=False,
        )

        if not allow_missing:
            missing_idx = joined["increment"].is_null().arg_true().min()
            if missing_idx is not None:
                raise ValueError(
                    "No asset definition available for "
                    f"'{joined['ticker'][missing_idx]}' on {joined['date'][missing_idx]}"
                )

        return joined["increment"]

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
                accounts_range = self.parse_account_range(acc)
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

    # ----------------------------------------------------------------------
    # Reconciliation

    def sanitize_reconciliation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard incoherent reconciliation data.

        This method discards reconciliation entries that:
        1. Have an unresolvable or missing period.
        2. Have invalid account references.
        3. Have unresolvable profit center references.
        4. Have invalid or unresolvable currency references.
        5. Specify a balance with an invalid or missing currency.
        6. Have both 'balance' and 'report_balance' missing.

        If a balance is specified but the currency is invalid, both 'balance' and 'currency'
        are set to NA.

        It also sets default tolerances when missing:
        - Half the currency precision if only 'balance' is present.
        - Half the base currency precision if only 'report_balance' is present.
        - Half the smaller precision if both balances are present.

        A warning specifying the reason is logged for each discarded entry.

        Args:
            df (pd.DataFrame): Reconciliation data to sanitize.

        Returns:
            pd.DataFrame: Sanitized reconciliation data containing only valid entries.
        """
        df = enforce_schema(df, RECONCILIATION_SCHEMA, keep_extra_columns=True)
        id_columns = RECONCILIATION_SCHEMA.query("id == True")["column"].tolist()
        id_data = df[id_columns]
        invalid_mask = pd.Series(False, index=df.index)

        mask = self._invalid_periods(df["period"], invalid_mask)
        self._log_invalid_rows("invalid periods", mask, id_data)
        invalid_mask |= mask

        mask = self._invalid_reconciliation_accounts(df["account"], invalid_mask)
        self._log_invalid_rows("invalid accounts", mask, id_data)
        invalid_mask |= mask

        mask = self._invalid_reconciliation_profit_centers(df["profit_center"], invalid_mask)
        self._log_invalid_rows("invalid profit centers", mask, id_data)
        invalid_mask |= mask

        drop_mask, patch_mask = self._invalid_currencies(
            df["currency"], df["period"], df["balance"], df["report_balance"], invalid_mask
        )
        self._log_invalid_rows("invalid currencies without report balance", drop_mask, id_data)
        self._log_invalid_rows(
            "invalid currencies with patch (clearing balance & currency)", patch_mask, id_data
        )
        df.loc[patch_mask, ["balance", "currency"]] = pd.NA
        invalid_mask |= drop_mask

        mask = self._invalid_missing_balances(df["balance"], df["report_balance"], invalid_mask)
        self._log_invalid_rows("missing both balances", mask, id_data)
        invalid_mask |= mask
        df["tolerance"] = df["tolerance"].fillna(0.0)

        return df.loc[~invalid_mask].reset_index(drop=True)

    def _invalid_periods(self, period: pd.Series, invalid_mask: pd.Series) -> pd.Series:
        """Mark rows with invalid periods."""
        def is_invalid(x):
            if pd.isna(x):
                return True
            try:
                return not bool(parse_date_span(x))
            except Exception:
                return True

        mask = period.apply(is_invalid)
        return mask & ~invalid_mask

    def _invalid_reconciliation_accounts(
        self, account: pd.Series, invalid_mask: pd.Series
    ) -> pd.Series:
        """Mark rows with invalid account references."""
        def is_invalid(x):
            try:
                acc = self.parse_account_range(x)
                return not acc["add"] and not acc["subtract"]
            except Exception:
                return True

        mask = account.apply(is_invalid)
        return mask & ~invalid_mask

    def _invalid_reconciliation_profit_centers(
        self, pc: pd.Series, invalid_mask: pd.Series
    ) -> pd.Series:
        """Mark rows with invalid profit centers."""
        valid_set = set(self.profit_centers.list()["profit_center"])
        mask = pc.notna() & ~pc.isin(valid_set)
        return mask & ~invalid_mask

    def _invalid_currencies(
        self,
        currency: pd.Series,
        period: pd.Series,
        balance: pd.Series,
        report_balance: pd.Series,
        invalid_mask: pd.Series,
    ) -> tuple[pd.Series, pd.Series]:
        """Mark rows with invalid currencies and identify patchable ones."""
        valid_idx = ~invalid_mask
        currency_valid = currency.loc[valid_idx]
        period_valid = period.loc[valid_idx].map(lambda x: parse_date_span(x)[1])
        precision = self.precision_vectorized(
            currencies=currency_valid, dates=period_valid, allow_missing=True
        )

        currency_na_mask = currency_valid.isna().to_numpy()
        precision_null_mask = precision.is_null().to_numpy()
        invalid_local = (~currency_na_mask) & precision_null_mask
        full_mask = pd.Series(False, index=currency.index)
        full_mask.loc[currency_valid.index[invalid_local]] = True

        drop_mask = full_mask & report_balance.isna()
        patch_mask = full_mask & balance.notna()
        return drop_mask, patch_mask

    def _invalid_missing_balances(
        self, balance: pd.Series, report_balance: pd.Series, invalid_mask: pd.Series
    ) -> pd.Series:
        """Mark rows missing both balances."""
        mask = balance.isna() & report_balance.isna()
        return mask & ~invalid_mask

    def _log_invalid_rows(self, reason: str, mask: pd.Series, id_data: pd.DataFrame) -> None:
        """Log a warning for invalid rows."""
        if mask.any():
            self._logger.warning(
                f"Discarding {mask.sum()} reconciliation rows with {reason}: "
                f"{first_elements_as_str(id_data.loc[mask].to_dict('records'))}"
            )

    def reconcile(
        self,
        df: pd.DataFrame,
        period: str | datetime.date | None = None,
        source_pattern: str | None = None
    ) -> pd.DataFrame:
        """Enrich reconciliation data with actual account balances.

        Args:
            df (pd.DataFrame): Expected balances compatible with RECONCILIATION_SCHEMA.
            period (datetime.date | str | None): Time period to filter reconciliation data.
                See `parse_date_span` for possible values.
            source_pattern (str | None): Regex for filtering the 'source' column.

        Returns:
            pd.DataFrame: Enriched reconciliation DataFrame with added
                'actual_balance' and 'actual_report_balance' columns.
        """
        if period is not None:
            start, end = parse_date_span(period)
            start = pd.to_datetime(start) if start else None
            end = pd.to_datetime(end)

            def row_within_period(row) -> bool:
                r_start, r_end = parse_date_span(row["period"])
                r_start = pd.to_datetime(r_start) if r_start else pd.to_datetime(r_end)
                r_end = pd.to_datetime(r_end)
                return (start is None or r_start >= start) and (r_end <= end)

            df = df[df.apply(row_within_period, axis=1)].reset_index(drop=True)

        if source_pattern is not None:
            df = df[df['source'].str.contains(source_pattern, na=False)]

        df = self.sanitize_reconciliation(df)
        balances = self.account_balances(df)
        balances.index = df.index
        df[["actual_balance", "actual_report_balance"]] = balances[["balance", "report_balance"]]
        df["actual_balance"] = df.apply(
            lambda row: row["actual_balance"].get(row["currency"], 0.0), axis=1
        )
        return df

    def reconciliation_summary(self, df: pd.DataFrame) -> list[str]:
        """
        Generates human-readable error messages for all reconciliation mismatches beyond tolerance.

        This method compares expected and actual balances across operational and reporting
        currencies and generates descriptive error messages for discrepancies that exceed the
        defined tolerance.

        Args:
            df (pd.DataFrame): A DataFrame with columns from RECONCILIATION_SCHEMA_CSV,
                extended with the following:
                - actual_balance: Calculated balance from ledger data
                - actual_report_balance: Calculated reporting balance (if available)

        Returns:
            list[str]: Formatted error messages for each mismatch that exceeds tolerance.
        """
        def account_description(account):
            """Return the text describing a given account."""
            acc_df = self.accounts.list().query("account == @account")
            if len(acc_df) != 1:
                raise ValueError(f"Account {account} not found.")
            return acc_df["description"].item()

        messages = []

        if not df.empty:
            df["delta"] = df["actual_balance"].fillna(0) - df["balance"].fillna(0)
            df["end"] = [parse_date_span(period)[1] for period in df['period']]
            df["precision"] = \
                self.precision_vectorized(df["currency"], dates=df["end"], allow_missing=True)
            df["report_delta"] = \
                df["actual_report_balance"].fillna(0) - df["report_balance"].fillna(0)
            failed_delta = \
                (df["delta"].abs() > df["tolerance"] + df["precision"] / 2) & df["balance"].notna()
            for row in df.loc[failed_delta].to_dict("records"):
                if str(row["account"]).isdigit():
                    desc = account_description(int(row["account"]))
                    msg = f"Account {row['account']} '{desc}'"
                else:
                    msg = f"Account {row['account']}"
                delta = self.round_to_precision(row['delta'], row['currency'], date=row['end'])
                messages.append(
                    f"{msg}: Actual balance of {row['currency']} {row['actual_balance']:,} "
                    f"differs by {row['currency']} {delta:,} from expected "
                    f"balance of {row['currency']} {row['balance']:,} as of {row['period']}."
                )

            precision = self.precision_vectorized(["reporting_currency"], [None])[0]
            failed_report_delta = (
                (df["report_delta"].abs() > df["tolerance"] + precision / 2)
                & df["report_balance"].notna()
            )
            for row in df.loc[failed_report_delta].to_dict("records"):
                if str(row["account"]).isdigit():
                    desc = account_description(int(row["account"]))
                    msg = f"Account {row['account']} '{desc}'"
                else:
                    msg = f"Account {row['account']}"

                report_delta = self.round_to_precision(row['report_delta'],
                                                       self.reporting_currency, date=row['end'])
                messages.append(
                    f"{msg}: Actual reporting currency balance of {row['actual_report_balance']:,} "
                    f"differs by {report_delta:,} from "
                    f"expected balance of {row['report_balance']:,} as of {row['period']}."
                )

        return messages

    # ----------------------------------------------------------------------
    # Profit center

    def sanitize_profit_center(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard profit centers that contain the '+' character and log a warning.

        The '+' character is reserved for profit center concatenation.

        Args:
            df (pd.DataFrame): Profit center data to sanitize.

        Returns:
            pd.DataFrame: A sanitized DataFrame containing only valid profit center entries.
        """
        df = enforce_schema(df, PROFIT_CENTER_SCHEMA, keep_extra_columns=True)

        invalid_mask = df["profit_center"].astype(str).str.contains(r"\+")
        if invalid_mask.any():
            invalid_names = df.loc[invalid_mask, "profit_center"].tolist()
            self._logger.warning(
                f"Discarding {invalid_mask.sum()} profit center entries with '+' "
                f"in 'profit_center': {first_elements_as_str(invalid_names)}"
            )

        return df.loc[~invalid_mask].reset_index(drop=True)

    def parse_profit_centers(self, profit_center: str | list[str] | set[str]) -> set[str]:
        """
        Parse a profit center expression into a set of valid profit center names.

        The input can be a '+'-separated string (e.g., "Shop+General+Bakery"),
        a list of names, or a set of names. Any undefined or invalid profit
        centers are discarded with a warning.

        Args:
            profit_center (str | list[str] | set[str]): Profit center input.
                - str: A string with profit centers separated by '+'.
                - list[str] or set[str]: A collection of profit center names.

        Returns:
            set[str]: A set of valid profit center names.

        Raises:
            ValueError: If the input format is invalid or no valid profit centers are found.
        """
        if isinstance(profit_center, (list, set)):
            items = set(profit_center)
        elif isinstance(profit_center, str):
            items = set(part.strip() for part in profit_center.strip().split("+") if part.strip())
        else:
            raise ValueError(
                f"Expecting str, list, or set as input, not {type(profit_center).__name__}."
            )

        if not all(isinstance(i, str) for i in items):
            raise ValueError("All profit center entries must be strings.")

        defined = set(self.profit_centers.list()["profit_center"])
        valid = items & defined
        invalid = items - defined

        if invalid:
            self._logger.warning(
                f"Discarding {len(invalid)} undefined profit centers: "
                f"{first_elements_as_str(invalid)}."
            )

        if not valid:
            raise ValueError(f"No valid profit centers found in: {profit_center}")

        return valid

    # ----------------------------------------------------------------------
    # Target Balance

    def sanitize_target_balance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard incoherent target balance data.

        This method applies the following validation rules:
        1. The `balance` column must not be NA, otherwise discard the row.
        2. `lookup_period` should follow formats supported by `parse_date_span()`,
            otherwise discard the row.
        3. `lookup_accounts` should follow the format of `parse_account_range()`,
            otherwise discard the row.
        4. `lookup_profit_centers` must reference valid profit centers,
            otherwise discard the row.
        5. Discard any row that violates journal-level integrity rules
            (see `sanitize_journal()`), except for rules involving `tax_code`,
            `amount`, and `report_amount`.

        A warning is logged for each dropped entry with the specific reason.

        Args:
            df (pd.DataFrame): Target balance data to sanitize.

        Returns:
            pd.DataFrame: A sanitized DataFrame containing only valid target balance entries.
        """

        df = enforce_schema(df, TARGET_BALANCE_SCHEMA, keep_extra_columns=True)

        invalid_ids = set()
        invalid_ids = self._invalid_balance(df, invalid_ids)
        invalid_ids = self._invalid_lookup_period(df, invalid_ids)
        invalid_ids = self._invalid_accounts_range(df, invalid_ids)
        invalid_ids = self._invalid_lookup_profit_centers(df, invalid_ids)
        invalid_ids = self._invalid_accounts(df, invalid_ids)
        precision = self.precision_vectorized(df["currency"], dates=df["date"], allow_missing=True)
        invalid_ids = self._invalid_assets(df, invalid_ids, precision)
        # TODO: validate currency and price including possible "reporting_currency" value
        invalid_ids = self._invalid_profit_centers(df, invalid_ids)

        return df.query("id not in @invalid_ids").reset_index(drop=True)

    def _invalid_balance(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with missing balance values."""
        invalid_mask = df["balance"].isna()
        missing_ids = set(df.loc[invalid_mask, "id"]) - invalid_ids

        if missing_ids:
            self._logger.warning(
                f"Discarding {len(missing_ids)} target balance entries with missing 'balance': "
                f"{first_elements_as_str(missing_ids)}"
            )
            invalid_ids = invalid_ids.union(missing_ids)

        return invalid_ids

    def _invalid_lookup_period(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with invalid 'lookup_period' values."""

        def is_invalid(span: str) -> bool:
            if pd.isna(span):
                return True
            try:
                parse_date_span(span)
                return False
            except Exception:
                return True

        invalid_mask = df["lookup_period"].apply(is_invalid).astype(bool)
        new_ids = set(df.loc[invalid_mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with invalid 'lookup_period': "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids

    def _invalid_accounts_range(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with unresolvable 'lookup_accounts' values."""

        def is_invalid(val: str) -> bool:
            if pd.isna(val):
                return True
            try:
                accounts = self.parse_account_range(val)
                return (len(accounts["add"]) == 0) and (len(accounts["subtract"]) == 0)
            except Exception:
                return True

        mask = df["lookup_accounts"].apply(is_invalid).astype(bool)
        new_ids = set(df.loc[mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with unresolvable 'lookup_accounts': "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids

    def _invalid_lookup_profit_centers(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with unresolvable 'lookup_profit_centers' values."""

        valid_profit_centers = set(self.profit_centers.list()["profit_center"])

        if not valid_profit_centers:
            mask = df["lookup_profit_centers"].notna()
            reason = "assigned to a profit center, while no profit centers are defined"
        else:
            mask = (
                df["lookup_profit_centers"].notna()
                & ~df["lookup_profit_centers"].isin(valid_profit_centers)
            )
            reason = "unresolvable profit centers"

        new_ids = set(df.loc[mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with `{reason}`: "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids

    # ----------------------------------------------------------------------
    # Reporting

    def account_sheet_tables(
        self,
        period: str,
        config: pd.DataFrame,
        profit_centers: str | None = None,
        root_folder: str | None = None,
        output: Literal["typst", "dataframe"] = "typst"
    ) -> dict[str, str | pd.DataFrame]:
        """
        Generate Typst-formatted tables or formatted DataFrames with transaction history
        for each account.

        For each account number, this method retrieves its transaction history for the
        specified period and optional profit centers, formats the data according to the
        provided configuration DataFrame, and returns either Typst-formatted strings
        or raw formatted DataFrames.

        Args:
            period (str): Period or date range for transactions. Accepts values such as
                a year ("2024"), month ("2024-01"), quarter ("2024-Q1"), or a full
                start–end span. See `parse_date_span` for accepted formats.
            config (pd.DataFrame): A configuration DataFrame specifying which columns to
                include, their labels, widths, and alignments.
            profit_centers (str | None, optional): Optional filter to include only
                transactions for the specified profit center(s).
            root_folder (str | None, optional): If provided, values in the 'document'
                column will be rendered as Typst links using this base folder path.
            output (Literal["typst", "dataframe"]): Determines return type: either
                rendered Typst table strings or raw formatted DataFrames. Defaults to "typst".

        Returns:
            dict[str, str | pd.DataFrame]: A mapping from account numbers to either Typst
            table strings or formatted DataFrames, depending on `output`.
        """
        config = enforce_schema(config, ACCOUNT_SHEET_REPORT_CONFIG_SCHEMA)
        accounts_df = self.accounts.list()
        tolerance = self.precision_vectorized([self.reporting_currency], [None])[0] / 2
        column_order = config["column"].tolist()
        result = {}

        for account in accounts_df["account"]:
            df = self.account_history(
                account, period=period, profit_centers=profit_centers, drop=True
            )
            if df.empty:
                continue

            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["amount"] = format_threshold(df["amount"], tolerance)
            df["balance"] = format_threshold(df["balance"], tolerance)
            df["description"] = format_typst_text(df["description"])
            if "report_amount" in df.columns:
                df["report_amount"] = format_threshold(df["report_amount"], tolerance)
            if "report_balance" in df.columns:
                df["report_balance"] = format_threshold(df["report_balance"], tolerance)
            if "document" in df.columns:
                df["document"] = format_typst_links(df["document"], root_folder)

            visible_cols = [col for col in column_order if col in df.columns]
            df = df[visible_cols]

            if output == "dataframe":
                result[account] = df
            else:
                colspecs = config.set_index("column").loc[visible_cols]["width"].tolist()
                align = config.set_index("column").loc[visible_cols]["align"].tolist()
                labels = config.set_index("column").loc[visible_cols]["label"].tolist()
                df.columns = [f"*{lbl.replace(' ', '\\-').upper()}*" for lbl in labels]

                result[account] = df_to_typst(
                    df, columns=colspecs, align=align, hline=[1], colnames=True
                )

        return result
