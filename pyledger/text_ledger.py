"""This module defines TextLedger, extending StandaloneLedger to store data in text files."""

import datetime
import pandas as pd
import yaml
from typing import List
from pathlib import Path
from pyledger.decorators import timed_cache
from pyledger.standalone_ledger import StandaloneLedger
from pyledger.constants import (
    ACCOUNT_SCHEMA,
    ASSETS_SCHEMA,
    DEFAULT_SETTINGS,
    LEDGER_SCHEMA,
    TAX_CODE_SCHEMA
)
from pyledger.helpers import (
    save_files,
    write_fixed_width_csv,
)
from consistent_df import enforce_schema


# TODO: remove once old systems are migrated
LEDGER_COLUMN_SHORTCUTS = {
    "cur": "currency",
    "vat": "tax_code",
    "target": "target_balance",
    "base_amount": "report_amount",
    "counter": "contra",
}
TAX_CODE_COLUMN_SHORTCUTS = {
    "text": "description",
    "inclusive": "is_inclusive",
    "inverse_account": "contra",
}
ACCOUNT_COLUMN_SHORTCUTS = {
    "text": "description",
    "vat_code": "tax_code",
}


class TextLedger(StandaloneLedger):
    """
    Stand-alone ledger system storing data in text files.

    TextLedger stores accounting data in text files, optimized for version
    control systems like Git. To produce concise Git diffs, files follow a
    strict format, and mutator functions are minimally invasive, modifying
    only necessary files and preserving row order whenever possible.

    To enhance readability, tabular data, such as the account chart and general
    ledger entries, is stored in a fixed-width CSV format, with entries padded
    with spaces for consistent column widths. Configuration settings, including
    the reporting currency, are stored in YAML format.
    """

    def __init__(
        self,
        root_path: Path = Path.cwd(),
    ):
        """Initializes the TextLedger with a root path for file storage.
        If no root path is provided, defaults to the current working directory.
        """
        self.root_path = Path(root_path).expanduser()
        super().__init__()

    # ----------------------------------------------------------------------
    # Settings

    @property
    @timed_cache(15)
    def settings(self):
        return self.read_settings_file(self.root_path / "settings.yml").copy()

    @settings.setter
    def settings(self, settings: dict):
        """Save settings to a YAML file.

        This method stores accounting settings such as the reporting currency
        to `<root>/settings.yml`. The YAML format is ideal for version control
        and human readability.

        Args:
            settings (dict): A dictionary containing the system settings to be saved.
        """
        with open(self.root_path / "settings.yml", "w") as f:
            yaml.dump(self.standardize_settings(settings), f, default_flow_style=False)
        self.__class__.settings.fget.cache_clear()

    def read_settings_file(self, file: Path) -> dict:
        """Read settings from the specified file.

        This method returns standardized accounting settings, including the reporting currency.
        If the specified settings file does not exist, DEFAULT_SETTINGS are returned.
        The system thus continues running even if the <root> directory is empty, which is useful
        for testing and demonstration purposes.

        Args:
            file (Path): The path to the settings file.

        Returns:
            dict: Standardized system settings.
        """
        if file.exists():
            with open(file, "r") as f:
                result = yaml.safe_load(f)
        else:
            self._logger.warning("Settings file missing, reverting to default settings.")
            result = self.standardize_settings(DEFAULT_SETTINGS)

        return result

    # ----------------------------------------------------------------------
    # Ledger

    @timed_cache(15)
    def ledger(self) -> pd.DataFrame:
        return self.read_ledger_files(self.root_path / "ledger")

    def read_ledger_files(self, root: Path) -> pd.DataFrame:
        """Reads ledger entries from CSV files in the given root directory.

        Iterates through all CSV files in the root directory, reading each file
        into a DataFrame and ensuring the data conforms to `LEDGER_SCHEMA`.
        Files that cannot be processed are skipped with a warning. The Data
        from all valid files is then combined into a single DataFrame.

        IDs are not stored in ledger files but are dynamically generated when
        reading each file. The `id` is constructed by combining the relative
        path to the root directory with the row's position, separated by a
        colon (`{path}:{position}`). These IDs are non-persistent and may
        change if a file's entries are modified. Successive rows that belong to
        the same transaction are identified by recording the date only on the
        first row; subsequent rows without a date are considered part of the
        same transaction.

        Args:
            root (Path): Root directory containing the ledger files.

        Returns:
            pd.DataFrame: The aggregated ledger data conforming to `LEDGER_SCHEMA`.
        """
        if not root.exists():
            return self.standardize_ledger(None)
        if not root.is_dir():
            raise NotADirectoryError(f"Ledger root folder is not a directory: {root}")

        ledger = []
        for file in root.rglob("*.csv"):
            relative_path = str(file.relative_to(root))
            try:
                df = pd.read_csv(file, skipinitialspace=True)
                # TODO: Remove the following line once legacy systems are migrated.
                df = df.rename(columns=LEDGER_COLUMN_SHORTCUTS)
                df = self.standardize_ledger(df)
                if not df.empty:
                    df["id"] = relative_path + ":" + df["id"]
                ledger.append(df)
            except Exception as e:
                self._logger.warning(f"Skipping {relative_path}: {e}")

        if ledger:
            result = pd.concat(ledger, ignore_index=True)
            result = enforce_schema(result, LEDGER_SCHEMA, sort_columns=True)
        else:
            result = None

        return self.standardize_ledger(result)

    @staticmethod
    def csv_path(id: pd.Series) -> pd.Series:
        """Extract storage path from ledger id."""
        return id.str.replace(":[0-9]+$", "", regex=True)

    @staticmethod
    def id_from_path(id: pd.Series) -> pd.Series:
        """Extract numeric portion of ledger id."""
        return id.str.replace("^.*:", "", regex=True).astype(int)

    def write_ledger_directory(self, df: pd.DataFrame | None = None):
        """Save ledger entries to multiple CSV files in the ledger directory.

        Saves ledger entries to several fixed-width CSV files, formatted by
        `write_ledger_file`. The storage location within the `<root>/ledger`
        directory is determined by the portion of the ID up to the last
        colon (':').

        Args:
            df (pd.DataFrame, optional): The ledger DataFrame to save.
                If not provided, defaults to the current ledger.
        """
        if df is None:
            df = self.ledger()

        df = self.standardize_ledger(df)
        df["__csv_path__"] = self.csv_path(df["id"])
        save_files(df, root=self.root_path / "ledger", func=self.write_ledger_file)
        self.ledger.cache_clear()

    @staticmethod
    def write_ledger_file(df: pd.DataFrame, file: str) -> pd.DataFrame:
        """Save ledger entries to a fixed-width CSV file.

        This method stores ledger entries in a fixed-width CSV format, ideal
        for version control systems like Git. Entries are padded with spaces
        to maintain a consistent column width for improved readability.

        The "id" column is not saved. For transactions spanning multiple rows
        with the same id, the date is recorded only on the first row. Rows
        without a date belong to the transaction that began in the preceding
        row with a date.

        Args:
            df (pd.DataFrame): The ledger entries to save.
            file (str): Path of the CSV file to write.

        Returns:
            pd.DataFrame: The formatted DataFrame saved to the file.
        """
        df = enforce_schema(df, LEDGER_SCHEMA, sort_columns=True, keep_extra_columns=True)

        # Record date only on the first row of collective transactions
        df = df.iloc[TextLedger.id_from_path(df["id"]).argsort(kind="mergesort")]
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        # Drop columns that are all NA and not required by the schema
        na_columns = df.columns[df.isna().all()]
        mandatory_columns = LEDGER_SCHEMA["column"][LEDGER_SCHEMA["mandatory"]]
        df = df.drop(columns=set(na_columns).difference(mandatory_columns) | {"id"})

        # Write a CSV with fixed-width in all columns but the last two in the schema
        n_fixed = LEDGER_SCHEMA["column"].head(-2).isin(df.columns).sum()
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    def add_ledger_entry(self, entry: pd.DataFrame, path: str = "default.csv") -> str:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError("Ids need to be identical across a single ledger entry.")

        ledger = self.ledger()
        df_same_file = ledger[self.csv_path(ledger["id"]) == path]
        if df_same_file.empty:
            id = f"{path}:1"
        else:
            ids_same_file = self.id_from_path(ledger["id"])
            id = f"{path}:{max(ids_same_file) + 1}"
        df = pd.concat([df_same_file, entry.assign(id=id)])
        full_path = self.root_path / "ledger" / path
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        self.write_ledger_file(df, full_path)
        self.ledger.cache_clear()

        return id

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError("Ids need to be identical across a single ledger entry.")
        id = entry["id"].iloc[0]

        ledger = self.ledger()
        path = self.csv_path(pd.Series(id)).item()
        df_same_file = ledger[self.csv_path(ledger["id"]) == path]
        if id not in df_same_file["id"].values:
            raise ValueError(f"Ledger entry with id '{id}' not found.")

        df_same_file = pd.concat([df_same_file[df_same_file["id"] != id], entry])
        ledger = self.write_ledger_file(df_same_file, self.root_path / "ledger" / path)
        self.ledger.cache_clear()

    def delete_ledger_entries(self, ids: List[str], allow_missing: bool = False) -> None:
        ledger = self.ledger()
        if not allow_missing:
            missing_ids = set(ids) - set(ledger["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        df = ledger[~ledger["id"].isin(ids)]
        paths_to_update = self.csv_path(pd.Series(ids)).unique()
        for path in paths_to_update:
            df_same_file = df[self.csv_path(df["id"]) == path]
            file_path = self.root_path / "ledger" / path
            if df_same_file.empty:
                file_path.unlink()
            else:
                self.write_ledger_file(df_same_file, file_path)
        self.ledger.cache_clear()

    # ----------------------------------------------------------------------
    # Tax codes

    @timed_cache(15)
    def tax_codes(self) -> pd.DataFrame:
        return self.read_tax_codes(self.root_path / "tax_codes.csv")

    def read_tax_codes(self, file: Path) -> pd.DataFrame:
        """Read tax codes from the specified CSV file.

        This method reads tax codes from the specified file and enforces the standard
        data format. If an error occurs during reading or standardization, an empty
        DataFrame with standard TAX_CODE_SCHEMA is returned and a warning is logged.

        Args:
            file (Path): The path to the CSV file containing tax codes.

        Returns:
            pd.DataFrame: A DataFrame formatted according to TAX_CODE_SCHEMA.
        """
        try:
            tax_codes = pd.read_csv(file, skipinitialspace=True)
            # TODO: remove line once old systems are migrated
            tax_codes.rename(columns=TAX_CODE_COLUMN_SHORTCUTS, inplace=True)
            tax_codes = self.standardize_tax_codes(tax_codes)
        except Exception as e:
            tax_codes = self.standardize_tax_codes(None)
            self._logger.warning(f"Error reading {file} file: {e}")
        return tax_codes

    @classmethod
    def write_tax_codes_file(cls, df: pd.DataFrame, file: Path):
        """Save tax codes to a fixed-width CSV file.

        This method stores tax codes in a fixed-width CSV format.
        Values are padded with spaces to maintain consistent column width and improve readability.
        Optional columns that contain only NA values are dropped for conciseness.

        Args:
            df (pd.DataFrame): The tax codes to save.
            file (str): Path of the CSV file to write.

        Returns:
            pd.DataFrame: The formatted DataFrame saved to the file.
        """
        df = enforce_schema(df, TAX_CODE_SCHEMA, sort_columns=True, keep_extra_columns=True)
        optional = TAX_CODE_SCHEMA.loc[~TAX_CODE_SCHEMA["mandatory"], "column"].to_list()
        to_drop = [col for col in optional if df[col].isna().all() and not df.empty]
        df.drop(columns=to_drop, inplace=True)
        n_fixed = TAX_CODE_SCHEMA["column"].isin(df.columns).sum()
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    def add_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        tax_codes = self.tax_codes()
        if (tax_codes["id"] == id).any():
            raise ValueError(f"Tax code '{id}' already exists")

        new_tax_code = self.standardize_tax_codes(pd.DataFrame({
            "id": [id],
            "description": [description],
            "account": [account],
            "rate": [rate],
            "is_inclusive": [is_inclusive],
        }))
        tax_codes = pd.concat([tax_codes, new_tax_code])
        self.write_tax_codes_file(tax_codes, self.root_path / "tax_codes.csv")
        self.tax_codes.cache_clear()

    def modify_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        tax_codes = self.tax_codes().copy()
        if (tax_codes["id"] == id).sum() != 1:
            raise ValueError(f"Tax code '{id}' not found or duplicated.")

        tax_codes.loc[
            tax_codes["id"] == id, ["rate", "account", "is_inclusive", "description"]
        ] = [rate, account, is_inclusive, description]
        self.write_tax_codes_file(tax_codes, self.root_path / "tax_codes.csv")
        self.tax_codes.cache_clear()

    def delete_tax_codes(
        self, codes: List[str] = [], allow_missing: bool = False
    ) -> None:
        tax_codes = self.tax_codes()
        if not allow_missing:
            missing = set(codes) - set(tax_codes["id"])
            if missing:
                raise ValueError(f"Tax code(s) '{', '.join(missing)}' not found.")

        tax_codes = tax_codes[~tax_codes["id"].isin(codes)]
        file_path = self.root_path / "tax_codes.csv"
        if not tax_codes.empty:
            self.write_tax_codes_file(tax_codes, file_path)
        elif file_path.exists():
            file_path.unlink()
        self.tax_codes.cache_clear()

    # ----------------------------------------------------------------------
    # Accounts

    @timed_cache(15)
    def accounts(self) -> pd.DataFrame:
        return self.read_accounts(self.root_path / "accounts.csv")

    def read_accounts(self, file: Path) -> pd.DataFrame:
        """Read and accounts from the specified CSV file.

        This method reads accounts from the specified file and enforces the standard
        data format. If an error occurs during reading or standardization, an empty
        DataFrame with standard ACCOUNT_SCHEMA is returned and a warning is logged.

        Args:
            file (Path): The path to the CSV file containing accounts.

        Returns:
            pd.DataFrame: A DataFrame formatted according to ACCOUNTS_SCHEMA.
        """

        try:
            accounts = pd.read_csv(file, skipinitialspace=True)
            # TODO: remove line once old systems are migrated
            accounts.rename(columns=ACCOUNT_COLUMN_SHORTCUTS, inplace=True)
            accounts = self.standardize_accounts(accounts)
        except Exception as e:
            accounts = self.standardize_accounts(None)
            self._logger.warning(f"Skipping {file} file: {e}")
        return accounts

    @classmethod
    def write_accounts_file(cls, df: pd.DataFrame, file: Path):
        """Save accounts to a fixed-width CSV file.

        This method stores accounts in a fixed-width CSV format.
        Values are padded with spaces to maintain a consistent column width and
        improve readability. Optional columns that contain only NA values
        are dropped for conciseness.

        Args:
            df (pd.DataFrame): The tax codes to save.
            file (str): Path of the CSV file to write.

        Returns:
            pd.DataFrame: The formatted DataFrame saved to the file.
        """
        df = enforce_schema(df, ACCOUNT_SCHEMA, sort_columns=True, keep_extra_columns=True)
        optional = ACCOUNT_SCHEMA.loc[~ACCOUNT_SCHEMA["mandatory"], "column"].to_list()
        to_drop = [col for col in optional if df[col].isna().all() and not df.empty]
        df.drop(columns=to_drop, inplace=True)
        n_fixed = ACCOUNT_SCHEMA["column"].isin(df.columns).sum()
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    def add_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        accounts = self.accounts()
        if (accounts["account"] == account).any():
            raise ValueError(f"Account '{account}' already exists")

        new_account = self.standardize_accounts(pd.DataFrame({
            "account": [account],
            "currency": [currency],
            "description": [description],
            "tax_code": [tax_code],
            "group": [group],
        }))
        accounts = pd.concat([accounts, new_account])
        self.write_accounts_file(accounts, self.root_path / "accounts.csv")
        self.accounts.cache_clear()

    def modify_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        accounts = self.accounts()
        if (accounts["account"] == account).sum() != 1:
            raise ValueError(f"Account '{account}' not found or duplicated.")

        accounts.loc[
            accounts["account"] == account,
            ["currency", "description", "tax_code", "group"]
        ] = [currency, description, tax_code, group]
        accounts = self.standardize_accounts(accounts)
        self.write_accounts_file(accounts, self.root_path / "accounts.csv")
        self.accounts.cache_clear()

    def delete_accounts(
        self, accounts: List[int] = [], allow_missing: bool = False
    ) -> None:
        df = self.accounts()
        if not allow_missing:
            missing = set(accounts) - set(df["account"])
            if missing:
                raise KeyError(f"Account(s) '{', '.join(missing)}' not found.")

        df = df[~df["account"].isin(accounts)]
        file_path = self.root_path / "accounts.csv"
        if not df.empty:
            self.write_accounts_file(df, file_path)
        elif file_path.exists():
            file_path.unlink()
        self.accounts.cache_clear()

    # ----------------------------------------------------------------------
    # Assets

    @timed_cache(15)
    def assets(self) -> pd.DataFrame:
        return self.read_assets(self.root_path / "assets.csv")

    def read_assets(self, file: Path) -> pd.DataFrame:
        """Read assets from the a CSV file.

        Reads assets from a CSV file and enforce the standard data format.
        If an error occurs during reading or standardization,
        returns an empty DataFrame with `ASSETS_SCHEMA` and logs a warning.

        Args:
            file (Path): The path to the CSV file to read.

        Returns:
            pd.DataFrame: A DataFrame formatted according to ASSETS_SCHEMA.
        """
        try:
            assets = pd.read_csv(file, skipinitialspace=True)
            assets = self.standardize_assets(assets)
        except Exception as e:
            assets = self.standardize_assets(None)
            self._logger.warning(f"Skipping {file} file: {e}")
        return assets

    @classmethod
    def write_assets_file(cls, df: pd.DataFrame, file: Path):
        """Writes the assets DataFrame to a fixed-width CSV file.

        Drops optional columns that contain only NA values and writes the
        assets DataFrame to a fixed-width CSV file. Entries are padded with
        spaces for consistent column widths and improved readability.

        Args:
            df (pd.DataFrame): The assets to save.
            file (Path): Path to the CSV file to write.

        Returns:
            pd.DataFrame: The formatted DataFrame that was saved to the file.
        """
        df = enforce_schema(df, ASSETS_SCHEMA, sort_columns=True, keep_extra_columns=True)
        optional = ASSETS_SCHEMA.loc[~ASSETS_SCHEMA["mandatory"], "column"].to_list()
        to_drop = [col for col in optional if df[col].isna().all() and not df.empty]
        df.drop(columns=to_drop, inplace=True)
        n_fixed = ASSETS_SCHEMA["column"].isin(df.columns).sum()
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    def add_asset(self, ticker: str, increment: float, date: datetime.date = None) -> pd.DataFrame:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )
        if mask.any():
            raise ValueError("The asset with this unique combination already exists.")

        asset = self.standardize_assets(pd.DataFrame({
            "ticker": [ticker],
            "date": [date],
            "increment": [increment]
        }))
        assets = pd.concat([assets, asset])
        self.write_assets_file(assets, self.root_path / "assets.csv")
        self.assets.cache_clear()

    def modify_asset(self, ticker: str, increment: float, date: datetime.date = None) -> None:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )
        if not mask.any():
            raise ValueError(f"Asset with ticker '{ticker}' and date '{date}' not found.")

        assets.loc[mask, "increment"] = increment
        assets = self.standardize_assets(assets)
        self.write_assets_file(assets, self.root_path / "assets.csv")
        self.assets.cache_clear()

    def delete_asset(
        self, ticker: str, date: datetime.date = None, allow_missing: bool = False
    ) -> None:
        assets = self.assets()
        mask = (assets["ticker"] == ticker) & (
            (assets["date"] == pd.Timestamp(date)) | (pd.isna(assets["date"]) & pd.isna(date))
        )

        if mask.any():
            assets = assets[~mask].reset_index(drop=True)
        else:
            if not allow_missing:
                raise ValueError(f"Asset with ticker '{ticker}' and date '{date}' not found.")

        file_path = self.root_path / "assets.csv"
        if not assets.empty:
            self.write_assets_file(assets, file_path)
        elif file_path.exists():
            file_path.unlink()
        self.assets.cache_clear()

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self.settings["reporting_currency"]

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self.settings = self.settings | {"reporting_currency": currency}
