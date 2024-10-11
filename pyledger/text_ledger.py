"""This module defines the TextLedger class, an extension of StandaloneLedger,
designed to read and process tax, accounts, ledger, etc. from text files.
"""

import logging
import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
from pyledger.helpers import write_fixed_width_csv
from pyledger.standalone_ledger import StandaloneLedger
from pyledger.constants import LEDGER_SCHEMA, FIXED_LEDGER_COLUMNS


class TextLedger(StandaloneLedger):
    """TextLedger class for reading and processing tax, accounts, ledger, etc.
    from text files.
    """

    def __init__(
        self,
        root_path: Path = Path.cwd(),
        cache_timeout: int = 300,
        reporting_currency: str = "USD",
    ):
        """Initializes the TextLedgerClient with a root path for file storage.
        If no root path is provided, defaults to the current working directory.
        """
        self.root_path = root_path
        self._reporting_currency = reporting_currency
        self._cache_timeout = cache_timeout
        self._ledger_cache = None
        self._ledger_cache_time = None
        self._logger = logging.getLogger(__name__)
        self._ledger = self.ledger()
        self._prices = self.standardize_prices(None)
        self._tax_codes = self.standardize_tax_codes(None)
        self._accounts = self.standardize_accounts(None)

    def ledger(self, short_names: bool = False) -> pd.DataFrame:
        if self._ledger_cache is not None and not self._is_expired(self._ledger_cache_time):
            return self._ledger_cache

        ledger = self.standardize_ledger(None)
        ledger_folder = self.root_path / "ledger"
        ledger_folder.mkdir(parents=True, exist_ok=True)
        ledger_files = list(ledger_folder.rglob("*.csv"))
        for file in ledger_files:
            try:
                path = file.relative_to(self.root_path)
                df = pd.read_csv(file, skipinitialspace=True)
                if not df.empty:
                    df["file_path"] = str(path)
                    if ledger.empty:
                        ledger = df.copy()
                    else:
                        ledger = pd.concat([ledger, df], ignore_index=True)
            except Exception as e:
                self._logger.warning(f"Skipping {path}: {e}")

        if not ledger.empty:
            id_type = LEDGER_SCHEMA.loc[LEDGER_SCHEMA["column"] == "id", "dtype"].values[0]
            ledger["id"] = (
                ledger["file_path"] + ":"
                + ledger["date"].notna().cumsum().astype(id_type)
            )
            ledger.drop(columns="file_path", inplace=True)
            # TODO: remove this code block when old system will be migrated
            if short_names:
                LEDGER_COLUMN_SHORTCUTS = {
                    "cur": "currency",
                    "vat": "tax_code",
                    "target": "target_balance",
                    "base_amount": "report_amount",
                    "counter": "contra",
                }
                ledger = ledger.rename(columns=LEDGER_COLUMN_SHORTCUTS)

        ledger = self.standardize_ledger(ledger)
        self._ledger_cache = ledger
        self._ledger_cache_time = datetime.now()
        return self._ledger_cache

    def _ledger_for_save(self, df: pd.DataFrame) -> pd.DataFrame:
        """This method Prepares the ledger DataFrame for saving by extracting the file
        path from the "id" column and assigning it to a new "file_path" column.

        Args:
            df (pd.DataFrame): The ledger DataFrame to prepare.

        Returns:
            pd.DataFrame: The formatted ledger DataFrame ready for saving.
        """
        def extract_file_path(id_value: str) -> str:
            return id_value.split(":")[0] if ":" in str(id_value) else "ledger/default.csv"

        df = self.standardize_ledger(df)
        df["file_path"] = df["id"].apply(extract_file_path)
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        if df.loc[~df.duplicated(subset="id"), "date"].isna().any():
            raise ValueError(
                "A valid 'date' is required in the first occurrence of every 'id'."
            )

        df.drop(columns=["id"], inplace=True)
        return df

    def add_ledger_entry(self, entry: pd.DataFrame) -> str:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        df = self.ledger()
        file_path = entry["id"].iat[0].split(":")
        file_path = "ledger/default.csv" if len(file_path) == 1 else file_path[0]
        df_same_file = df[df["id"].str.startswith(file_path)]

        ledger = self._ledger_for_save(pd.concat([df_same_file, entry]))
        self.save_files(ledger, "ledger", len(FIXED_LEDGER_COLUMNS))
        self._invalidate_ledger_cache()

        isd = df_same_file["id"].str.split(":").str[1].astype(int)
        id = isd.max() + 1 if not isd.empty else 1
        return f"{file_path}:{id}"

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        df = self.ledger()
        file_path = entry["id"].iat[0].split(":")[0]
        df_same_file = df[df["id"].str.startswith(file_path)]
        if entry["id"].iat[0] not in df_same_file["id"].values:
            raise ValueError(
                f"Ledger entry with id '{entry["id"].iat[0]}' not found."
            )

        ledger = pd.concat([df_same_file[df_same_file["id"] != entry["id"].iat[0]], entry])
        ledger = self._ledger_for_save(ledger)
        self.save_files(ledger, "ledger", len(FIXED_LEDGER_COLUMNS))
        self._invalidate_ledger_cache()

    def delete_ledger_entries(self, ids: List[str], allow_missing: bool = False) -> None:
        df = self.ledger()
        if not allow_missing:
            missing_ids = set(ids) - set(df["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        df = df[~df["id"].isin(ids)]
        df = self._ledger_for_save(df)
        self.save_files(df, "ledger", len(FIXED_LEDGER_COLUMNS))
        self._invalidate_ledger_cache()

    def _invalidate_ledger_cache(self) -> None:
        """
        Invalidates the cached ledger data.
        Resets the cache and cache timestamp to ensure the next access reads from disk.
        """
        self._ledger_cache = None
        self._ledger_cache_time = None

    # File operations
    # ----------------------------------------------------------------------

    def save_files(self, df: pd.DataFrame, folder: str, fixed_n: int = None) -> None:
        """Saves the DataFrame entries to CSV files within the specified folder.

        Groups entries by their 'file_path' and updates only the necessary files.
        Removes any files or directories that are no longer referenced in the DataFrame,
        ensuring the directory structure is maintained. If the 'file_path' column
        is missing, a default path is assigned.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to be saved.
            folder (str): The folder within the root path where files will be saved.
            fixed_n (int, optional): Number of columns to apply fixed-width formatting.
        """

        if fixed_n is None:
            fixed_n = len(df.columns)
        default_folder = self.root_path / folder
        default_folder.mkdir(parents=True, exist_ok=True)
        if "file_path" not in df.columns:
            df["file_path"] = f"{folder}/default.csv"
        else:
            df["file_path"] = df["file_path"].fillna(f"{folder}/default.csv")
        grouped = df.groupby("file_path")
        sorted_file_paths = {self.root_path / file_path for file_path in grouped.groups.keys()}

        # Delete files not present in sorted file paths
        for file in default_folder.rglob("*.csv"):
            if file not in sorted_file_paths:
                file.unlink()
                self._logger.info(f"Deleted file: {file}")

        # Delete empty directories
        sorted_directories = sorted(
            (d for d in default_folder.rglob("*") if d.is_dir()),
            key=lambda x: len(x.parts),
            reverse=True
        )
        for directory in sorted_directories:
            if not any(directory.iterdir()):
                directory.rmdir()
                self._logger.info(f"Deleted empty directory: {directory}")

        # Save grouped DataFrame entries to their respective file paths
        for file_path in sorted_file_paths:
            group_df = grouped.get_group(str(file_path.relative_to(self.root_path)))
            group_df.drop(columns="file_path", inplace=True)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            write_fixed_width_csv(group_df, file_path, n=fixed_n)

        # Ensure the default file exists
        default_file = default_folder / "default.csv"
        if not default_file.exists():
            default_file.touch()
            self._logger.info(f"Created default file: {default_file}")

    # TODO: This section was copied form MemoryLedger temporary
    # Need to define logic for TextLedger and replace following
    # ----------------------------------------------------------------------
    # Tax codes

    def tax_codes(self) -> pd.DataFrame:
        return self.standardize_tax_codes(self._tax_codes.copy())

    def add_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        if (self._tax_codes["id"] == id).any():
            raise ValueError(f"Tax code '{id}' already exists")

        new_tax_code = self.standardize_tax_codes(pd.DataFrame({
            "id": [id],
            "description": [description],
            "account": [account],
            "rate": [rate],
            "is_inclusive": [is_inclusive],
        }))
        self._tax_codes = pd.concat([self._tax_codes, new_tax_code])

    def modify_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        is_inclusive: bool = True,
        description: str = "",
    ) -> None:
        if (self._tax_codes["id"] == id).sum() != 1:
            raise ValueError(f"Tax code '{id}' not found or duplicated.")

        self._tax_codes.loc[
            self._tax_codes["id"] == id, ["rate", "account", "is_inclusive", "description"]
        ] = [rate, account, is_inclusive, description]
        self._tax_codes = self.standardize_tax_codes(self._tax_codes)

    def delete_tax_codes(
        self, codes: List[str] = [], allow_missing: bool = False
    ) -> None:
        if not allow_missing:
            missing = set(codes) - set(self._tax_codes["id"])
            if missing:
                raise ValueError(f"Tax code(s) '{', '.join(missing)}' not found.")

        self._tax_codes = self._tax_codes[~self._tax_codes["id"].isin(codes)]

    # TODO: This section was copied form MemoryLedger temporary
    # Need to define logic for TextLedger and replace following
    # ----------------------------------------------------------------------
    # Accounts

    def accounts(self) -> pd.DataFrame:
        return self.standardize_accounts(self._accounts.copy())

    def add_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        if (self._accounts["account"] == account).any():
            raise ValueError(f"Account '{account}' already exists")

        new_account = self.standardize_accounts(pd.DataFrame({
            "account": [account],
            "currency": [currency],
            "description": [description],
            "tax_code": [tax_code],
            "group": [group],
        }))
        self._accounts = pd.concat([self._accounts, new_account])

    def modify_account(
        self,
        account: int,
        currency: str,
        description: str,
        group: str,
        tax_code: str = None,
    ) -> None:
        if (self._accounts["account"] == account).sum() != 1:
            raise ValueError(f"Account '{account}' not found or duplicated.")

        self._accounts.loc[
            self._accounts["account"] == account,
            ["currency", "description", "tax_code", "group"]
        ] = [currency, description, tax_code, group]
        self._accounts = self.standardize_accounts(self._accounts)

    def delete_accounts(
        self, accounts: List[int] = [], allow_missing: bool = False
    ) -> None:
        if not allow_missing:
            missing = set(accounts) - set(self._accounts["account"])
            if missing:
                raise KeyError(f"Account(s) '{', '.join(missing)}' not found.")

        self._accounts = self._accounts[~self._accounts["account"].isin(accounts)]

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self._reporting_currency = currency

    def _is_expired(self, cache_time: datetime | None) -> bool:
        """Checks if the cache has expired based on the cache timeout.

        Args:
            cache_time (datetime | None): The timestamp when the cache was last updated.

        Returns:
            bool: True if the cache is expired or cache_time is None, False otherwise.
        """
        if cache_time is None:
            return True
        return (datetime.now() - cache_time) > timedelta(seconds=self._cache_timeout)
