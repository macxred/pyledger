"""This module defines the TextLedger class, an extension of StandaloneLedger,
designed to read and process tax, accounts, ledger, etc. from text files.
"""

import logging
import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime
from pyledger.constants import LEDGER_SCHEMA, FIXED_LEDGER_COLUMNS
from pyledger.helpers import write_fixed_width_csv
from pyledger.standalone_ledger import StandaloneLedger

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

    def ledger(self) -> pd.DataFrame:
        if self._ledger_cache is not None and not self._is_expired(self._ledger_cache_time):
            return self._ledger_cache

        ledger_folder = self.root_path / 'ledger'
        ledger_folder.mkdir(parents=True, exist_ok=True)

        dsf = []
        ledger_files = list(ledger_folder.rglob('*.csv'))
        for file in ledger_files:
            try:
                path = file.relative_to(self.root_path)
                df = pd.read_csv(file, skipinitialspace=True)
                df['file_path'] = str(path)
                dsf.append(df)
            except Exception as e:
                self._logger.warning(f"Skipping {path}: {e}")

        if not len(dsf):
            df = self.standardize_ledger(None)
            df["file_path"] = "ledger/default.csv"
            dsf.append(df)

        df = pd.concat(dsf, ignore_index=True)
        id_type = LEDGER_SCHEMA.loc[LEDGER_SCHEMA['column'] == 'id', 'dtype'].values[0]
        df["id"] = df["file_path"] + ":" + df["date"].notna().cumsum().astype(id_type)
        df.drop(columns="file_path", inplace=True)
        df = self.standardize_ledger(df)

        self._ledger_cache = df
        self._ledger_cache_time = datetime.now()
        return self._ledger_cache

    def ledger_for_save(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formats and validates the DataFrame for storage by extracting file paths,
        standardizing columns, and ensuring data consistency.

        Args:
            df (pd.DataFrame): The DataFrame to prepare.

        Returns:
            pd.DataFrame: The formatted DataFrame ready for saving.
        """
        def extract_file_path(id_value: str) -> str:
            return id_value.split(":")[0] if ":" in str(id_value) else "ledger/default.csv"

        df = self.standardize_ledger(df)
        df["file_path"] = df["id"].apply(extract_file_path)
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        if df.loc[~df.duplicated(subset="id"), "date"].isna().any():
            raise ValueError("A valid 'date' is required in the first occurrence of every 'id'.")

        df = df.drop(columns=["id"])
        return df

    def add_ledger_entry(self, entry: pd.DataFrame) -> str:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError("Id needs to be unique and present in all rows of a collective booking.")

        df = self.ledger()
        file_path = entry["id"].iat[0].split(":")
        file_path = "ledger/default.csv" if len(file_path) == 1 else file_path[0]
        df_same_file = df[df["id"].str.startswith(file_path)]

        ledger = self.ledger_for_save(pd.concat([df_same_file, entry]))
        self.save_ledger(ledger)
        self.invalidate_ledger_cache()

        existing_ids = df_same_file["id"].str.split(":").str[1].astype(int)
        created_id = existing_ids.max() + 1 if not existing_ids.empty else 1
        return f"{file_path}:{created_id}"

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError("Id needs to be unique and present in all rows of a collective booking.")

        df = self.ledger()
        file_path = entry["id"].iat[0].split(":")[0]
        df_same_file = df[df["id"].str.startswith(file_path)]
        if entry["id"].iat[0] not in df_same_file["id"].values:
            raise ValueError(f"Ledger entry with id '{entry["id"].iat[0]}' not found.")

        ledger = pd.concat([df_same_file[df_same_file["id"] != entry["id"].iat[0]], entry])
        ledger = self.ledger_for_save(ledger)
        self.save_ledger(ledger)
        self.invalidate_ledger_cache()

    def delete_ledger_entries(self, ids: List[str], allow_missing: bool = False) -> None:
        ledger = self.ledger()
        if not allow_missing:
            missing_ids = set(ids) - set(ledger["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        ledger = ledger[~ledger["id"].isin(ids)]
        ledger = self.ledger_for_save(ledger)
        self.save_ledger(ledger)
        self.invalidate_ledger_cache()

    def save_ledger(self, df: pd.DataFrame) -> None:
        """
        Saves the ledger DataFrame to the corresponding CSV files within the 'ledger' folder.
        Groups rows by 'file_path' and writes only the necessary files.
        Deletes any existing empty files or directories that are no longer present in the sorted file paths.

        Args:
            df (pd.DataFrame): The DataFrame with a 'file_path' column.
        """
        ledger_folder = self.root_path / "ledger"
        ledger_folder.mkdir(parents=True, exist_ok=True)
        df["file_path"] = df["file_path"].fillna("ledger/default.csv")
        grouped = df.groupby('file_path')
        sorted_file_paths = sorted(grouped.groups.keys(), key=lambda p: Path(p).parts)
        existing_files = list(ledger_folder.rglob('*.csv'))
        existing_dirs = [d for d in ledger_folder.rglob('*') if d.is_dir()]
        expected_files = {self.root_path / file_path for file_path in sorted_file_paths}

        # Delete files that are not present in sorted_file_paths
        for file in existing_files:
            if file not in expected_files:
                file.unlink()
                self._logger.info(f"Deleted file: {file}")

        # Delete empty directories that are not needed
        for directory in existing_dirs:
            if not any(directory.iterdir()):
                directory.rmdir()
                self._logger.info(f"Deleted empty directory: {directory}")

        # Save the updated files
        for file_path in sorted_file_paths:
            group_df = grouped.get_group(file_path)
            group_df.drop(columns="file_path", inplace=True)
            full_path = self.root_path / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            write_fixed_width_csv(group_df, full_path, n=len(FIXED_LEDGER_COLUMNS))

        default_file = ledger_folder / "default.csv"
        if not default_file.exists():
            default_file.touch()
            self._logger.info(f"Created default ledger file: {default_file}")

    def invalidate_ledger_cache(self) -> None:
        """
        Invalidates the cached ledger data.
        Resets the cache and cache timestamp to ensure the next access reads from disk.
        """
        self._ledger_cache = None
        self._ledger_cache_time = None

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


    def _is_expired(self, cache_time: datetime) -> bool:
        """
        Checks if the cache has expired based on the timeout.

        Args:
            cache_time (datetime): The timestamp when the cache was last updated.

        Returns:
            bool: True if the cache is expired or cache_time is None, False otherwise.
        """
        return (datetime.now() - cache_time).total_seconds() > self._cache_timeout
