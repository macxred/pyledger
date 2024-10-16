"""This module defines the TextLedger class, an extension of StandaloneLedger,
designed to read and process tax, accounts, ledger, etc. from text files.
"""

import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
from pyledger.standalone_ledger import StandaloneLedger
from pyledger.constants import LEDGER_SCHEMA
from pyledger.helpers import save_files, write_fixed_width_csv
from consistent_df import enforce_schema


# TODO: replace with json realization
SETTINGS = {
    "reporting_currency": "CHF",
    "precision": {
        "CAD": 0.01,
        "CHF": 0.01,
        "EUR": 0.01,
        "GBP": 0.01,
        "HKD": 0.01,
        "USD": 0.01,
    },
}

# TODO: remove once old systems are migrated
LEDGER_COLUMN_SHORTCUTS = {
    "cur": "currency",
    "vat": "tax_code",
    "target": "target_balance",
    "base_amount": "report_amount",
    "counter": "contra",
}


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
        """Initializes the TextLedger with a root path for file storage.
        If no root path is provided, defaults to the current working directory.
        """
        super().__init__(settings=SETTINGS)
        self.root_path = Path(root_path).expanduser()
        self._reporting_currency = reporting_currency
        self._cache_timeout = cache_timeout
        self._ledger_time = None
        self._ledger = self.standardize_ledger(None)
        self._prices = self.standardize_prices(None)
        self._tax_codes = self.standardize_tax_codes(None)
        self._accounts = self.standardize_accounts(None)

    def ledger(self) -> pd.DataFrame:
        if self._is_expired(self._ledger_time):
            self._ledger = self.read_ledger_files()
            self._ledger_time = datetime.now()
        return self._ledger

    def read_ledger_files(self) -> pd.DataFrame:
        """Read ledger entries from all CSV files in the ledger directory.

        Find and process each CSV file in the directory separately:
        - Prepend the file path relative to the root directory to the 'id' column with a : sign.
        - Skip files that fail processing with a warning.

        Concatenate the DataFrames from each file into a single ledger DataFrame.

        Returns:
            pd.DataFrame: A DataFrame following the LEDGER_SCHEMA.

        Raises:
            NotADirectoryError: If the provided ledger path is not a directory.
        """

        ledger_folder = self.root_path / "ledger"
        if not ledger_folder.exists():
            return self.standardize_ledger(None)
        if not ledger_folder.is_dir():
            raise NotADirectoryError(f"Ledger root folder is not a directory: {ledger_folder}")

        ledger = []
        for file in ledger_folder.rglob("*.csv"):
            try:
                df = pd.read_csv(file, skipinitialspace=True)
                # TODO: remove line once old systems are migrated
                df = df.rename(columns=LEDGER_COLUMN_SHORTCUTS)
                df = self.standardize_ledger(df)
                if not df.empty:
                    df["id"] = str(file.relative_to(self.root_path / "ledger")) + ":" + df["id"]
                ledger.append(df)
            except Exception as e:
                self._logger.warning(f"Skipping {file} file: {e}")
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
        """Extract numeric id from ledger id."""
        return id.str.extract(r"(\d+)$")[0]

    def write_ledger_directory(self, df: pd.DataFrame = None):
        """Save ledger entries to multiple fixed-width CSV files.

        This method saves all ledger entries across multiple files in a root directory.
        It ensures the same file structure using the write_ledger_file method to enforce
        a standard, deterministic structure for a single ledger file.

        If no DataFrame is provided, it defaults to the current ledger.

        Args:
            df (pd.DataFrame, optional): The ledger DataFrame to prepare. If None,
                                         defaults to the current ledger.
        """

        if df is None:
            df = self.ledger()

        df = self.standardize_ledger(df)
        df["__csv_path__"] = self.csv_path(df["id"])
        save_files(df, root=self.root_path / "ledger", func=self.write_ledger_file)
        self._invalidate_ledger()

    @staticmethod
    def write_ledger_file(df: pd.DataFrame, path: Path) -> pd.DataFrame:
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
            path (Path): File path relative to the <root>/ledger directory.

        Returns:
            pd.DataFrame: The formatted DataFrame saved to the file.
        """
        df = enforce_schema(df, LEDGER_SCHEMA, sort_columns=True, keep_extra_columns=True)

        # Record date only on the first row of collective transactions
        df.sort_values(by="id", inplace=True)
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        # Drop columns that are all NA and not required by the schema
        na_columns = df.columns[df.isna().all()]
        mandatory_columns = LEDGER_SCHEMA["column"][LEDGER_SCHEMA["mandatory"]]
        df = df.drop(columns=set(na_columns).difference(mandatory_columns) | {"id"})

        # Write a CSV with fixed-width in all columns but the last two in the schema
        n_fixed = LEDGER_SCHEMA["column"].head(-2).isin(df.columns).sum()
        write_fixed_width_csv(df, path=path, n=n_fixed)

        return df

    def add_ledger_entry(self, entry: pd.DataFrame, path: str = "default.csv") -> str:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        ledger = self.ledger()
        df_same_file = ledger[self.csv_path(ledger["id"]) == path]
        while entry["id"].iloc[0] in df_same_file["id"].values:
            entry["id"] = f"new_{entry['id'].iloc[0]}"
        df = pd.concat([df_same_file, entry])
        full_path = self.root_path / "ledger" / path
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        self.write_ledger_file(df, full_path)
        self._invalidate_ledger()

        # Return the id of a newly created ledger entry
        ids = self.id_from_path(ledger["id"]).astype(int).to_list() + [0]
        return f"{path}:{int(max(ids)) + 1}"

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        ledger = self.ledger()
        path = self.csv_path(entry["id"]).iloc[0]
        df_same_file = ledger[self.csv_path(ledger["id"]) == path]
        if entry["id"].iat[0] not in df_same_file["id"].values:
            raise ValueError(f"Ledger entry with id '{entry['id'].iat[0]}' not found.")

        df_same_file = pd.concat([df_same_file[df_same_file["id"] != entry["id"].iat[0]], entry])
        ledger = self.write_ledger_file(df_same_file, self.root_path / "ledger" / path)
        self._invalidate_ledger()

    def delete_ledger_entries(self, ids: List[str], allow_missing: bool = False) -> None:
        ledger = self.ledger()
        if not allow_missing:
            missing_ids = set(ids) - set(ledger["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        df = ledger[~ledger["id"].isin(ids)]
        paths_to_update = self.csv_path(ledger["id"]).unique()
        for path in paths_to_update:
            df_same_file = df[self.csv_path(df["id"]) == path]
            file_path = self.root_path / "ledger" / path
            if df_same_file.empty:
                file_path.unlink()
            else:
                self.write_ledger_file(df_same_file, file_path)
        self._invalidate_ledger()

    def _invalidate_ledger(self) -> None:
        """Invalidates the cached ledger data."""
        self._ledger = None
        self._ledger_time = None

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
