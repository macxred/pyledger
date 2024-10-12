"""This module defines the TextLedger class, an extension of StandaloneLedger,
designed to read and process tax, accounts, ledger, etc. from text files.
"""

import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
from pyledger.standalone_ledger import StandaloneLedger
from pyledger.constants import LEDGER_SCHEMA
from pyledger.helpers import save_files, save_fixed_width_csv
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
        self._ledger = self.ledger()
        self._prices = self.standardize_prices(None)
        self._tax_codes = self.standardize_tax_codes(None)
        self._accounts = self.standardize_accounts(None)

    def ledger(self) -> pd.DataFrame:
        if self._is_expired(self._ledger_time):
            self._ledger = self.read_ledger_files(self.root_path / "ledger")
            self._ledger_time = datetime.now()
        return self._ledger

    # number after colon (:) in id did not start from zero for each file
    # need to standardize before concatenating in order to solely drop one file when data frame is ill formatted
    # Check whether id had a problem
    def read_ledger_files(self, root: Path) -> pd.DataFrame:
        if not root.exists():
            return self.standardize_ledger(None)
        if not root.is_dir():
            raise NotADirectoryError(f"Ledger root folder is not a directory: {root}")
        ledger = []
        for file in root.rglob("*.csv"):
            try:
                df = pd.read_csv(file, skipinitialspace=True)
                # TODO: remove line once old systems are migrated
                df = df.rename(columns=LEDGER_COLUMN_SHORTCUTS)
                self.standardize_ledger(df)
                if not df.empty:
                    df["id"] = str(file.relative_to(root)) + ":" + df["id"]
                ledger.append(df)
            except Exception as e:
                self._logger.warning(f"Skipping {path}: {e}")
        if ledger:
            result = pd.concat(result, ignore_index=True)
        else:
            result = None
        return self.standardize_ledger(result)


    @staticmethod
    def csv_path(id: pd.Series) -> pd.Series:
        """Extract storrage path from ledger id"""
        return id.str.replace(":[0-9]+$", "", regex=True)

    def write_ledger_directory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Save ledger entries to multiple fixed with csv files

        This method Prepares the ledger DataFrame for saving by extracting the file
        path from the "id" column and assigning it to a new "__csv_path__" column.

        Args:
            df (pd.DataFrame): The ledger DataFrame to prepare.

        Returns:
            pd.DataFrame: The formatted ledger DataFrame ready for saving.
        """
        df = self.standardize_ledger(df)
        df["__csv_path__"] = self.csv_path(df["id"])
        save_files(df, root=self.root_path / "ledger", func=self.write_ledger_file)


    @classmethod
    def write_ledger_file(cls, df: pd.DataFrame, file: Path):
        """Save ledger as fixed-width csv file

        Write given entries to a fixed-width csv file, dropping unneeded
        optional columns. This method defines the hard-coded canonical
        text representation of an accounting ledger.

        Args:
            df (pd.DataFrame): Ledger entries to save.
        """
        df = enforce_schema(df, LEDGER_SCHEMA, sort_columns=True, keep_extra_columns=True)
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)
        df = df.drop(columns=["id"]) # TODO: Also drop optional columns that only contain NAs
        n_fixed = len(LEDGER_SCHEMA["columns"].head(-2).isin(df.columns))
        save_fixed_width_csv(df, file=file, n=n_fixed)
        return df


    def add_ledger_entry(self, entry: pd.DataFrame, path: str = "default.csv") -> str:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        ledger = self.ledger()
        df_same_file = ledger[self.csv_path(ledger["id"]) == path]
        while entry['id'] in df_same_file['id']:
            entry['id'] = "new_" + entry['id']
        df = pd.concat([df_same_file, entry])
        ledger = self.write_ledger_file(df, path)
        self._invalidate_ledger()

        # return id of newly created entry
        # (quite complicated, better ideas are welcome)
        ledger = self.standardize_ledger(ledger)
        ledger['id'] = path + ":" + ledger['id']
        id = ledger['id'][~ledger['id'].isin(df_same_file['id'])].unique()
        if len(id) == 1:
            raise ValueError(f"Unexpected number of new ledger ids: {id}")
        return id.item()

    def modify_ledger_entry(self, entry: pd.DataFrame) -> None:
        entry = self.standardize_ledger(entry)
        if entry["id"].nunique() != 1:
            raise ValueError(
                "Id needs to be unique and present in all rows of a collective booking."
            )

        df = self.ledger()
        path = entry["id"].iat[0].split(":")[0]
        df_same_file = df[df["id"].str.startswith(path)]
        if entry["id"].iat[0] not in df_same_file["id"].values:
            raise ValueError(
                f"Ledger entry with id '{entry["id"].iat[0]}' not found."
            )

        ledger = pd.concat([df_same_file[df_same_file["id"] != entry["id"].iat[0]], entry])
        fixed_n = sum(
            col not in ["description", "document", "id"] for col in LEDGER_SCHEMA["column"]
        )
        ledger = self._ledger_for_save(ledger)
        save_files(ledger, self.root_path / "ledger", fixed_n)
        self._invalidate_ledger()

    def delete_ledger_entries(self, ids: List[str], allow_missing: bool = False) -> None:
        df = self.ledger()
        if not allow_missing:
            missing_ids = set(ids) - set(df["id"])
            if missing_ids:
                raise KeyError(f"Ledger entries with ids '{', '.join(missing_ids)}' not found.")

        df = df[~df["id"].isin(ids)]
        fixed_n = sum(
            col not in ["description", "document", "id"] for col in LEDGER_SCHEMA["column"]
        )
        df = self._ledger_for_save(df)
        save_files(df, self.root_path / "ledger", fixed_n)
        self._invalidate_ledger()

    def _invalidate_ledger(self) -> None:
        """
        Invalidates the cached ledger data.
        Resets the cache and cache timestamp to ensure the next access reads from disk.
        """
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
