"""Provides abstract storage entities for accounting data."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, Any
import pandas as pd
from consistent_df import enforce_schema, df_to_consistent_str, nest, unnest

from pyledger.constants import DEFAULT_FILE_PATH_COLUMN
from .decorators import timed_cache
from .helpers import save_files, write_fixed_width_csv


class AccountingEntity(ABC):
    """
    Abstract base class representing accounting entities stored in tabular form
    (e.g., journal, accounts, tax codes). Defines the standard interface
    that all accounting entities must implement.
    Accessors and mutators receive or return pandas DataFrames consistent with
    the entity's specific column schema.
    """

    def __init__(
        self,
        schema: pd.DataFrame,
        prepare_for_mirroring: Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x,
        on_change: Callable[[], None] = lambda: None,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Initialize the AccountingEntity.

        Args:
            schema (pd.DataFrame): DataFrame with columns 'column', 'dtype', 'id' (boolean)
                                   defining the entity's DataFrame schema.
            prepare_for_mirroring (Callable[[pd.DataFrame], pd.DataFrame], optional):
                Function to prepare data for mirroring. Defaults to identity function.
            on_change (Callable[[], None], optional):
                Callback that triggers after any data change.
            *args, **kwargs: Additional arguments passed to the superclass.
        """
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger("ledger")
        self._schema = schema
        self._id_columns = schema.query("id == True")["column"].to_list()
        self._prepare_for_mirroring = prepare_for_mirroring
        self._on_change = on_change

    def standardize(self, data: pd.DataFrame, drop_extra_columns: bool = False) -> pd.DataFrame:
        """
        Standardize the given DataFrame to conform to the entity's schema.

        Validates that required columns are present, fills any missing optional
        columns with NA, and enforces specified data types for each column.
        Child classes may perform additional, entity-specific data conversions.

        Args:
            data (pd.DataFrame): DataFrame compatible with the entity's DataFrame schema.
            drop_extra_columns (bool, optional): If True, drop columns
                                                 outside the defined schema.

        Returns:
            pd.DataFrame: The standardized DataFrame.

        Raises:
            ValueError: If required columns are missing or data types are incorrect.
        """
        df = enforce_schema(data, self._schema, keep_extra_columns=not drop_extra_columns)

        # Convert -0.0 to 0.0
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                df.loc[df[col].notna() & (df[col] == 0.0), col] = 0.0

        return df

    @abstractmethod
    def list(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        """
        Retrieve all entries.

        Args:
            drop_extra_columns (bool, optional): If True, drop columns
                                                 outside the defined schema.

        Returns:
            pd.DataFrame: DataFrame adhering to the entity's column schema.
        """

    @abstractmethod
    def add(self, data: pd.DataFrame) -> None:
        """
        Add new entries.

        Args:
            data (pd.DataFrame): DataFrame containing new entries to add,
                                 compatible with the entity's DataFrame schema.

        Raises:
            ValueError: If the IDs in `data` are already present.
        """

    @abstractmethod
    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        """
        Delete entries.

        Args:
            id (pd.DataFrame): DataFrame containing IDs of entries to delete.
                               Must contain all ID columns defined in the schema.
            allow_missing (bool, optional): If True, does not raise an error if a combination
                                            of identifier columns is not present.

        Raises:
            ValueError: If combination of ID columns is not present and `allow_missing` is False.
        """

    @abstractmethod
    def modify(self, data: pd.DataFrame) -> None:
        """
        Modify existing entries.

        For entries identified by the unique identifier columns in `data`,
        overwrite existing values with the incoming values from `data`.
        Columns not present in `data` remain unchanged.

        Args:
            data (pd.DataFrame): DataFrame containing entries to modify. Must contain all ID
                                 columns defined in the schema; other columns are optional.

        Raises:
            ValueError: If a combination of ID columns is not present in `data`.
        """

    def mirror(self, target: pd.DataFrame, delete: bool = False) -> Dict[str, int]:
        """
        Align the current data with the incoming target data.

        Updates the current data to match the `target` by adding new entries,
        modifying existing ones, and optionally deleting entries not present
        in `target`.

        Invokes the 'prepare_for_mirroring' function set by the owning system
        to align the incoming data with the system's requirements and existing
        data prior to mirroring.

        Args:
            target (pd.DataFrame): DataFrame representing the desired target state,
                                   compatible with the entity's DataFrame schema.
            delete (bool, optional): If True, deletes current entries not present in `target`.

        Returns:
            Dict[str, int]: Summary statistics of the mirroring process containing:
                - 'initial' (int): Number of entries before synchronization.
                - 'target' (int): Number of entries in the target DataFrame.
                - 'added' (int): Number of entries added.
                - 'deleted' (int): Number of entries deleted.
                - 'updated' (int): Number of entries updated.
        """
        current = self.list()
        incoming = self._prepare_for_mirroring(self.standardize(pd.DataFrame(target)))
        merged = current.merge(
            incoming, on=self._id_columns, how="outer",
            suffixes=('_current', ''), indicator=True
        )

        # Handle deletions
        if delete:
            to_delete = merged.loc[merged["_merge"] == "left_only", self._id_columns]
            self.delete(to_delete)

        # Handle additions
        to_add = merged.loc[merged["_merge"] == "right_only", incoming.columns]
        if len(to_add):
            self.add(to_add)

        # Handle updates
        current_cols = merged.columns[merged.columns.str.endswith("_current")]
        incoming_cols = current_cols.str.replace("_current$", "", regex=True)
        both_rows = merged[merged['_merge'] == 'both']
        current_rows = both_rows[current_cols].rename(columns=lambda x: x.replace("_current", ""))
        diff_values = current_rows.ne(both_rows[incoming_cols])
        # Comparing of NA values isn't possible using .ne(), need to compare NA values separately.
        diff_nans = current_rows.isna() != both_rows[incoming_cols].isna()
        diff = (diff_values | diff_nans).any(axis=1)
        to_update = both_rows.loc[diff, incoming.columns]
        if len(to_update):
            self.modify(to_update)

        return {
            "initial": len(current),
            "target": len(incoming),
            "added": len(to_add),
            "deleted": len(to_delete) if delete else 0,
            "updated": len(to_update)
        }


class JournalEntity(AccountingEntity):
    """
    Specialized AccountingEntity for journal entries, with custom
    `standardize` and `mirror` methods tailored to journal-specific needs.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def standardize(self, data: pd.DataFrame, drop_extra_columns: bool = False) -> pd.DataFrame:
        """
        Standardize journal data to conform to the entity's schema.

        Performs additional processing specific to journal entries, including:
        - Adds 'id' column if missing, assigning IDs based on consecutive non-null dates.
        - Fills missing 'date' values by forward and backward filling within 'id' groups.

        Args:
            data (pd.DataFrame): DataFrame compatible with the entity's DataFrame schema.
            drop_extra_columns (bool, optional): If True, drop columns outside the schema.

        Returns:
            pd.DataFrame: The standardized DataFrame.

        Raises:
            ValueError: If required columns are missing or data types are incorrect.
        """
        df = super().standardize(data=data, drop_extra_columns=drop_extra_columns)

        # Add id column if missing: Entries without a date share id of the last entry with a date
        if df["id"].isna().all():
            id_type = self._schema.query("column == 'id'")['dtype'].item()
            df["id"] = df["date"].notna().cumsum().astype(id_type)

        # Fill missing (NA) dates
        df["date"] = df.groupby("id")["date"].ffill()
        df["date"] = df.groupby("id")["date"].bfill()
        df["date"] = df["date"].dt.tz_localize(None).dt.floor('D')

        return df

    def mirror(self, target: pd.DataFrame, delete: bool = False) -> Dict[str, int]:
        """
        Synchronize the current journal data with the target journal data.

        Custom implementation for journal entries that accounts for
        transactions spanning multiple rows.

        Args:
            target (pd.DataFrame): DataFrame representing the desired target journal state.
            delete (bool, optional): If True, deletes current journal entries not present in target.

        Returns:
            Dict[str, int]: Summary statistics of the mirroring process containing:
                - 'initial' (int): Number of journal entries before synchronization.
                - 'target' (int): Number of journal entries in the target DataFrame.
                - 'added' (int): Number of journal entries added.
                - 'deleted' (int): Number of journal entries deleted.
                - 'updated' (int): Number of journal entries updated (always 0 for journal entries).

        Notes:
            - IDs are not preserved during the mirroring process.
            - Deletions may alter existing IDs.
        """

        def nest_journal(df: pd.DataFrame) -> pd.DataFrame:
            """Nest to create one row per transaction, add unique string identifier."""
            nest_by = [col for col in df.columns if col not in ["id", "date"]]
            df = nest(df, columns=nest_by, key="txn")
            df["txn_str"] = [
                f"{str(date)},{df_to_consistent_str(txn)}"
                for date, txn in zip(df["date"], df["txn"])
            ]
            return df

        current = nest_journal(self.list())
        incoming = self._prepare_for_mirroring(self.standardize(pd.DataFrame(target)))
        incoming = nest_journal(incoming)
        if incoming["id"].duplicated().any():
            raise ValueError("Non-unique dates in `target` transactions.")

        # Count occurrences of each unique transaction in current and incoming,
        # find number of additions and deletions for each unique transaction
        count = pd.DataFrame({
            "current": current["txn_str"].value_counts(),
            "incoming": incoming["txn_str"].value_counts(),
        })
        count = count.fillna(0).reset_index(names="txn_str")
        count["n_add"] = (count["incoming"] - count["current"]).clip(lower=0).astype(int)
        count["n_delete"] = (count["current"] - count["incoming"]).clip(lower=0).astype(int)

        # Handle deletions
        if delete and any(count["n_delete"] > 0):
            ids = [
                id
                for txn_str, n in zip(count["txn_str"], count["n_delete"])
                if n > 0
                for id in current.loc[current["txn_str"] == txn_str, "id"]
                .tail(n=n)
                .values
            ]
            self.delete({"id": ids})

        # Handle additions
        for txn_str, n in zip(count["txn_str"], count["n_add"]):
            if n > 0:
                txn = unnest(incoming.loc[incoming["txn_str"] == txn_str, :].head(1), "txn")
                txn.drop(columns="txn_str", inplace=True)
                if txn["id"].dropna().nunique() > 0:
                    id = txn["id"].dropna().unique()[0]
                else:
                    id = txn["description"].iat[0]
                for _ in range(n):
                    try:
                        self.add(txn)
                    except Exception as e:
                        raise Exception(
                            f"Error while adding journal entry {id}: {e}"
                        ) from e

        return {
            "initial": int(count["current"].sum()),
            "target": int(count["incoming"].sum()),
            "added": int(count["n_add"].sum()),
            "deleted": int(count["n_delete"].sum()) if delete else 0,
            "updated": 0
        }


class StandaloneAccountingEntity(AccountingEntity):
    """
    Abstract base class for local storage of tabular accounting data,
    without relying on an external system.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def _store(self, data: pd.DataFrame) -> None:
        """
        Update storage with an updated version of the DataFrame.

        Args:
            data (pd.DataFrame): The updated DataFrame to store.
        """

    def _prepare_addition(self, data: pd.DataFrame):
        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))
        overlap = pd.merge(current, incoming, on=self._id_columns, how='inner')
        if not overlap.empty:
            raise ValueError("Unique identifiers already exist.")
        combined = pd.concat([current, incoming], ignore_index=True)
        return incoming, combined

    def add(self, data: pd.DataFrame):
        incoming, combined = self._prepare_addition(data)
        self._store(combined)
        return incoming[self._id_columns].iloc[0].to_dict()

    def _prepare_modification(self, data: pd.DataFrame):
        current = self.list()
        data = pd.DataFrame(data)
        cols = set(self._schema["column"]).intersection(data.columns)
        cols = cols.union(self._schema.query("id")["column"])
        reduced_schema = self._schema.query("column in @cols")
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        missing = incoming[self._id_columns].merge(
            current[self._id_columns], on=self._id_columns, how='left', indicator=True
        )
        if any(missing['_merge'] != 'both'):
            raise ValueError("Some elements in 'data' are not present.")
        merged = current.merge(
            incoming, on=self._id_columns, how='left', suffixes=('', '_incoming'), indicator=True
        )
        if any(merged['_merge'] == 'right_only'):
            raise ValueError("Some elements in 'data' are not present.")
        incoming_cols = merged.columns[merged.columns.str.endswith("_incoming")]
        current_cols = incoming_cols.str.replace("_incoming$", "", regex=True)
        mask = merged["_merge"] == "both"
        replaced = merged.loc[mask, ~merged.columns.str.endswith("_incoming")]
        replaced = replaced.drop(columns=["_merge"])
        for current_col, incoming_col in zip(current_cols, incoming_cols):
            updated = merged.loc[mask, incoming_col].astype(merged[current_col].dtype)
            merged.loc[mask, current_col] = updated
        new = merged.drop(columns=["_merge", *incoming_cols])
        return incoming, replaced, new

    def modify(self, data: pd.DataFrame):
        _, _, new = self._prepare_modification(data)
        self._store(new)

    def _prepare_deletion(self, id: pd.DataFrame, allow_missing: bool = False):
        current = self.list()
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        if not allow_missing:
            missing = pd.merge(incoming, current, on=self._id_columns, how='left', indicator=True)
            if not missing[missing['_merge'] != 'both'].empty:
                raise ValueError("Some ids are not present in the data.")
        merged = current.merge(incoming, on=self._id_columns, how='left', indicator=True)
        new = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        drop = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])
        return drop, new

    def delete(self, id: pd.DataFrame, allow_missing: bool = False):
        _, new = self._prepare_deletion(id, allow_missing=allow_missing)
        self._store(new)


class DataFrameEntity(StandaloneAccountingEntity):
    """
    Stores tabular accounting data as a DataFrame in memory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._df = self.standardize(None)

    def list(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        return self.standardize(self._df.copy(), drop_extra_columns=drop_extra_columns)

    def _store(self, data: pd.DataFrame):
        self._df = data.reset_index(drop=True)
        self._on_change()


class JournalDataFrameEntity(JournalEntity, DataFrameEntity):
    """
    Journal entity that stores journal entries as a DataFrame in memory.
    """

    def modify(self, data: pd.DataFrame) -> None:
        """
        Modify existing journal entries.

        Overrides the base implementation because DataFrameEntity.modify does
        not support duplicate IDs.

        Args:
            data (pd.DataFrame): DataFrame containing journal entries to modify. Must contain all
                                 ID columns defined in the schema; other columns are optional.

        Raises:
            ValueError: If a combination of ID columns is not present in `data`.
        """
        self.delete(data, allow_missing=False)
        self.add(data)


class CSVAccountingEntity(StandaloneAccountingEntity):
    """Stores tabular accounting data in a fixed-width CSV file."""

    def __init__(
        self, path: Path | str, column_shortcuts: dict = {}, *args, **kwargs
    ):
        """Initialize the CSVAccountingEntity.

        Args:
            file_path (Path | str): Path to the CSV file.
            column_shortcuts (dict, optional): Mapping of column old names to new names.
        """
        super().__init__(*args, **kwargs)
        self._path = Path(path).expanduser()
        # TODO: remove once the old system is migrated
        self._column_shortcuts = column_shortcuts

    @timed_cache(120)
    def list(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        return self._read_data(drop_extra_columns=drop_extra_columns)

    def _store(self, data: pd.DataFrame, path: Path | str = None):
        """
        Store the DataFrame to a CSV file. If the DataFrame is empty, the CSV file is deleted.

        Args:
            data (pd.DataFrame): DataFrame to be stored.
            path (Path, optional): Path where the CSV file will be saved.
                Defaults to the instance's defined path.
        """
        if path is None:
            path = self._path
        else:
            path = Path(path).expanduser()

        if data.empty:
            path.unlink(missing_ok=True)
        else:
            self._write_file(data, path)
        self.list.cache_clear()
        self._on_change()

    def _read_data(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        """Read data from the CSV file.

        This method reads data from the file and enforces the standard
        data format. If an error occurs during reading or standardization, an empty
        DataFrame with standard SCHEMA is returned.
        """
        if self._path.exists():
            data = pd.read_csv(self._path, skipinitialspace=True)
            data.rename(columns=self._column_shortcuts, inplace=True)
        else:
            data = None
        return self.standardize(data, drop_extra_columns=drop_extra_columns)

    def _write_file(self, df: pd.DataFrame, path: Path | str):
        """Save data to a fixed-width CSV file.

        This method stores data in a fixed-width CSV format.
        Values are padded with spaces to maintain consistent column width and improve readability.
        Optional columns that contain only NA values are dropped for conciseness.
        """
        df = enforce_schema(df, self._schema, sort_columns=True, keep_extra_columns=True)
        optional = self._schema.loc[~self._schema["mandatory"], "column"].to_list()
        to_drop = [col for col in optional if df[col].isna().all() and not df.empty]
        df.drop(columns=to_drop, inplace=True)
        n_fixed = self._schema["column"].isin(df.columns).sum()
        write_fixed_width_csv(df, file=path, n=n_fixed)


class MultiCSVEntity(CSVAccountingEntity):
    """
    Stores tabular data in multiple CSV files within a root directory.

    Paths relative to the root directory are stored in a specified `file_path_column`.
    """

    def __init__(
        self,
        write_file: Callable[[pd.DataFrame, Path], None] = None,
        file_path_column: str = DEFAULT_FILE_PATH_COLUMN,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path_column = file_path_column
        self._write_file = write_file

    def _read_data(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        """Reads data from CSV files in the root directory.

        Iterates through all CSV files in the root directory, reading each file
        into a DataFrame and ensuring the data conforms to the entity's schema.
        Files that cannot be processed are skipped with a warning. The Data
        from all valid files is then combined into a single DataFrame.

        Returns:
            pd.DataFrame: DataFrame adhering to the entity's column schema.
                          Relative paths to the root directory are stored in an
                          in an additional configurable `file_path_column`.
        """
        if not self._path.exists():
            df = self.standardize(None)
            df[self.file_path_column] = pd.Series(dtype=pd.StringDtype())
            return df
        if not self._path.is_dir():
            raise NotADirectoryError(f"Root folder is not a directory: {self._path}")

        result = []
        for file in self._path.rglob("*.csv"):
            relative_path = str(file.relative_to(self._path))
            try:
                df = pd.read_csv(file, skipinitialspace=True)
                # TODO: Remove the following line once legacy systems are migrated.
                df = df.rename(columns=self._column_shortcuts)
                df = self.standardize(df, drop_extra_columns=drop_extra_columns)
                if not df.empty:
                    df[self.file_path_column] = relative_path
                result.append(df)
            except Exception as e:
                self._logger.warning(f"Skipping {relative_path}: {e}")

        if result:
            result = pd.concat(result, ignore_index=True)
            result = enforce_schema(result, self._schema,
                                    sort_columns=True, keep_extra_columns=not drop_extra_columns)
        else:
            result = self.standardize(None)
            result[self.file_path_column] = pd.Series(dtype=pd.StringDtype())

        return self.standardize(result, drop_extra_columns=drop_extra_columns)

    def add(self, data: pd.DataFrame, default_path: str = "default.csv") -> list[str]:
        """Add new entries.

        Args:
            data (pd.DataFrame): DataFrame containing new entries to add,
                                compatible with the entity's DataFrame schema.
            default_path (str, optional): The file where data with missing (NA)
                                        `file_path_column` values will be stored.

        Returns:
            pd.DataFrame: A list containing the unique identifiers of the added data.
        """
        incoming = pd.DataFrame(data)
        col = self.file_path_column
        if col in incoming.columns:
            incoming[col] = incoming[col].astype(pd.StringDtype())
            incoming[col] = incoming[col].fillna(default_path)
        elif len(incoming) > 0:
            incoming[col] = default_path
            incoming[col] = incoming[col].astype(pd.StringDtype())
        else:
            incoming[col] = pd.Series(dtype=pd.StringDtype)
        incoming, combined = self._prepare_addition(incoming)
        paths_to_update = incoming[col].unique()
        for path in paths_to_update:
            full_path = self._path / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            df = combined.query(f"`{col}` == @path")
            df = df.drop(columns=col)
            self._store(df, full_path)
        return incoming[self._id_columns].to_dict()

    def modify(self, data: pd.DataFrame):
        col = self.file_path_column
        incoming, replaced, new = self._prepare_modification(data)
        paths_to_update = set(incoming[col]).union(set(replaced[col]))
        for path in paths_to_update:
            full_path = self._path / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            df = new.query(f"`{col}` == @path")
            df = df.drop(columns=col)
            self._store(df, full_path)

    def delete(self, id: pd.DataFrame, allow_missing: bool = False):
        col = self.file_path_column
        drop, new = self._prepare_deletion(id, allow_missing=allow_missing)
        paths_to_update = drop[col].unique()
        for path in paths_to_update:
            df = new.query(f"`{col}` == @path")
            df = df.drop(columns=col)
            self._store(df, self._path / path)


class CSVJournalEntity(JournalEntity, MultiCSVEntity):
    """
    Stores journal entries in multiple CSV files, with files determined by the
    ID portion of the entries up to the first colon ':'.

    IDs are not stored in journal files but are dynamically generated when
    reading each file. These IDs are non-persistent and may change if a file's
    entries are modified. See `_read_data` for details.
    """

    def _csv_path(self, id: pd.Series) -> pd.Series:
        """Extract storage path from journal id."""
        return id.str.replace(":[^:]+$", "", regex=True)

    def _id_from_path(self, id: pd.Series) -> pd.Series:
        """Extract numeric portion of journal id."""
        return id.str.replace("^.*:", "", regex=True).astype(int)

    def _read_data(self, drop_extra_columns: bool = False) -> pd.DataFrame:
        """Reads journal entries from CSV files in the root directory.

        Iterates through all CSV files in the root directory, reading each file
        into a DataFrame and ensuring the data conforms to the entity's schema.
        Files that cannot be processed are skipped with a warning. The Data
        from all valid files is then combined into a single DataFrame.

        IDs are not stored in journal files but are dynamically generated when
        reading each file. The `id` is constructed by combining the relative
        path to the root directory with the row's position, separated by a
        colon (`{path}:{position}`). These IDs are non-persistent and may
        change if a file's entries are modified. Successive rows that belong to
        the same transaction are identified by recording the date only on the
        first row; subsequent rows without a date are considered part of the
        same transaction.

        Returns:
            pd.DataFrame: DataFrame adhering to the entity's column schema.
        """
        df = super()._read_data(drop_extra_columns=False)
        if not df.empty:
            df["id"] = df[self.file_path_column] + ":" + df["id"]
        df = df.drop(columns=self.file_path_column)
        return self.standardize(df, drop_extra_columns=drop_extra_columns)

    def write_directory(self, df: pd.DataFrame | None = None):
        """Save journal entries to multiple CSV files in the journal directory.

        Saves journal entries to several fixed-width CSV files, formatted by
        `_write_file`. The storage location within the `<root>/journal`
        directory is determined by the portion of the ID up to the last
        colon (':').

        Args:
            df (pd.DataFrame, optional): The journal DataFrame to save.
                If not provided, defaults to the current journal data.
        """
        if df is None:
            df = self.list()

        df = self.standardize(df).copy()
        df[self.file_path_column] = self._csv_path(df["id"])
        save_files(
            df, root=self._path, file_path_column=self.file_path_column, func=self._write_file
        )
        self.list.cache_clear()
        self._on_change()

    def add(self, data: pd.DataFrame, path: str = "default.csv") -> list[str]:
        """Add new entries.

        IDs in the input `data` are not conserved. IDs are not stored in journal
        files but are dynamically generated when reading each file.

        Args:
            data (pd.DataFrame): DataFrame containing new entries to add,
                                compatible with the entity's DataFrame schema.
            path (str, optional): The file path where the data will be stored.

        Returns:
            pd.DataFrame: A list containing the unique identifiers of the added data.
        """

        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))
        df_same_file = current[self._csv_path(current["id"]) == path]

        # Assign unique IDs incrementing from the max ID
        id = 0 if df_same_file.empty else self._id_from_path(df_same_file["id"]).max()
        incoming["id"] = incoming["id"].rank(method="dense").astype(int) + id
        incoming["id"] = f"{path}:" + incoming["id"].astype(str)

        df = pd.concat([df_same_file, incoming], ignore_index=True)
        full_path = self._path / path
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        self._store(df, full_path)

        return incoming["id"].to_list()

    def modify(self, data: pd.DataFrame):
        """
        Modify existing journal entries.

        Overrides the base implementation because `JournalDataFrameEntity.modify`
        does not preserve the order of rows within the file.

        Args:
            data (pd.DataFrame): DataFrame containing journal entries to modify. Must contain all
                required columns as defined in the schema; other columns are optional.

        Raises:
            ValueError: If a combination of ID columns is not present in `data`.

        Notes:
            - Modifying only specific columns isn't supported. Collective transactions span
            multiple rows and it is impossible to identify which row to update.
        """
        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))

        missing = pd.merge(incoming, current, on=self._id_columns, how='left', indicator=True)
        if not missing[missing['_merge'] != 'both'].empty:
            raise ValueError("Some ids are not present in the data.")

        current = current.query("id not in @incoming['id']")
        updated = pd.concat([current, incoming], ignore_index=True)
        paths_to_update = self._csv_path(incoming["id"]).unique()
        for path in paths_to_update:
            df_same_file = updated[self._csv_path(updated["id"]) == path]
            df_same_file = df_same_file.iloc[
                self._id_from_path(df_same_file["id"]).argsort(kind='mergesort')
            ]
            self._store(df_same_file, self._path / path)

    def delete(self, id: pd.DataFrame, allow_missing: bool = False):
        current = self.list()
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        if not allow_missing:
            missing = pd.merge(incoming, current, on=self._id_columns, how='left', indicator=True)
            if not missing[missing['_merge'] != 'both'].empty:
                raise ValueError("Some ids are not present in the data.")
        new = current.merge(incoming, on=self._id_columns, how='left', indicator=True)
        new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])

        paths_to_update = self._csv_path(incoming["id"]).unique()
        for path in paths_to_update:
            self._store(new[self._csv_path(new["id"]) == path], self._path / path)
