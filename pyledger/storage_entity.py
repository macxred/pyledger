"""Provides abstract storage entities for accounting data."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, Any
import pandas as pd
from consistent_df import enforce_schema, df_to_consistent_str, nest, unnest
from pyledger.decorators import timed_cache
from pyledger.helpers import write_fixed_width_csv


class AccountingEntity(ABC):
    """
    Abstract base class representing an accounting entity (e.g., general ledger,
    account chart, tax codes, or configuration settings). Defines the standard
    interface that all accounting entities must implement.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def standardize(self, data):
        """Convert data to a consistent representation."""

    @abstractmethod
    def list(self):
        """Retrieve all entries."""

    @abstractmethod
    def add(self, data):
        """Add new entries."""

    @abstractmethod
    def modify(self, data):
        """Modify existing entries."""

    @abstractmethod
    def delete(self, id, allow_missing: bool = False):
        """
        Delete entries.

        Args:
            id (pd.DataFrame): Identifiers of entries to delete.
            allow_missing (bool, optional): If True, don't raise an error if an ID is not present.
        """

    @abstractmethod
    def mirror(self, target, delete: bool = False) -> Dict[str, int]:
        """
        Synchronize the current data with the incoming target data.

        Args:
            target (pd.DataFrame): The desired target state.
            delete (bool, optional): If True, deletes current entries not present in `target`.

        Returns:
            Dict[str, int]: Summary statistics of the mirroring process.
        """


class TabularEntity(AccountingEntity):
    """
    Abstract base class for accounting entities stored in tabular form (e.g.,
    general ledger, account chart, tax codes). Accessors return pandas DataFrames
    consistent with the entity's specific column schema.
    """

    def __init__(
        self,
        schema: pd.DataFrame,
        prepare_for_mirroring: Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Initialize the TabularEntity.

        Args:
            schema (pd.DataFrame): DataFrame with columns 'column', 'dtype', 'id' (boolean)
                                   defining the entity's DataFrame schema.
            prepare_for_mirroring (Callable[[pd.DataFrame], pd.DataFrame], optional):
                Function to prepare data for mirroring. Defaults to identity function.
            *args, **kwargs: Additional arguments passed to the superclass.
        """
        super().__init__(*args, **kwargs)
        self._schema = schema
        self._id_columns = schema.query("id == True")["column"].to_list()
        self._prepare_for_mirroring = prepare_for_mirroring

    def standardize(self, data: pd.DataFrame, keep_extra_columns: bool = False) -> pd.DataFrame:
        """
        Standardize the given DataFrame to conform to the entity's schema.

        Validates that required columns are present, fills any missing optional
        columns with NA, and enforces specified data types for each column.
        Child classes may perform additional, entity-specific data conversions.

        Args:
            data (pd.DataFrame): DataFrame compatible with the entity's DataFrame schema.
            keep_extra_columns (bool, optional): If True, retains columns
                                                 outside the defined schema.

        Returns:
            pd.DataFrame: The standardized DataFrame.

        Raises:
            ValueError: If required columns are missing or data types are incorrect.
        """
        df = enforce_schema(data, self._schema, keep_extra_columns=keep_extra_columns)

        # Convert -0.0 to 0.0
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                df.loc[df[col].notna() & (df[col] == 0.0), col] = 0.0

        return df

    @abstractmethod
    def list(self) -> pd.DataFrame:
        """
        Retrieve all entries.

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
        diff = current_rows.ne(both_rows[incoming_cols]).any(axis=1)
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


class TabularLedgerEntity(TabularEntity):
    """
    Specialized TabularEntity for general ledger entries, with custom
    `standardize` and `mirror` methods tailored to ledger-specific needs.

    Notes:
        - IDs are not preserved during operations.
        - Deletion may alter existing IDs.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def standardize(self, data: pd.DataFrame, keep_extra_columns: bool = False) -> pd.DataFrame:
        """
        Standardize ledger data to conform to the entity's schema.

        Performs additional processing specific to ledger entries, including:
        - Adds 'id' column if missing, assigning IDs based on consecutive non-null dates.
        - Fills missing 'date' values by forward and backward filling within 'id' groups.

        Args:
            data (pd.DataFrame): DataFrame compatible with the entity's DataFrame schema.
            keep_extra_columns (bool, optional): If True, retain columns outside the schema.

        Returns:
            pd.DataFrame: The standardized DataFrame.

        Raises:
            ValueError: If required columns are missing or data types are incorrect.
        """
        df = super().standardize(data=data, keep_extra_columns=keep_extra_columns)

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
        Synchronize the current ledger data with the target ledger data.

        Custom implementation for ledger entries that accounts for
        transactions spanning multiple rows.

        Args:
            target (pd.DataFrame): DataFrame representing the desired target ledger state.
            delete (bool, optional): If True, deletes current ledger entries not present in target.

        Returns:
            Dict[str, int]: Summary statistics of the mirroring process containing:
                - 'initial' (int): Number of ledger entries before synchronization.
                - 'target' (int): Number of ledger entries in the target DataFrame.
                - 'added' (int): Number of ledger entries added.
                - 'deleted' (int): Number of ledger entries deleted.
                - 'updated' (int): Number of ledger entries updated (always 0 for ledger entries).

        Notes:
            - IDs are not preserved during the mirroring process.
            - Deletions may alter existing IDs.
        """

        def nest_ledger(df: pd.DataFrame) -> pd.DataFrame:
            """Nest to create one row per transaction, add unique string identifier."""
            nest_by = [col for col in df.columns if col not in ["id", "date"]]
            df = nest(df, columns=nest_by, key="txn")
            df["txn_str"] = [
                f"{str(date)},{df_to_consistent_str(txn)}"
                for date, txn in zip(df["date"], df["txn"])
            ]
            return df

        current = nest_ledger(self.list())
        incoming = self._prepare_for_mirroring(self.standardize(pd.DataFrame(target)))
        incoming = nest_ledger(incoming)
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
                            f"Error while adding ledger entry {id}: {e}"
                        ) from e

        return {
            "initial": int(count["current"].sum()),
            "target": int(count["incoming"].sum()),
            "added": count["n_add"].sum(),
            "deleted": count["n_delete"].sum() if delete else 0,
            "updated": 0
        }


class StandaloneTabularEntity(TabularEntity):
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

    def add(self, data: pd.DataFrame):
        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))
        overlap = pd.merge(current, incoming, on=self._id_columns, how='inner')
        if not overlap.empty:
            raise ValueError("Unique identifiers already exist.")
        self._store(pd.concat([current, incoming], ignore_index=True))
        return incoming[self._id_columns].iloc[0].to_dict()

    def modify(self, data: pd.DataFrame):
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
        for current_col, incoming_col in zip(current_cols, incoming_cols):
            merged.loc[mask, current_col] = merged.loc[mask, incoming_col]
        new = merged.drop(columns=["_merge", *incoming_cols])
        self._store(new)

    def delete(self, id: pd.DataFrame, allow_missing: bool = False):
        current = self.list()
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        if not allow_missing:
            missing = pd.merge(incoming, current, on=self._id_columns, how='left', indicator=True)
            if not missing[missing['_merge'] != 'both'].empty:
                raise ValueError("Some ids are not present in the data.")
        new = current.merge(incoming, on=self._id_columns, how='left', indicator=True)
        new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])
        self._store(new)


class DataFrameEntity(StandaloneTabularEntity):
    """
    Stores tabular accounting data as a DataFrame in memory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._df = self.standardize(None)

    def list(self) -> pd.DataFrame:
        return self._df.copy()

    def _store(self, data: pd.DataFrame):
        self._df = data.reset_index(drop=True)


class LedgerDataFrameEntity(TabularLedgerEntity, DataFrameEntity):
    """
    Ledger entity that stores ledger entries as a DataFrame in memory.

    Notes:
        - IDs are not preserved during modification.
        - Deleting entries may alter existing IDs.
    """

    def modify(self, data: pd.DataFrame) -> None:
        """
        Modify existing ledger entries.

        Overrides the base implementation because DataFrameEntity.modify does
        not support duplicate IDs.

        Args:
            data (pd.DataFrame): DataFrame containing ledger entries to modify. Must contain all
                                 ID columns defined in the schema; other columns are optional.

        Raises:
            ValueError: If a combination of ID columns is not present in `data`.

        Notes:
            - IDs are not preserved during modification.
        """
        self.delete(data, allow_missing=False)
        self.add(data)


class CSVDataFrameEntity(StandaloneTabularEntity):
    """Stores tabular accounting data in a fixed-width CSV file."""

    def __init__(
        self, file_path: Path, column_shortcuts: dict = {}, *args, **kwargs
    ):
        """Initialize the CSVDataFrameEntity.

        Args:
            file_path (Path): Path to the CSV file.
            column_shortcuts (dict, optional): Mapping of column shortcuts to full names.
        """
        super().__init__(*args, **kwargs)
        self._file_path = file_path
        # TODO: remove once the old system is migrated
        self._column_shortcuts = column_shortcuts

    @timed_cache(15)
    def list(self) -> pd.DataFrame:
        return self._read_file()

    def _store(self, data: pd.DataFrame):
        """Store the DataFrame to the CSV file.
        If the DataFrame is empty, the CSV file is deleted if it exists.
        """
        if data.empty:
            self._file_path.unlink(missing_ok=True)
        else:
            self._write_file(data)
        self.list.cache_clear()

    def _read_file(self) -> pd.DataFrame:
        """Read data from the CSV file.

        This method reads data from the file and enforces the standard
        data format. If an error occurs during reading or standardization, an empty
        DataFrame with standard SCHEMA is returned.
        """
        try:
            data = pd.read_csv(self._file_path, skipinitialspace=True)
            data.rename(columns=self._column_shortcuts, inplace=True)
        except Exception:
            data = None
        return self.standardize(data)

    def _write_file(self, df: pd.DataFrame):
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
        write_fixed_width_csv(df, file=self._file_path, n=n_fixed)
