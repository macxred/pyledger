"""Provides abstract storage entities for accounting data."""

from abc import ABC, abstractmethod
import pandas as pd
from consistent_df import enforce_schema, df_to_consistent_str, nest, unnest
from pyledger.decorators import timed_cache


class AccountingEntity(ABC):
    """
    Abstract base class for accounting entities, such as general ledger, account chart,
    tax codes or configuration settings.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def standardize(self, data):
        pass

    @abstractmethod
    def list(self):
        pass

    @abstractmethod
    def add(self, data):
        pass

    @abstractmethod
    def modify(self, data):
        pass

    @abstractmethod
    def delete(self, id, allow_missing: bool = False):
        pass

    @abstractmethod
    def mirror(self, target, delete: bool = False):
        pass


class TabularEntity(AccountingEntity):
    """
    Abstract base class for storage of accounting entities in tabular form,
    such as general ledger, account chart or tax codes. Accessors return type
    consistent pandas DataFrames in each entity's specific column schema.
    """

    def __init__(self, schema, prepare_for_mirroring=lambda x: x, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema
        self._id_columns = schema.query("id == True")["column"].to_list()
        self._prepare_for_mirroring = prepare_for_mirroring

    def standardize(self, data: pd.DataFrame, keep_extra_columns: bool = False) -> pd.DataFrame:
        """
        Convert data to a consistent representation.

        Validates that required columns are present, fills any missing optional
        columns with NA, and enforces specified data types for each column.
        If applicable, performs entity-specific data standardization.

        Args:
            data (pd.DataFrame): The DataFrame to standardize.
            keep_extra_columns (bool): If True, retains columns outside the defined schema.

        Returns:
            pd.DataFrame: The standardized DataFrame.

        Raises:
            ValueError: If data types are incorrect or a required column is missing.
        """
        return enforce_schema(data, self._schema, keep_extra_columns=keep_extra_columns)

    @abstractmethod
    def list(self) -> pd.DataFrame:
        """Retrieves all entries.

        Returns:
            pd.DataFrame that adheres to the entity's column schema.
        """

    @abstractmethod
    def add(self, data: pd.DataFrame) -> None:
        """
        Adds new entries.

        Parameters:
            data (pd.DataFrame): DataFrame with new entries to add.

        Raises:
            ValueError: If the IDs in `date` are already present.
        """
        pass

    @abstractmethod
    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> pd.DataFrame:
        """
        Deletes entries.

        Parameters:
            id (pd.DataFrame): DataFrame with IDs of entries to be deleted.
            allow_missing (bool): If True, no error is raised if an id is not present.

        Raises:
            ValueError: If the IDs in `date` are not present and not `allow_missing`.
        """
        pass

    @abstractmethod
    def modify(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Modifies entries.

        Parameters:
            data (pd.DataFrame): The DataFrame containing entries to be modified.

        Raises:
            ValueError: If the IDs in `date` are not present.
        """
        pass

    def mirror(self, target: pd.DataFrame, delete: bool = False) -> dict:
        """Align current data with the incoming target data.

        Updates the current data to match the `target` by adding new entries,
        modifying existing ones, and optionally deleting entries not present
        in `target`.

        Invokes a 'prepare_for_mirroring' method set by the owning instance
        as the initial step in the mirroring process. This method
        matches the incoming DataFrame with the current system's requirements
        and aligns it with existing data, facilitating the identification
        of entries that need to be added, modified, or removed.

        Args:
            target (pd.DataFrame): The desired target state.
            delete (bool): If True, deletes current entries that are not
                           present in the `target`.

        Returns:
            dict: Summary statistics of the mirroring process:
                - 'initial' (int): Number of assets before synchronization.
                - 'target' (int): Number of assets in the target DataFrame.
                - 'added' (int): Number of assets added.
                - 'deleted' (int): Number of assets deleted.
                - 'updated' (int): Number of assets updated.
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
    TabularEntity adapted to specific needs of ledger entries, with custom
    standardize and mirror methods.
    """
    # # Standardize data frame schema, discard incoherent entries with a warning
    #
    # target = self.sanitize_ledger(target)

    def __init__(self, schema, *args, **kwargs):
        super().__init__(schema, *args, **kwargs)

    def standardize(self, data: pd.DataFrame, keep_extra_columns: bool = False) -> pd.DataFrame:
        df = enforce_schema(data, self._schema, keep_extra_columns=keep_extra_columns)

        # Add id column if missing: Entries without a date share id of the last entry with a date
        if df["id"].isna().all():
            id_type = self._schema.query("column == 'id'")['dtype'].item()
            df["id"] = df["date"].notna().cumsum().astype(id_type)

        # Fill missing (NA) dates
        df["date"] = df.groupby("id")["date"].ffill()
        df["date"] = df.groupby("id")["date"].bfill()
        df["date"] = df["date"].dt.tz_localize(None).dt.floor('D')

        # Convert -0.0 to 0.0
        for col in df.columns:
            if pd.Float64Dtype.is_dtype(df[col]):
                df.loc[df[col].notna() & (df[col] == 0), col] = 0.0

        return df

    def mirror(self, target: pd.DataFrame, delete: bool = False) -> dict:

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
            # We expect nesting to combine all rows with the same
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
    Abstract base class for local storage of tabular accounting data, without
    relying on an external system.
    """

    def __init__(self, schema, prepare_for_mirroring=lambda x: x, *args, **kwargs):
        super().__init__(schema, prepare_for_mirroring, *args, **kwargs)

    @abstractmethod
    def _store(self, data: pd.DataFrame):
        """Update storage with an updated version of the DataFrame."""

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
        incoming = self.standardize(pd.DataFrame(data))
        missing = pd.merge(incoming, current, on=self._id_columns, how='left', indicator=True)
        if not missing[missing['_merge'] != 'both'].empty:
            raise ValueError("Some elements in 'data' are not present.")
        for _, row in incoming.iterrows():
            if set(current.columns) != set(incoming.columns):
                raise NotImplementedError(
                    "Modify with a differing set of columns is not implemented yet.")
            incoming = incoming[current.columns.to_list()]
            mask = (current[self._id_columns].fillna("NA")
                    == incoming[self._id_columns].fillna("NA").values).all(axis=1)
            current.loc[mask] = incoming.values
        self._store(current)

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
    """Stores tabular accounting data as a DataFrame in memory."""

    def __init__(self, schema, *args, **kwargs):
        super().__init__(schema, *args, **kwargs)
        self._df = self.standardize(None)

    @timed_cache(15)
    def list(self) -> pd.DataFrame:
        return self._df.copy()

    def _store(self, data: pd.DataFrame):
        self._df = data.reset_index(drop=True)
        self.list.cache_clear()


class LedgerDataFrameEntity(TabularLedgerEntity, DataFrameEntity):

    def modify(self, data: pd.DataFrame):
        # DataFrameEntity.modify does not work for duplicate ids
        self.delete(data, allow_missing=False)
        self.add(data)
