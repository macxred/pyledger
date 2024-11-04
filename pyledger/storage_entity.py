"""Provides abstract storage entities for accounting data."""

from abc import ABC, abstractmethod
import pandas as pd
from consistent_df import enforce_schema

class AccountingEntity(ABC):
    """
    Abstract base class for accounting entities, such as general ledger, account chart,
    tax codes or configuration settings.
    """

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

    def __init__(self, schema, prepare_for_mirroring = lambda x: x):
        self._schema = schema
        self._id_columns = schema.query("id == True")["column"]
        self._prepare_for_mirroring = prepare_for_mirroring

    def standardize(self, data: pd.DataFrame | None, keep_extra_columns=False) -> pd.DataFrame:
        """
        Convert data to a consistent representation.

        Validates that data meets the requirements for the specific entity and
        standardizes data to ensure it contains all columns, correct data types,
        and no missing values in key fields.

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

        invokes a 'prepare_for_mirroring' method set by the owning instance
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

        See Also:
            prepare_assets_for_mirroring : Prepares the target data for synchronization.
        """
        current = self.list()
        incoming = self._prepare_for_mirroring(self.standardize(pd.DataFrame(target)))
        merged = current.merge(
            incoming, on=self._id_columns, how="outer", suffixes=('_current', '_incoming'),
            indicator=True
        )

        # Handle deletions
        if delete:
            to_delete = merged[merged["_merge"] == "left_only"]
            for _, row in to_delete.iterrows():
                self.delete(ticker=row["ticker"], date=row["date"])

        # Handle additions
        to_add = merged[merged["_merge"] == "right_only"]
        for _, row in to_add.iterrows():
            self.add(
                ticker=row["ticker"], increment=row["increment_incoming"], date=row["date"]
            )

        # Handle updates
        non_id_cols = [col for col in self._schema['column'] if col not in self._id_columns]
        non_id_current_cols = [col + '_current' for col in non_id_cols]
        non_id_incoming_cols = [col + '_incoming' for col in non_id_cols]
        both_rows = merged[merged['_merge'] == 'both']
        df_current = both_rows[non_id_current_cols]
        df_incoming = both_rows[non_id_incoming_cols]
        diff = df_current.ne(df_incoming).any(axis=1)
        to_update = both_rows[diff]
        for _, row in to_update.iterrows():
            # TODO: Only use '_incoming' columns, drop '_incoming' from names.
            self.modify(**row)

        return {
            "initial": len(current),
            "target": len(incoming),
            "added": len(to_add),
            "deleted": len(to_delete) if delete else 0,
            "updated": len(to_update)
        }


class StandaloneTabularEntity(TabularEntity):
    """
    Abstract base class for local storage of tabular accounting data, without
    relying on an external system.
    """

    @abstractmethod
    def _store(self, data: pd.DataFrame):
        """Update storage with an updated version of the DataFrame."""

    def add(self, data: pd.DataFrame):
        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))
        overlap = pd.merge(current, incoming, on=self.id_columns, how='inner')
        if not overlap.empty:
            raise ValueError("Unique identifiers already exist.")
        self._store(pd.concat([self.current, incoming], ignore_index=True))

    def modify(self, data: pd.DataFrame):
        current = self.list()
        incoming = self.standardize(pd.DataFrame(data))
        missing = pd.merge(incoming, self.current, on=self.id_columns, how='left', indicator=True)
        if not missing.empty:
            raise ValueError("Some elements in 'data' are not present.")
        for _, row in incoming.iterrows():
            mask = (current[self.id_columns] == incoming[incoming.id_columns].values).all(axis=1)
            if current.columns != incoming.columns:
                raise NotImplementedError(
                    "Modify with a differing set of columns is not implemented yet.")
            self.current.loc[mask] = incoming
        self._store(current)

    def delete(self, id: pd.DataFrame, allow_missing: bool = False):
        current = self.list()
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        if not allow_missing:
            missing = pd.merge(incoming, current, on=self.id_columns, how='left', indicator=True)
            if not missing.empty:
               raise ValueError("Some ids are not present in the data.")
        new = current.merge(id, on=self.id_columns, how='left', indicator=True)
        new = new[new['_merge'] == 'left_only'].drop(columns=['_merge'])
        self._store(new)

class DataFrameEntity(StandaloneTabularEntity):
    """Stores tabular accounting data as a DataFrame in memory."""

    def __init__(self):
        super.__init__(self)
        self._df = self.standardize(None)

    def list(self) -> pd.DataFrame:
        self._df.copy()

    def _store(self, data: pd.DataFrame):
        self._df = data.reset_index(drop=True)
