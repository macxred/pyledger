"""This module defines TextLedger, extending StandaloneLedger to store data in text files."""

import pandas as pd
import yaml
from pathlib import Path
from pyledger.decorators import timed_cache
from pyledger.standalone_ledger import StandaloneLedger
from pyledger.constants import (
    ACCOUNT_SCHEMA,
    ASSETS_SCHEMA,
    DEFAULT_SETTINGS,
    LEDGER_SCHEMA,
    PRICE_SCHEMA,
    REVALUATION_SCHEMA,
    TAX_CODE_SCHEMA
)
from pyledger.helpers import write_fixed_width_csv
from consistent_df import enforce_schema
from pyledger.storage_entity import CSVDataFrameEntity, LedgerCSVDataFrameEntity


# TODO: remove once old systems are migrated
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
        super().__init__()
        root_path = Path(root_path).expanduser()
        self.root_path = root_path
        self._assets = CSVDataFrameEntity(
            schema=ASSETS_SCHEMA, file_path=root_path / "assets.csv"
        )
        self._accounts = CSVDataFrameEntity(
            schema=ACCOUNT_SCHEMA, file_path=root_path / "accounts.csv"
        )
        self._tax_codes = CSVDataFrameEntity(
            schema=TAX_CODE_SCHEMA, file_path=root_path / "tax_codes.csv"
        )
        self._price_history = CSVDataFrameEntity(
            schema=PRICE_SCHEMA, file_path=root_path / "price_history.csv"
        )
        self._revaluations = CSVDataFrameEntity(
            schema=REVALUATION_SCHEMA, file_path=root_path / "revaluations.csv"
        )
        self._ledger = LedgerCSVDataFrameEntity(
            schema=LEDGER_SCHEMA,
            root_path=root_path,
            write_file=self.write_ledger_file,
            prepare_for_mirroring=self.sanitize_ledger
        )

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

    def _id_from_path(self, id: pd.Series) -> pd.Series:
        """Extract numeric portion of ledger id."""
        return id.str.replace("^.*:", "", regex=True).astype(int)

    def write_ledger_file(self, df: pd.DataFrame, file: str) -> pd.DataFrame:
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
        df = df.iloc[self._id_from_path(df["id"]).argsort(kind="mergesort")]
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        # Drop columns that are all NA and not required by the schema
        na_columns = df.columns[df.isna().all()]
        mandatory_columns = LEDGER_SCHEMA["column"][LEDGER_SCHEMA["mandatory"]]
        df = df.drop(columns=set(na_columns).difference(mandatory_columns) | {"id"})

        # Write a CSV with fixed-width in all columns but the last two in the schema
        n_fixed = LEDGER_SCHEMA["column"].head(-2).isin(df.columns).sum()
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    @property
    def reporting_currency(self):
        return self.settings["reporting_currency"]

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self.settings = self.settings | {"reporting_currency": currency}
