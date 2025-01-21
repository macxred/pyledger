"""This module defines TextLedger, extending StandaloneLedger to store data in text files."""

import math
import pandas as pd
import yaml
from pathlib import Path
from .decorators import timed_cache
from .standalone_ledger import StandaloneLedger
from .constants import (
    ACCOUNT_SCHEMA,
    ASSETS_SCHEMA,
    PROFIT_CENTER_SCHEMA,
    DEFAULT_CONFIGURATION,
    LEDGER_SCHEMA,
    PRICE_SCHEMA,
    REVALUATION_SCHEMA,
    TAX_CODE_SCHEMA
)
from .helpers import write_fixed_width_csv
from consistent_df import enforce_schema
from .storage_entity import CSVAccountingEntity, CSVLedgerEntity


# TODO: remove once old systems are migrated
LEDGER_COLUMN_SHORTCUTS = {
    "cur": "currency",
    "vat": "tax_code",
    "base_amount": "report_amount",
    "counter": "contra",
    "text": "description"
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
    with spaces for consistent column widths. Configuration including
    the reporting currency, are stored in YAML format.
    """

    def __init__(self, root: Path = Path.cwd()):
        """Initializes the TextLedger with a root path for file storage.
        If no root path is provided, defaults to the current working directory.
        """
        super().__init__()
        self.root = Path(root).expanduser()
        settings_dir = self.root / "settings"
        settings_dir.mkdir(parents=True, exist_ok=True)
        self._assets = CSVAccountingEntity(
            schema=ASSETS_SCHEMA, path=self.root / "settings/assets.csv"
        )
        self._accounts = CSVAccountingEntity(
            schema=ACCOUNT_SCHEMA, path=self.root / "account_chart.csv",
            column_shortcuts=ACCOUNT_COLUMN_SHORTCUTS,
            on_change=self.serialized_ledger.cache_clear
        )
        self._tax_codes = CSVAccountingEntity(
            schema=TAX_CODE_SCHEMA, path=self.root / "settings/tax_codes.csv",
            column_shortcuts=TAX_CODE_COLUMN_SHORTCUTS,
            on_change=self.serialized_ledger.cache_clear
        )
        self._price_history = CSVAccountingEntity(
            schema=PRICE_SCHEMA, path=self.root / "settings/price_history.csv"
        )
        self._revaluations = CSVAccountingEntity(
            schema=REVALUATION_SCHEMA, path=self.root / "settings/evaluations.csv"
        )
        self._ledger = CSVLedgerEntity(
            schema=LEDGER_SCHEMA,
            path=self.root / "journal",
            write_file=self.write_ledger_file,
            column_shortcuts=LEDGER_COLUMN_SHORTCUTS,
            prepare_for_mirroring=self.sanitize_ledger,
            on_change=self.serialized_ledger.cache_clear,
        )
        self._profit_centers = CSVAccountingEntity(
            schema=PROFIT_CENTER_SCHEMA, path=self.root / "settings/profit_center.csv"
        )

    # ----------------------------------------------------------------------
    # Configuration

    @property
    @timed_cache(120)
    def configuration(self):
        return self.read_configuration_file(self.root / "settings/configuration.yml").copy()

    @configuration.setter
    def configuration(self, configuration: dict):
        """Save configuration to a YAML file.

        This method stores accounting configuration such as the reporting currency
        to `<root>/settings/configuration.yml`. The YAML format is ideal for version control
        and human readability.

        Args:
            configuration (dict): A dictionary containing the system configuration to be saved.
        """
        with open(self.root / "settings/configuration.yml", "w") as f:
            yaml.dump(self.standardize_configuration(configuration), f, default_flow_style=False)
        self.__class__.configuration.fget.cache_clear()

    def read_configuration_file(self, file: Path) -> dict:
        """Read configuration from the specified file.

        This method returns standardized accounting configuration, including the reporting currency.
        If the specified configuration file does not exist, DEFAULT_CONFIGURATION are returned.
        The system thus continues running even if the <root> directory is empty, which is useful
        for testing and demonstration purposes.

        Args:
            file (Path): The path to the configuration file.

        Returns:
            dict: Standardized system configuration.
        """
        if file.exists():
            with open(file, "r") as f:
                result = yaml.safe_load(f)
        else:
            self._logger.warning("configuration file missing, reverting to default configuration.")
            result = self.standardize_configuration(DEFAULT_CONFIGURATION)

        return result

    # ----------------------------------------------------------------------
    # Ledger

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
        df = df.iloc[self.ledger._id_from_path(df["id"]).argsort(kind="mergesort")]
        df["date"] = df["date"].where(~df.duplicated(subset="id"), None)

        # Apply the smallest precision
        def format_with_precision(series: pd.Series, precision: float) -> pd.Series:
            """Formats a series to a specific decimal precision."""
            decimal_places = -1 * math.floor(math.log10(precision))
            return series.apply(lambda x: pd.NA if pd.isna(x) else f"{x:.{decimal_places}f}")

        increment = df.apply(lambda row: self.precision(row["currency"], row["date"]), axis=1).min()
        df["amount"] = format_with_precision(df["amount"], increment)
        df["report_amount"] = format_with_precision(
            df["report_amount"], self.precision(self.reporting_currency)
        )

        # Drop columns that are all NA and not required by the schema
        na_columns = df.columns[df.isna().all()]
        mandatory_columns = LEDGER_SCHEMA["column"][LEDGER_SCHEMA["mandatory"]]
        df = df.drop(columns=set(na_columns).difference(mandatory_columns) | {"id"})

        # Write a CSV with fixed-width in all columns but the last two in the schema
        n_fixed = LEDGER_SCHEMA["column"].head(-2).isin(df.columns).sum()
        Path(file).expanduser().parent.mkdir(parents=True, exist_ok=True)
        write_fixed_width_csv(df, file=file, n=n_fixed)

        return df

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self):
        return self.configuration["reporting_currency"]

    @reporting_currency.setter
    def reporting_currency(self, currency):
        self.configuration = self.configuration | {"reporting_currency": currency}
