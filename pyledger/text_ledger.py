"""This module defines the TextLedger class, an extension of StandaloneLedger,
designed to read and process ledger and price data from text files.
"""

import json
import logging
from pathlib import Path
import pandas as pd
from .helpers import write_fixed_width_csv
from .standalone_ledger import StandaloneLedger
from .constants import (
    REQUIRED_LEDGER_COLUMNS,
    OPTIONAL_LEDGER_COLUMNS,
    LEDGER_COLUMN_SEQUENCE,
    LEDGER_COLUMN_SHORTCUTS
)

# TODO:
# - Write functions standardize_prices, standardize_vat_codes,
#   standardize_account_chart, standardize_fx_adjustments,
#   standardize_settings analogues to LedgerEnging.standardize_ledger.
#   Decide whether functions should be available at the parent class
#   LedgerEnding or at the derived class TextLEdger.
# - Make sure these functions are also used when creating empty data frames.
# - When reading prices, add id containing source file and line number,
#   similar as when reading ledger entries.


def _read_csv_or_none(path: str, *args, **kwargs) -> pd.DataFrame | None:
    if path is None:
        return None
    else:
        path = Path(path).expanduser()
        return pd.read_csv(path, *args, skipinitialspace=True, **kwargs)


def _read_json(path: str) -> dict:
    with open(Path(path).expanduser(), "r") as file:
        result = json.load(file)
    return result


class TextLedger(StandaloneLedger):
    """TextLedger class for reading and processing ledger and price data
    from text files.
    """

    def __init__(
        self,
        settings: str,
        accounts: str,
        ledger: str,
        prices: str = None,
        vat_codes: str = None,
        fx_adjustments: str = None,
    ) -> None:
        """Initialize the TextLedger with paths to settings, account chart,
        ledger, prices, VAT codes, and FX adjustments files.

        Args:
            settings (str): Path to the JSON settings file.
            accounts (str): Path to the CSV accounts file.
            ledger (str): Path to the directory containing ledger CSV files.
            prices (str, optional): Path to the directory containing price CSV files.
            vat_codes (str, optional): Path to the CSV VAT codes file.
            fx_adjustments (str, optional): Path to the CSV FX adjustments file.
        """
        self._logger = logging.getLogger("ledger")
        super().__init__(
            settings=_read_json(settings),
            accounts=_read_csv_or_none(accounts),
            ledger=self.read_ledger(ledger),
            prices=self.read_prices(prices),
            vat_codes=_read_csv_or_none(vat_codes),
            fx_adjustments=_read_csv_or_none(fx_adjustments),
        )

    def read_prices(self, path: str = None) -> pd.DataFrame:
        """Reads all price files in the directory and returns combined data as a
        single DataFrame. The return value is an empty DataFrame with the
        correct structure if no files are found or all are skipped due to
        errors.

        Args:
            path (str, optional): Path to the directory containing price CSV files.

        Returns:
            pd.DataFrame: Combined price data as a DataFrame with columns 'ticker',
            'date', 'currency', 'price'.
        """
        df_list = []
        if path is not None:
            directory = Path(path).expanduser()
            for file in directory.rglob("*.csv"):
                try:
                    short_file = file.relative_to(directory)
                    df = pd.read_csv(
                        file,
                        comment="#",
                        skipinitialspace=True,
                        skip_blank_lines=True,
                    )
                    df = self.standardize_price_df(df)
                    df_list.append(df)
                except Exception as e:
                    self._logger.warning(f"Skip {short_file}: {e}")
        if len(df_list) > 0:
            result = pd.concat(df_list, ignore_index=True)
        else:
            # Empty DataFrame with identical structure
            result = pd.DataFrame(columns=self.REQUIRED_PRICE_COLUMNS)
        return result

    def read_ledger(self, path: str) -> pd.DataFrame:
        """Reads all ledger files in the directory and returns combined data.
        Returns an empty DataFrame with the correct structure if no files
        are found or all are skipped due to errors.

        Args:
            path (str): Path to the directory containing ledger CSV files.

        Returns:
            pd.DataFrame: Combined ledger data as a DataFrame with columns 'id',
            'date', 'text', 'document', 'account', 'counter_account', 'currency',
            'amount', 'base_currency_amount', 'vat_code'.
        """
        directory = Path(path).expanduser()
        df_list = []

        for file in directory.rglob("*.csv"):
            try:
                short_file = file.relative_to(directory)
                df = self.read_ledger_file(file, id=short_file)
                df_list.append(df)
            except Exception as e:
                self._logger.warning(f"Skip {short_file}: {e}")
        if len(df_list) > 0:
            result = pd.concat(df_list, ignore_index=True)
        else:
            # Empty DataFrame with identical structure
            cols = REQUIRED_LEDGER_COLUMNS + OPTIONAL_LEDGER_COLUMNS
            result = self.standardize_ledger(pd.DataFrame(columns=cols))

        return result

    @classmethod
    def read_ledger_file(cls, path: Path, id: str = "") -> pd.DataFrame:
        """Reads a single ledger file and standardizes its format.
        Adds an 'id' column representing file path and line number.
        Converts short column names to standard names.

        Args:
            path (Path): Path to the ledger CSV file.
            id (str, optional): Identifier for the file, used in the 'id' column.

        Returns:
            pd.DataFrame: Standardized DataFrame with ledger data.
        """
        df = pd.read_csv(path, skipinitialspace=True)
        has_id = "id" in df.columns
        df = cls.standardize_ledger_columns(df)

        # Add 'id' column representing file path and line number.
        # Rows without a date form a collective posting with preceding rows up
        # to the last row with non-missing date. Rows belonging to a collective
        # posting share the same id.
        if not has_id:
            digits = len(str(df.shape[0]))
            df["id"] = [
                None if pd.isna(df.at[i, "date"]) else f"{id}:{i:0{digits}d}"
                for i in range(len(df))
            ]
            df["id"] = df["id"].ffill()
            df["date"] = df["date"].ffill()

        return df

    @classmethod
    def write_ledger_file(
        cls,
        df: pd.DataFrame,
        path: str,
        short_names: bool = False,
        drop_unused_columns: bool = False,
        digits: int | None = None,
    ) -> None:
        """Writes a ledger DataFrame to a CSV file.
        CSV files are formatted with fixed column width and standard column
        order to improve human readability and traceability with a version
        control system such as git.

        Args:
            df (pd.DataFrame): DataFrame containing the ledger data to be written.
            path (str): Path where the CSV file will be saved.
            short_names (bool, optional): Flag to use shortened column names.
            drop_unused_columns (bool, optional): Flag to drop DataFrame columns
            that solely contain NA values.
            digits (int | None, optional): Number of digits to print for floating
            point values.

        Raises:
            ValueError: If required columns are missing.
        """
        missing = set(REQUIRED_LEDGER_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if "id" in df.columns:
            # Drop id: Ensure elements with the same 'id' immediately follow
            # each other, then set 'date' to None in rows with duplicated 'id'.
            unique_id = pd.DataFrame({"id": df["id"].unique()})
            unique_id["__sequence__"] = range(len(unique_id))
            df = df.merge(unique_id, on="id", how="left")
            df = df.sort_values(by="__sequence__")
            df["date"] = df["date"].where(~df["__sequence__"].duplicated(), None)
            if df.loc[~df["__sequence__"].duplicated(), "date"].isna().any():
                raise ValueError(
                    "A valid 'date' is required in the first occurrence of every 'id'."
                )
            df = df.drop(columns=["id", "__sequence__"])

        if drop_unused_columns:
            all_na = df.columns[df.isna().all()]
            df = df.drop(
                columns=set(all_na) - set(REQUIRED_LEDGER_COLUMNS)
            )

        # Default column order
        cols = (
            [col for col in LEDGER_COLUMN_SEQUENCE if col in df.columns]
            + [col for col in df.columns if col not in LEDGER_COLUMN_SEQUENCE]
        )
        df = df[cols]

        fixed_width_cols = [
            col
            for col in LEDGER_COLUMN_SEQUENCE
            if (col in df.columns) and not (col in ["text", "document"])
        ]

        if digits is not None:
            float_cols = df.select_dtypes(include=["float"]).columns
            df[float_cols] = df[float_cols].map(
                lambda x: f"{x:.{digits}f}" if pd.notna(x) else None
            )
            df[float_cols] = df[float_cols].astype(pd.StringDtype())

        if short_names:
            reverse_shortcuts = {
                v: k for k, v in LEDGER_COLUMN_SHORTCUTS.items()
            }
            df = df.rename(columns=reverse_shortcuts)

        file = Path(path).expanduser()
        file.parent.mkdir(parents=True, exist_ok=True)
        write_fixed_width_csv(df, path=file, n=len(fixed_width_cols))

    def add_ledger_entry(self, data: dict) -> None:
        raise NotImplementedError(
            "add_ledger_entry is not implemented for TextLedger."
        )
