import logging
import json, pandas as pd
from pathlib import Path
from .standalone_ledger import StandaloneLedger
from .helpers import write_fixed_width_csv

# TODO:
# - write functions standardize_prices, standardize_vat_codes,
#   standardize_account_chart, standardize_fx_adjustments,
#   standardize_settings analogues to LedgerEnging.standardize_ledger.
#   Decide whether functions should be available at the parent class
#   LedgerEnding or at the derived class TextLEdger
# - Make sure these functions are also used when creating empty data frames.
# - When reading prices, add id containing source file and line number,
#   similar as when reading ledger entries

def _read_csv_or_none(path, *args, **kwargs):
    if path is None:
        return None
    else:
        path = Path(path).expanduser()
        return pd.read_csv(path, *args, skipinitialspace=True, *kwargs)

def _read_json(path):
    with open(Path(path).expanduser(), 'r') as file:
        result = json.load(file)
    return result


class TextLedger(StandaloneLedger):

    def __init__(self, settings: str, accounts: str, ledger: str,
                 prices: str = None, vat_codes: str =  None,
                 fx_adjustments: str = None):
        self._logger = logging.getLogger('ledger')
        super().__init__(
            settings=_read_json(settings),
            accounts=_read_csv_or_none(accounts),
            ledger=self.read_ledger(ledger),
            prices=self.read_prices(prices),
            vat_codes=_read_csv_or_none(vat_codes),
            fx_adjustments=_read_csv_or_none(fx_adjustments))

    def read_prices(self, path: str = None) -> pd.DataFrame:
        """
        Reads all price files in the directory and returns combined data as a
        single DataFrame. The return value is an empty DataFrame with the
        correct structure if no files are found or all are skipped due to
        errors.

        :return: Combined price data as a DataFrame with columns 'ticker',
            'date', 'currency', 'price'.
        """
        df_list = []
        if path is not None:
            directory = Path(path).expanduser()
            for file in directory.rglob("*.csv"):
                try:
                    short_file = file.relative_to(directory)
                    df = pd.read_csv(file, comment="#", skipinitialspace=True,
                                     skip_blank_lines=True)
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

    def read_ledger(self, path) -> pd.DataFrame:
        """
        Reads all ledger files in the directory and returns combined data.
        Returns an empty DataFrame with the correct structure if no files
        are found or all are skipped due to errors.

        :return: Combined ledger data as a DataFrame with columns 'id', 'date',
            'text', 'document', 'account', 'counter_account', 'currency',
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
            cols = self.REQUIRED_LEDGER_COLUMNS + self.OPTIONAL_LEDGER_COLUMNS
            result = self.standardize_ledger(pd.DataFrame(columns=cols))

        return result

    @classmethod
    def read_ledger_file(cls, path: Path, id: str = '') -> pd.DataFrame:
        """
        Reads a single ledger file and standardizes its format.
        Adds an 'id' column representing file path and line number.
        Converts short column names to standard names.
        :param path: Path to the ledger CSV file.
        :return: Standardized DataFrame with ledger data.
        """
        df = pd.read_csv(path, skipinitialspace=True)
        has_id = 'id' in df.columns
        df = cls.standardize_ledger(df)

        # Add 'id' column representing file path and line number.
        # Rows without a date form a collective posting with preceding rows up
        # to the last row with non-missing date. Rows belonging to a collective
        # posting share the same id.
        if not has_id:
            digits = len(str(df.shape[0]))
            df['id'] = [
                None if pd.isna(df.at[i, 'date']) else f"{id}:{i:0{digits}d}"
                for i in range(len(df))
            ]
            df['id'] = df['id'].ffill()
            df['date'] = df['date'].ffill()

        return df

    @classmethod
    def write_ledger_file(cls, df: pd.DataFrame, path: str,
                          short_names: bool = False,
                          drop_unused_columns: bool = False,
                          digits=None) -> None:
        """
        Writes a ledger DataFrame to a CSV file
        CSV files are formatted with fixed column width and standard column
        order to improve human readability and traceability with a version
        control system such as git.

        :param df: DataFrame containing the ledger data to be written.
        :param path: Path where the CSV file will be saved.
        :param short_names: Boolean flag to use shortened column names.
        :param digits (int | None): Number of digits to print for floating
            point values.
        :param drop_unused_columns: Boolean flag to use drop data frame columns
            that solely contain NA values.
        """
        missing = set(cls.REQUIRED_LEDGER_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if 'id' in df.columns:
            # Drop id: Ensure elements with the same 'id' immediately follow
            # each other, then set 'date' to None in rows with duplicated 'id'.
            unique_id = pd.DataFrame({'id': df['id'].unique()})
            unique_id['__sequence__'] = range(len(unique_id))
            df = df.merge(unique_id, on='id', how='left')
            df = df.sort_values(by='__sequence__')
            df['date'] = df['date'].where(~df['__sequence__'].duplicated(), None)
            if df.loc[~df['__sequence__'].duplicated(), 'date'].isna().any():
                raise ValueError("A valid 'date' is required in the first "
                                 "occurrence of every 'id'.")
            df = df.drop(columns=['id', '__sequence__'])

        if drop_unused_columns:
            all_na = df.columns[df.isna().all()]
            df = df.drop(
                columns=set(all_na) - set(cls.REQUIRED_LEDGER_COLUMNS))

        # Default column order
        cols = (
            [col for col in cls.LEDGER_COLUMN_SEQUENCE if col in df.columns] +
            [col for col in df.columns if col not in cls.LEDGER_COLUMN_SEQUENCE])
        df = df[cols]

        fixed_width_cols = [
            col for col in cls.LEDGER_COLUMN_SEQUENCE
            if (col in df.columns) and not (col in ['text', 'document'])]

        if digits is not None:
            float_cols = df.select_dtypes(include=['float']).columns
            df[float_cols] = df[float_cols].map(
                lambda x: f"{x:.{digits}f}" if pd.notna(x) else None)
            df[float_cols] = df[float_cols].astype(pd.StringDtype())

        if short_names:
            reverse_shortcuts = {v: k for k, v in
                                 cls.LEDGER_COLUMN_SHORTCUTS.items()}
            df = df.rename(columns=reverse_shortcuts)

        file = Path(path).expanduser()
        file.parent.mkdir(parents=True, exist_ok=True)
        write_fixed_width_csv(df, path=file, n = len(fixed_width_cols))

    def add_ledger_entry(self, data):
        raise NotImplementedError("add_ledger_entry is not implemented for TextLedger.")
