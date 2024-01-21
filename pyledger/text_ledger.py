import datetime, json, numpy as np, pandas as pd
from pathlib import Path
from .ledger_engine import LedgerEngine
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

class TextLedger(LedgerEngine):

    _settings = None
    _account_chart = None
    _vat_codes = None
    _fx_adjustments = None
    _ledger = None
    _serialized_ledger = None
    _prices = None

    def __init__(self, settings: str, accounts: str, ledger: str,
                 prices: str = None, vat_codes: str =  None,
                 fx_adjustments: str = None):
        super().__init__()
        self._account_files = accounts
        self._ledger_files = ledger
        self._vat_code_files = vat_codes
        self._price_files = prices
        self._settings_file = settings
        self._fx_adjustment_files = fx_adjustments
        self.init_and_validate_settings()

    def init_and_validate_settings(self):
        """
        Read Settings and check for coherence.
        """
        # Read settings
        self._settings = self.read_settings(self._settings_file)
        if self._vat_code_files is None:
            # Empty DataFrame with identical structure
            self._vat_codes = pd.DataFrame(
                columns=self.REQUIRED_VAT_CODE_COLUMNS +
                        self.OPTIONAL_VAT_CODE_COLUMNS)
        else:
            self._vat_codes = self.read_vat_codes(self._vat_code_files)
        if self._fx_adjustment_files is None:
            # Empty DataFrame with identical structure
            self._fx_adjustments = pd.DataFrame(
                columns=self.REQUIRED_FX_ADJUSTMENT_COLUMNS)
        else:
            self._fx_adjustments = self.read_fx_adjustments(
                self._fx_adjustment_files)
        self._account_chart = self.read_account_chart(self._account_files)

        # # Sample data
        # df = pd.DataFrame({
        #     "ticker": ["CAD", "GBP", "HKD", "EUR", "USD", "GBP", "CAD", "HKD", "EUR", "USD"],
        #     "date": ["2022-12-30", "2022-12-30", "2022-12-30", "2022-12-30", "2022-12-30",
        #             "2023-12-20", "2023-12-20", "2023-12-20", "2023-12-20", "2023-12-20"],
        #     "currency": "CHF",
        #     "price": [0.68293, 1.11777, 0.11850, 0.98984, 0.92463, 1.09037, 0.64529, 0.11050, 0.94404, 0.86277]
        # })

        # Store prices in nested dict: each self._prices[ticker][currency] is
        # a DataFrame with columns 'date' and 'price' that is sorted by 'date'
        df = self.read_all_price_files()
        self._prices = {}
        for (ticker, currency), group in df.groupby(['ticker', 'currency']):
            group = group[['date', 'price']].sort_values('date')
            group = group.reset_index(drop=True)
            if not ticker in self._prices.keys():
                self._prices[ticker] = {}
            self._prices[ticker][currency] = group

        # Ensure all vat code accounts are defined in account chart
        vat_codes = set(self._vat_codes.index)
        missing = set(self._account_chart['vat_code'].dropna()) - vat_codes
        if len(missing) > 0:
            raise ValueError(f"Some VAT codes in account chart not defined: "
                             f"{missing}.")

        # Ensure all account vat_codes are defined in vat_codes
        accounts = set(self._account_chart.index)
        missing = set(self._vat_codes['account'].dropna()) - accounts
        if len(missing) > 0:
            raise ValueError(f"Some accounts in VAT code definitions are not "
                             f"defined in the account chart: {missing}.")

        # Ensure all credit and debit accounts in fx_adjustments are defined
        # in account chart
        df = self.fx_adjustments()
        missing = (set(df['credit']) | set(df['debit'])) - accounts
        if len(missing) > 0:
            raise ValueError(f"Some accounts in FX adjustment definitions are "
                             f"not defined in the account chart: {missing}.")

    @classmethod
    def read_settings(self, path: str) -> dict:
        """
        Reads settings from a JSON file.
        Ensures 'base_currency' and 'smallest_price_increment' are present.

        Write example settings:
        settings = {
            'base_currency': 'CHF',
            'precision': {
                'CHF': 0.01, 'EUR': 0.01, 'USD': 0.01,
                'CAD': 0.01, 'GBP': 0.01, 'HKD': 0.01}
        }
        with open('/Users/lukas/macx/accounts/settings.json', 'w') as f:
            json.dump(settings, f, indent=4)

        :param path: Path to the settings JSON file.
        :return: dict with the settings.
        :raises ValueError: If required keys are missing or have incorrect types.
        """
        with open(Path(path).expanduser(), 'r') as file:
            settings = json.load(file)

        # Check for 'base_currency'
        if ('base_currency' not in settings or
                not isinstance(settings['base_currency'], str)):
            raise ValueError("Missing/invalid 'base_currency' in settings.")

        # Check for 'precision'
        if ('precision' not in settings or
                not isinstance(settings['precision'], dict)):
            raise ValueError("Missing/invalid 'precision'.")

        # Validate contents of 'precision'
        for key, value in settings['precision'].items():
            if not isinstance(key, str) or not isinstance(value, (float, int)):
                raise ValueError("Invalid types in 'precision'.")

        settings['precision']['base_currency'] = (
            settings['precision'][settings['base_currency']])

        return settings

    @classmethod
    def read_account_chart(cls, path: str) -> pd.DataFrame:
        """
        Reads an account chart from a CSV file.
        Raises an exception if required columns are not present.
        :param path: Path to the account chart CSV file.
        :return: DataFrame with the account chart.
        """
        df = pd.read_csv(path, skipinitialspace=True)

        # Ensure required columns and values are present
        missing = set(cls.REQUIRED_ACCOUNT_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns missing in account chart: "
                             f"{missing}.")
        if df['account'].isna().any():
            raise ValueError("Missing 'account' values in account chart.")

        # Enforce data types
        def to_str_or_na(value):
            return str(value) if pd.notna(value) else value
        df['account'] = df['account'].astype(int)
        df['currency'] = df['currency'].apply(to_str_or_na)
        df['text'] = df['text'].apply(to_str_or_na)
        df['vat_code'] = df['vat_code'].apply(to_str_or_na)

        df.set_index('account', inplace=True)
        return df

    @classmethod
    def read_vat_codes(cls, path: str) -> pd.DataFrame:
        """
        Reads tax codes from a CSV file.
        Raises an exception if required columns are not present.
        :param path: Path to the tax codes CSV file.
        :return: DataFrame with the tax codes.
        """
        df = pd.read_csv(path, skipinitialspace=True)

        # Check for missing required columns
        missing = set(cls.REQUIRED_VAT_CODE_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} "
                             f"are missing in {path}.")

        # Add missing columns
        for col in cls.OPTIONAL_VAT_CODE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Enforce data types
        inception = pd.Timestamp('0000-01-01')
        df['id'] = df['id'].astype(pd.StringDtype())
        df['text'] = df['text'].astype(pd.StringDtype())
        df['inclusive'] = df['inclusive'].astype(bool)
        df['account'] = df['account'].astype(pd.Int64Dtype())
        df['date'] = pd.to_datetime(df['date']).fillna(inception).dt.date
        df['rate'] = df['rate'].astype(float)

        # Ensure account is defined if rate is other than zero
        missing = list(df['id'][(df['rate'] != 0) & df['account'].isna()])
        if len(missing) > 0:
            raise ValueError(f"Account must be defined for non-zero rate in "
                             f"vat_codes: {missing}.")

        return df.set_index('id')

    @classmethod
    def read_fx_adjustments(cls, path: str) -> pd.DataFrame:
        """
        Reads definitions for foreign exchange adjustment from a CSV file.
        Raises an exception if required columns are not present.
        :param path: Path to the FX adjustments CSV file.
        :return: DataFrame with the FX adjustments.
        """
        df = pd.read_csv(path, skipinitialspace=True)

        # Check for missing required columns
        missing = set(cls.REQUIRED_FX_ADJUSTMENT_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} are "
                             f"missing in {path}.")

        # Enforce data types
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['adjust'] = df['adjust'].astype(pd.StringDtype())
        df['credit'] = df['credit'].astype(pd.Int64Dtype())
        df['debit'] = df['debit'].astype(pd.Int64Dtype())
        df['text'] = df['text'].astype(pd.StringDtype())

        return df.sort_values('date')

    @classmethod
    def read_price_file(cls, path: str) -> pd.DataFrame:
        """
        Reads a price list from a CSV file.
        Ensures required columns are present and do not contain missing values.
        :param path: Path to the prices CSV file.
        :return: DataFrame with the prices.
        """
        df = pd.read_csv(path, comment="#", skipinitialspace=True,
                         skip_blank_lines=True)

        # Check for missing required columns
        missing = set(cls.REQUIRED_PRICE_COLUMNS) - set(df.columns)
        if len(missing) > 0:
            raise ValueError(f"Required columns {missing} missing.")

        # Check for missing values in required columns
        has_missing_value = [column for column in cls.REQUIRED_PRICE_COLUMNS
                             if df[column].isnull().any()]
        if len(has_missing_value) > 0:
            raise ValueError(f"Missing values in column {has_missing_value}.")

        # Enforce data types
        df['ticker'] = df['ticker'].astype(str)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['currency'] = df['currency'].astype(str)
        df['price'] = df['price'].astype(float)

        return df

    def read_all_price_files(self) -> pd.DataFrame:
        """
        Reads all price files in the directory and returns combined data as a
        single DataFrame. The return value is an empty DataFrame with the
        correct structure if no files are found or all are skipped due to
        errors.

        :return: Combined price data as a DataFrame with columns 'ticker',
            'date', 'currency', 'price'.
        """
        directory = Path(self._price_files).expanduser()
        df_list = []

        for path in directory.rglob("*.csv"):
            try:
                file = path.relative_to(directory)
                df = self.read_price_file(path)
                df_list.append(df)
            except Exception as e:
                self._logger.warning(f"Skip {file}: {e}")
        if len(df_list) > 0:
            result = pd.concat(df_list, ignore_index=True)
        else:
            # Empty DataFrame with identical structure
            result = pd.DataFrame(columns=self.REQUIRED_PRICE_COLUMNS)
        return result

    def ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame with all ledger transactions.
        :return: Combined DataFrame with ledger data.
        """
        if self._ledger is None:
            self._ledger = self.read_all_ledger_files()
        return self._ledger

    def serialized_ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame with all ledger transactions in long format.
        :return: Combined DataFrame with ledger data.
        """
        if self._serialized_ledger is None:
            self.read_ledger()
        return self._serialized_ledger

    def _base_currency_amount(self, amount, currency, date):
        base_currency = self.base_currency
        if not (len(amount) == len(currency) == len(date)):
            raise ValueError("Vectors 'amount', 'currency', and 'date' must "
                             "have the same length.")
        result = [
            self.round_to_precision(
                a * self.price(t, date=d, currency=base_currency)[1],
                base_currency, date=d)
            for a, t, d in zip(amount, currency, date)]
        return result


    def vat_journal_entries(self, df):
        """
        Create journal entries to book VAT according to vat_codes.

        Iterates through the provided DataFrame and calculates VAT for entries
        that have a non-null 'vat_code'. It generates a new journal entry for
        each VAT account.

        Parameters:
        - df (DataFrame): A pandas DataFrame containing ledger entries.

        Returns:
        - DataFrame: A new DataFrame with VAT journal entries.
        Returns empty DataFrame with the correct structure if no VAT codes are
        present.
        """
        vat_definitions = self.vat_codes().to_dict('index')
        vat_journal_entries = []
        account_chart = self.account_chart()
        for _, row in df.loc[df['vat_code'].notna()].iterrows():
            vat = vat_definitions[row['vat_code']]
            account_vat_code = (account_chart['vat_code'][row['account']]
                                if pd.notna(row['account']) else None)
            counter_vat_code = (account_chart['vat_code'][row['counter_account']]
                                if pd.notna(row['counter_account']) else None)
            if pd.isna(account_vat_code) and pd.isna(counter_vat_code):
                self._logger.warning(f"Skip vat code '{row['vat_code']}' "
                                     f"for {row['id']}: Neither account nor "
                                     f"counter account have a vat_code.")
            elif pd.isna(account_vat_code) and pd.notna(counter_vat_code):
                multiplier = 1.0
                if vat['inclusive']:
                    account = row['counter_account']
                else:
                    account = row['account']
            elif pd.notna(account_vat_code) and pd.isna(counter_vat_code):
                multiplier = -1.0
                if vat['inclusive']:
                    account = row['account']
                else:
                    account = row['counter_account']
            else:
                self._logger.warning(f"Skip vat code '{row['vat_code']}' "
                                     f"for {row['id']}: Both account and "
                                     f"counter accounts have vat_codes.")

            # Calculate VAT amount
            if vat['inclusive']:
                amount = multiplier * row['amount'] * vat['rate'] / (1 + vat['rate'])
            else:
                amount = row['amount'] * vat['rate']
            amount = amount * multiplier
            amount = self.round_to_precision(amount, row['currency'])

            # Create a new journal entry for the VAT amount
            if amount != 0:
                base_entry = {
                        'date': row['date'],
                        'text': "VAT: " + row['text'],
                        'account': account,
                        'document': row['document'],
                        'currency': row['currency'],
                        'base_currency_amount': np.nan,
                        'vat_code': row['vat_code']
                    }
                if pd.notna(vat['account']):
                    vat_journal_entries.append(base_entry | {
                        'id': f"{row['id']}:vat",
                        'counter_account': vat['account'],
                        'amount': amount
                    })
                if pd.notna(vat['inverse_account']):
                    vat_journal_entries.append(base_entry | {
                        'id': f"{row['id']}:vat",
                        'counter_account': vat['inverse_account'],
                        'amount': -1 * amount
                    })

        # Return a DataFrame
        if len(vat_journal_entries) > 0:
            result = pd.DataFrame(vat_journal_entries)
        else:
            # Empty DataFrame with identical structure
            cols = self.REQUIRED_LEDGER_COLUMNS + self.OPTIONAL_LEDGER_COLUMNS
            result = pd.DataFrame(columns=cols)
        return result

    def read_ledger(self) -> pd.DataFrame:
        # Ledger definition
        df = self.read_all_ledger_files()
        df = self.sanitize_ledger(df)
        df = df.sort_values(['date', 'id'])
        self._ledger = df.copy()

        # Calculate amount to match target balance
        # TODO: drop target_balance
        if 'target_balance' in df.columns:
            new_amount = []
            for i in np.where(df['target_balance'].notna())[0]:
                date = df['date'].iloc[i]
                account = df['account'].iloc[i]
                currency = self.account_currency(account)
                self._serialized_ledger = self.serialize_ledger(
                    df.loc[df['date'] <= date, :])
                balance = self.account_balance(account=account, date=date)
                balance = balance[currency]
                amount = df['target_balance'].iloc[i] - balance
                amount = self.round_to_precision(amount, ticker=currency,
                                                 date=date)
                new_amount.append(amount)
                df.loc[range(df.shape[0]) == i, "amount"] = amount

        # Add automated journal entries for VAT
        vat = self.vat_journal_entries(df)
        if vat.shape[0] > 0:
            df = pd.concat([df, vat])

        # Insert missing base currency amounts
        index = df['base_currency_amount'].isna()
        df.loc[index, 'base_currency_amount'] = self._base_currency_amount(
            amount = df.loc[index, 'amount'],
            currency = df.loc[index, 'currency'],
            date = df.loc[index, 'date']
        )

        # FX adjustments
        adjustment = self.fx_adjustments()
        base_currency = self.base_currency
        for row in adjustment.to_dict('records'):
            self._serialized_ledger = self.serialize_ledger(df)
            date = row['date']
            accounts = self.account_range(row['adjust'])
            accounts = set(accounts['add']) - set(accounts['subtract'])
            adjustments = []
            for account in accounts:
                currency = self.account_currency(account)
                if currency != base_currency:
                    balance = self.account_balance(account, date=date)
                    fx_rate = self.price(currency, date=date,
                                         currency=base_currency)
                    assert fx_rate[0] == base_currency
                    target = balance[currency] * fx_rate[1]
                    amount = target - balance['base_currency']
                    amount = self.round_to_precision(amount, ticker=base_currency,
                                                    date=date)
                    id = f"fx_adjustment:{date}:{account}"
                    adjustments.append({
                        'id': id,
                        'date': date,
                        'account': account,
                        'currency': currency,
                        'amount': 0,
                        'base_currency_amount': amount,
                        'text': row['text']
                    })
                    adjustments.append({
                        'id': id,
                        'date': date,
                        'account': row['credit'] if amount > 0 else row['debit'],
                        'currency': base_currency,
                        'amount': -1 * amount,
                        'base_currency_amount': -1 * amount,
                        'text': row['text']
                    })
            if len(adjustments) > 0:
                adjustments = self.standardize_ledger(pd.DataFrame(adjustments))
                df = pd.concat([df, adjustments])

        # Serializes ledger with separate credit and debit entries.
        result = self.serialize_ledger(df)
        self._serialized_ledger = self.standardize_ledger(result)

    def read_all_ledger_files(self) -> pd.DataFrame:
        """
        Reads all ledger files in the directory and returns combined data.
        Returns an empty DataFrame with the correct structure if no files
        are found or all are skipped due to errors.

        :return: Combined ledger data as a DataFrame with columns 'id', 'date',
            'text', 'document', 'account', 'counter_account', 'currency',
            'amount', 'base_currency_amount', 'vat_code'.
        """
        directory = Path(self._ledger_files).expanduser()
        df_list = []

        for path in directory.rglob("*.csv"):
            try:
                file = path.relative_to(directory)
                df = self.read_ledger_file(path, id=file)
                df_list.append(df)
            except Exception as e:
                self._logger.warning(f"Skip {file}: {e}")
        if len(df_list) > 0:
            result = pd.concat(df_list, ignore_index=True)
        else:
            # Empty DataFrame with identical structure
            cols = self.REQUIRED_LEDGER_COLUMNS + self.OPTIONAL_LEDGER_COLUMNS
            result = self.standardize_ledger(pd.DataFrame(columns=cols))

        return result

    def read_ledger_file(self, path: Path, id: str = '') -> pd.DataFrame:
        """
        Reads a single ledger file and standardizes its format.
        Adds an 'id' column representing file path and line number.
        Converts short column names to standard names.
        :param path: Path to the ledger CSV file.
        :return: Standardized DataFrame with ledger data.
        """
        df = pd.read_csv(path, skipinitialspace=True)
        has_id = 'id' in df.columns
        df = self.standardize_ledger(df)

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
            # Drop id: Ensure rows with the same 'id' follow each other,
            # set 'date' to None in subsequent rows with the same 'id'.
            df = df.sort_values(by='id')
            df['date'] = df['date'].where(~df['id'].duplicated(), None)
            if df.loc[~df['id'].duplicated(), 'date'].isna().any():
                raise ValueError("A valid 'date' is required in the first "
                                 "occurrence of every 'id'.")
            df = df.drop(columns='id')

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

    def price(self, ticker: str, date: datetime.date = None,
              currency: str | None = None) -> (str, float):
        """
        Retrieve price for a given ticker as of a specified date. If no price
        is available on the exact date, return latest price observation prior
        to the specified date.

        Parameters:
            ticker (str): Asset identifier.
            date (datetime.date): Date for which the price is required.
            currency (str, optional): Currency in which the price is desired.

        Return:
            tuple: (currency, price) where 'currency' is a string indicating
                   the currency of the price, and 'price' is a float
                   representing the asset's price as of the specified date.
        """
        if (not currency is None) and (str(ticker) == str(currency)):
            return (currency, 1.0)

        if date is None:
            date = datetime.date.today()
        elif not isinstance(date, datetime.date):
            date = pd.to_datetime(date).date()

        if ticker not in self._prices:
            raise ValueError(f"No price data available for '{ticker}'.")

        if currency is None:
            # Assuming the first currency is the default if none specified
            currency = next(iter(self._prices[ticker]))

        if currency not in self._prices[ticker]:
            raise ValueError(f"No {currency} prices available for '{ticker}'.")

        prc = self._prices[ticker][currency]
        prc = prc.loc[prc['date'] <= date, 'price']

        if prc.empty:
            raise ValueError(f"No {currency} prices available for '{ticker}' "
                             f"before {date}.")

        return (currency, prc.iloc[-1].item())

    def _single_account_balance(self, account: int,
                                date: datetime.date = None) -> dict:
        return self._balance_from_serialized_ledger(account=account, date=date)

    @property
    def base_currency(self):
        return self._settings['base_currency']

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        return self._settings['precision'][ticker]

    def account_chart(self):
        return self._account_chart

    def vat_codes(self):
        return self._vat_codes

    def vat_rate(self, vat_code: str) -> float:
        """
        Retrieve the VAT rate for a given VAT code.

        Parameters:
        vat_code (str): VAT code to look up.

        Returns:
        float: VAT rate associated with the specified code.

        Raises:
        KeyError: If the VAT code is not defined.
        """
        if vat_code not in self._vat_codes.index:
            raise KeyError(f"VAT code not defined: {vat_code}")
        return self._vat_codes['rate'][vat_code]

    def vat_accounts(self, vat_code: str) -> list[int]:
        """
        Retrieve the accounts associated with a given VAT code.

        Parameters:
        vat_code (str): VAT code to look up.

        Returns:
        list[int]: List of accounts associated with the specified VAT code.

        Raises:
        KeyError: If the VAT code is not defined.
        """
        if vat_code not in self._vat_codes.index:
            raise KeyError(f"VAT code not defined: {vat_code}")
        return self._vat_codes['accounts'][vat_code]

    def fx_adjustments(self):
        return self._fx_adjustments

    def add_account(self, *args, **kwargs):
        raise NotImplementedError("add_account is not implemented yet.")

    def add_ledger_entry(self, *args, **kwargs):
        raise NotImplementedError("add_ledger_entry is not implemented yet.")

    def add_price(self, *args, **kwargs):
        raise NotImplementedError("add_price is not implemented yet.")

    def add_vat_code(self, *args, **kwargs):
        raise NotImplementedError("add_vat_code is not implemented yet.")

    def delete_account(self, *args, **kwargs):
        raise NotImplementedError("delete_account is not implemented yet.")

    def delete_ledger_entry(self, *args, **kwargs):
        raise NotImplementedError("delete_ledger_entry is not implemented yet.")

    def delete_price(self, *args, **kwargs):
        raise NotImplementedError("delete_price is not implemented yet.")

    def delete_vat_code(self, *args, **kwargs):
        raise NotImplementedError("delete_vat_code is not implemented yet.")

    def ledger_entry(self, *args, **kwargs):
        raise NotImplementedError("ledger_entry is not implemented yet.")

    def modify_account(self, *args, **kwargs):
        raise NotImplementedError("modify_account is not implemented yet.")

    def modify_ledger_entry(self, *args, **kwargs):
        raise NotImplementedError("modify_ledger_entry is not implemented yet.")

    def price_history(self, *args, **kwargs):
        raise NotImplementedError("price_history is not implemented yet.")

    def price_increment(self, *args, **kwargs):
        raise NotImplementedError("price_increment is not implemented yet.")

