import datetime
import collections
from warnings import warn
import numpy as np
import pandas as pd
from .ledger_engine import LedgerEngine

class StandaloneLedger(LedgerEngine):
    """
    StandaloneLedger is a self-contained implementation of the LedgerEngine
    class, that provides an abstract interface for a double entry accounting
    system. StandaloneLedger operates autonomously and does not connect to
    third party accounting software. It handles accounting data primarily
    in pandas DataFrames and provides methods to enforce type consistency
    for these DataFrames. This class serves as a base for any standalone
    ledger implementation with a specific data storage choice.

    Attributes:
        settings (dict): Accounting settings, such as beginning and end of the
            accounting period, rounding precision for currencies, etc.
        accounts (pd.DataFrame): Account chart.
        ledger (pd.DataFrame): General ledger data in original form entered
            into the accounting system, without automated enhancements such as
            base currency amounts, FX adjustments or VAT bookings.
        serialized_ledger (pd.DataFrame): Ledger in completed form, after
            automated enhancements, including base currency amounts,
            FX adjustments or VAT bookings. Data is returned in long format,
            detailing accounts and counter-accounts in separate rows.
        prices (pd.DataFrame, optional): Prices data for foreign currencies,
            securities, commodities, inventory, etc.
        vat_codes (pd.DataFrame, optional): VAT definitions.
        fx_adjustments (pd.DataFrame, optional): Definitions for automated
            calculation of FX adjustments.
    """

    _settings = None
    _account_chart = None
    _ledger = None
    _serialized_ledger = None
    _prices = None
    _vat_codes = None
    _fx_adjustments = None

    def __init__(self, settings: dict,
                 accounts: pd.DataFrame,
                 ledger: pd.DataFrame = None,
                 prices: pd.DataFrame = None,
                 vat_codes: pd.DataFrame = None,
                 fx_adjustments: pd.DataFrame = None):
        """
        Initialize the StandaloneLedger with provided accounting data and settings.

        Args:
            data (pd.DataFrame): The accounting data to be managed by the ledger.
            settings (dict): Configuration settings for the ledger operations.
        """
        super().__init__()
        self._settings = self.standardize_settings(settings)
        self._account_chart = self.standardize_account_chart(accounts)
        self._ledger = self.standardize_ledger_columns(ledger)
        self._prices = self.standardize_prices(prices)
        self._vat_codes = self.standardize_vat_codes(vat_codes)
        self._fx_adjustments = self.standardize_fx_adjustments(fx_adjustments)
        self.validate_accounts()

    @staticmethod
    def standardize_settings(settings: dict) -> dict:
        """
        Validates and standardizes the 'settings' dictionary. Ensures it
        contains items 'base_currency' and 'precision'.

        Example:
            settings = {
                'base_currency': 'USD',
                'precision': {
                    'CAD': 0.01, 'CHF': 0.01, 'EUR': 0.01,
                    'GBP': 0.01, 'HKD': 0.01, 'USD': 0.01
                }
            }
            StandaloneLedger.standardize_settings(settings)

        Args:
            settings (dict): The settings dictionary to be standardized.

        Returns:
            dict: The standardized settings dictionary.

        Raises:
            ValueError: If 'settings' is not a dictionary, or if 'base_currency'
                        is missing/not a string, or if 'precision' is missing/not a
                        dictionary, or if any key/value in 'precision' is of invalid
                        type.

        Note:
            Modifies 'precision' to include the 'base_currency' key.
        """
        if not isinstance(settings, dict):
            raise ValueError("'settings' must be a dict.")

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
    def standardize_account_chart(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates and standardizes the 'account_chart' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the account chart.

        Returns:
            pd.DataFrame: The standardized account chart DataFrame.

        Raises:
            ValueError: If required columns are missing, or if 'account' column
                        contains NaN values, or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=cls.REQUIRED_ACCOUNT_COLUMNS)

        # Ensure required columns and values are present
        missing = set(cls.REQUIRED_ACCOUNT_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns missing in account chart: {missing}.")
        if df['account'].isna().any():
            raise ValueError("Missing 'account' values in account chart.")

        # Add missing columns
        for col in cls.OPTIONAL_ACCOUNT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Enforce data types
        def to_str_or_na(value):
            return str(value) if pd.notna(value) else value
        df['account'] = df['account'].astype(int)
        df['currency'] = df['currency'].apply(to_str_or_na)
        df['text'] = df['text'].apply(to_str_or_na)
        df['vat_code'] = (df['vat_code'].apply(to_str_or_na)).astype(pd.StringDtype())

        return df.set_index('account')

    @classmethod
    def standardize_vat_codes(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates and standardizes the 'vat_codes' DataFrame to ensure it contains
        the required columns, correct data types, and logical consistency in the data.

        Args:
            df (pd.DataFrame): The DataFrame representing the VAT codes.

        Returns:
            pd.DataFrame: The standardized VAT codes DataFrame.

        Raises:
            ValueError: If required columns are missing, if data types are incorrect,
                        or if logical inconsistencies are found (e.g., non-zero rates
                        without defined accounts).
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=cls.REQUIRED_VAT_CODE_COLUMNS)

        # Ensure required columns and values are present
        missing = set(cls.REQUIRED_VAT_CODE_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} are missing.")

        # Add missing columns
        for col in cls.OPTIONAL_VAT_CODE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Enforce data types
        inception = pd.Timestamp('1900-01-01')
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
    def standardize_fx_adjustments(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates and standardizes the 'fx_adjustments' DataFrame to ensure it contains
        the required columns and correct data types.

        Args:
            df (pd.DataFrame): The DataFrame representing the FX adjustments.

        Returns:
            pd.DataFrame: The standardized FX adjustments DataFrame.

        Raises:
            ValueError: If required columns are missing or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=cls.REQUIRED_FX_ADJUSTMENT_COLUMNS)

        # Ensure required columns and values are present
        missing = set(cls.REQUIRED_FX_ADJUSTMENT_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns {', '.join(missing)} are missing.")

        # Enforce data types
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['adjust'] = df['adjust'].astype(pd.StringDtype())
        df['credit'] = df['credit'].astype(pd.Int64Dtype())
        df['debit'] = df['debit'].astype(pd.Int64Dtype())
        df['text'] = df['text'].astype(pd.StringDtype())

        return df.sort_values('date')

    @classmethod
    def standardize_price_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates and standardizes the 'prices' DataFrame to ensure it contains
        the required columns, correct data types, and no missing values in key fields.

        Args:
            df (pd.DataFrame): The DataFrame representing the prices.

        Returns:
            pd.DataFrame: The standardized prices DataFrame.

        Raises:
            ValueError: If required columns are missing, if there are missing values
                        in required columns, or if data types are incorrect.
        """
        if df is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=cls.REQUIRED_PRICE_COLUMNS)

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

    @classmethod
    def standardize_prices(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Store prices in nested dict: Each prices[ticker][currency] is
        a DataFrame with columns 'date' and 'price' that is sorted by 'date'
        """
        df = cls.standardize_price_df(df)
        result = {}
        for (ticker, currency), group in df.groupby(['ticker', 'currency']):
            group = group[['date', 'price']].sort_values('date')
            group = group.reset_index(drop=True)
            if not ticker in result.keys():
                result[ticker] = {}
            result[ticker][currency] = group
        return result

    def ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame with all ledger transactions.
        :return: Combined DataFrame with ledger data.
        """
        return self._ledger

    def serialized_ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame with all ledger transactions in long format.
        :return: Combined DataFrame with ledger data.
        """
        if self._serialized_ledger is None:
            self.complete_ledger()
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
                amount = row['amount'] * vat['rate'] / (1 + vat['rate'])
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

    def validate_accounts(self):
        """
        Validate coherence between account, VAT and FX adjustment definitions.
        """
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

    def complete_ledger(self) -> pd.DataFrame:
        # Ledger definition
        df = self.standardize_ledger(self._ledger)
        df = self.sanitize_ledger(df)
        df = df.sort_values(['date', 'id'])

        # Calculate amount to match target balance
        # TODO: drop target_balance
        if 'target_balance' in df.columns:
            warn("`target_balance` is deprecated and will be removed. "
                 "Specify an amount instead.", DeprecationWarning)
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
                adjustments = self.standardize_ledger_columns(pd.DataFrame(adjustments))
                df = pd.concat([df, adjustments])

        # Serializes ledger with separate credit and debit entries.
        result = self.serialize_ledger(df)
        self._serialized_ledger = self.standardize_ledger_columns(result)


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

    def add_ledger_entry(self, data):
        """
        Add one or more entries to the general ledger.
        """
        if isinstance(data, dict):
            # Transform one dict value to a list to avoid an error in
            # pd.DataFrame() when passing a dict of scalars:
            # ValueError: If using all scalar values, you must pass an index.
            first_key = next(iter(data))
            if not isinstance(data[first_key], collections.abc.Sequence):
                 data[first_key] = [data[first_key]]
        df = pd.DataFrame(data)
        automated_id = 'id' not in df.columns
        df = self.standardize_ledger_columns(df)

        # Ensure ID is not already in use
        duplicate = set(df['id']).intersection(self._ledger['id'])
        if len(duplicate) > 0:
            # breakpoint()
            if automated_id:
                # Replace ids by integers above the highest existing integer id
                min_id = df['id'].astype(pd.Int64Dtype()).min(skipna=True)
                max_id = self._ledger['id'].astype(pd.Int64Dtype()).max(skipna=True)
                offset = max_id - min_id + 1
                df['id'] = df['id'].astype(pd.Int64Dtype()) + offset
                df['id'] = df['id'].astype(pd.StringDtype())
            else:
                if len(duplicate) == 0:
                    message = f"Ledger id '{list(duplicate)[0]}' already used."
                else:
                    message = f"Ledger ids {duplicate} already used."
                raise ValueError(message)

        self._ledger = pd.concat([self._ledger, df], axis=0)

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

