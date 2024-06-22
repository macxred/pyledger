from abc import ABC, abstractmethod, abstractproperty
import datetime, logging, math, pandas as pd, numpy as np, os, re
import openpyxl
from pathlib import Path
from .helpers import represents_integer
from .time import parse_date_span
from . import excel

class LedgerEngine(ABC):
    """
    Abstract base class defining the core interface for managing a ledger
    system, including account chart, VAT management, and arbitrary assets or
    currencies.
    """
    REQUIRED_LEDGER_COLUMNS = [
        'date', 'account', 'amount', 'currency', 'text']
    OPTIONAL_LEDGER_COLUMNS = [
        'id', 'counter_account', 'base_currency_amount', 'vat_code', 'document']
    LEDGER_COLUMN_SHORTCUTS = {
        'cur': 'currency',
        'vat': 'vat_code',
        'target': 'target_balance',
        'base_amount': 'base_currency_amount',
        'counter': 'counter_account'}
    LEDGER_COLUMN_SEQUENCE = [
        'id', 'date', 'account', 'counter_account', 'currency', 'amount',
        'target_balance', 'balance', 'base_currency_amount',
        'base_currency_balance', 'vat_code', 'text', 'document']
    REQUIRED_ACCOUNT_COLUMNS = ['account', 'currency', 'text']
    OPTIONAL_ACCOUNT_COLUMNS = ['vat_code', 'group']
    REQUIRED_VAT_CODE_COLUMNS = ['id', 'rate', 'inclusive', 'account', 'text']
    OPTIONAL_VAT_CODE_COLUMNS = ['date', 'inverse_account']
    REQUIRED_PRICE_COLUMNS = ['ticker', 'date', 'currency', 'price']
    REQUIRED_FX_ADJUSTMENT_COLUMNS = ['date', 'adjust', 'credit', 'debit', 'text']

    _logger = None

    def __init__(self):
        self._logger = logging.getLogger('ledger')

    @property
    @abstractmethod
    def base_currency(self) -> str:
        """
        Returns the base currency used for financial reporting.
        """

    @abstractmethod
    def precision(self, ticker: str,
                        date: datetime.date = datetime.date.today()) -> float:
        """
        Returns the smallest increment for quotation of prices of a given
        asset or currency. This is the precision, to which prices should
        be rounded.
        :param ticker: Reference to the associated document.
        :param date: Date for which to retrieve the precision.
        """

    def round_to_precision(self, amount: float, ticker: str,
                        date: datetime.date = datetime.date.today()) -> float:
        """
        Round an amount to the precision of a specified asset or currency.

        This method retrieves the precision for the specified ticker and date,
        then rounds the amount to the nearest multiple of this precision.

        Parameters:
        amount (float): Value to be rounded.
        ticker (str): Ticker symbol of the asset or currency.
        date (datetime.date, optional): Date for precision determination.
                                        Defaults to today's date.

        Returns:
        float: Rounded amount, adjusted to the specified asset's precision.
        """
        precision = self.precision(ticker=ticker, date=date)
        result = round(amount / precision, 0) * precision
        return round(result, -1 * math.floor(math.log10(precision)))

    @abstractmethod
    def ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame representing all ledger transactions.
        :return: pd.DataFrame with columns `id` (str), `date` (datetime),
            `document` (str), `amounts` (DataFrame). `amounts` is a nested
            DataFrame with columns `account` (int), `counter_account`
            (int or None), `currency` (str), `amount` (float),
            `base_currency_amount` (float or None), and `vat_code` (str).
        """

    def serialized_ledger(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame with a long representation of all ledger
        transactions. Simple transactions with a credit and debit account are
        represented twice, one with 'account' corresponding to the credit
        account and one with account corresponding to the debit account.
        :return: pd.DataFrame with columns `id` (str), `date` (datetime),
            `account` (int), `counter_account` (int or None), `currency` (str),
            `amount` (float), `base_currency_amount` (float or None),
            `vat_code` (str), and `document` (str).
        """
        return self.serialize_ledger(self.ledger())

    @abstractmethod
    def ledger_entry(self, id) -> dict:
        """
        Retrieves a specific ledger entry by its ID.
        :param id: The unique identifier for the ledger entry.
        :return: A dictionary representing the ledger entry.
        """

    @abstractmethod
    def add_ledger_entry(self, date: datetime.date, document: str,
                         amounts: pd.DataFrame) -> None:
        """
        Adds a new posting to the ledger.
        :param date: Date of the transaction.
        :param document: Reference to the associated document.
        :param amounts: DataFrame detailing transaction amounts.
            Columns include: `account` (int), `counter_account`
            (int, optional), `currency` (str), `amount` (float),
            `base_currency_amount` (float, optional), `vat_code`
            (str, optional).
        """

    @abstractmethod
    def modify_ledger_entry(self, id: str, new_data: dict) -> None:
        """
        Modifies an existing ledger entry.
        :param id: String, unique identifier of the ledger entry to be modified.
        :param new_data: Dictionary, fields to be overwritten in the ledger
            entry. Keys typically include `date` (datetime.date), `document`
            (str), or `amounts` (DataFrame).
        """


    @abstractmethod
    def delete_ledger_entry(self, id: str) -> None:
        """
        Deletes a ledger entry by its ID.
        :param id: String, the unique identifier of the ledger entry to delete.
        """

    @abstractmethod
    def account_chart(self) -> pd.DataFrame:
        """
        Retrieves a data frame with all account definitions.
        :return: pd.DataFrame with columns `account` (int), `description`
            (str), `currency` (str), `vat_code` (str or None).
            `None` implies VAT is never applicable. If set, VAT is sometimes
            applicable, and transactions on this account must explicitly state
            a `vat_code`. The value in the account chart serves as default for
            new transactions.
        """

    def account_currency(self, account):
        account_chart = self.account_chart()
        if not int(account) in account_chart.index:
            raise ValueError(f"Account {account} is not defined.")
        return account_chart['currency'][account]

    @abstractmethod
    def add_account(self, account: int, description: str, currency: str, vat: bool = False) -> None:
        """
        Appends an account to the account chart.
        :param account: Integer, unique identifier for the account.
        :param description: String, description of the account.
        :param currency: String, currency of the account.
        :param vat: Boolean, indicates if VAT is applicable.
        """

    @abstractmethod
    def modify_account(self, account: int, new_data: dict) -> None:
        """
        Modifies an existing account definition.
        :param account: Integer, account to be modified.
        :param new_data: Dictionary, fields to be overwritten. Keys typically
            include `description` (str), `currency` (str), or `vat_code` (str).
        """

    @abstractmethod
    def delete_account(self, account: int) -> None:
        """
        Removes an account from the account chart.
        :account id: Int, the account to be removed.
        """

    @abstractmethod
    def price_history(self) -> pd.DataFrame:
        """
        Retrieves a data frame with all price definitions.
        :return: pd.DataFrame with columns `ticker` (str), `date` (Date),
            `currency` (str), and `price` (float). Tickers can be arbitrarily
            chosen and can represent anything, including foreign currencies,
            securities, commodities, or inventory. Price observations are
            uniquely defined by a date/ticker/currency triple. Prices can be
            defined in any other currency and are applied up to the subsequent
            price definition for the same ticker/currency pair.
        """

    @abstractmethod
    def price(self, ticker: str, date: datetime.date,
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
                   the currency of the price, and 'price' is a float representing
                   the asset's price as of the specified date.
        """

    @abstractmethod
    def add_price(self, ticker: str, date: datetime.date, currency: str,
                  price: float, overwrite: bool = False) -> None:
        """
        Appends a price to the price history.
        :param ticker: String, asset identifier.
        :param date: datetime.date, Date on which the price is recorded.
        :param currency: String, currency in which the price is quoted.
        :param price: Float, value of the asset as of the given date.
        :param overwrite: Bool, overwrite an existing price definition with
            same ticker, date and currency if one exists.
        """

    @abstractmethod
    def delete_price(self, ticker: str, date: datetime.date,
                     currency: str | None = None) -> None:
        """
        Removes a price definition from the history.
        :param ticker: String, asset identifier.
        :param date: datetime.date, Date on which the price is recorded.
        :param currency: String, currency in which the price is quoted.
            `None` indicates that price definitions for this ticker in
            all currencies should be removed.
        """

    @abstractmethod
    def vat_codes(self) -> pd.DataFrame:
        """
        Retrieves all vat definitions.
        :return: pd.DataFrame with columns `code` (str), `date` (Date),
            `rate` (float), `included` (bool), `account` (int).
        """

    @abstractmethod
    def add_vat_code(self, code: str, rate: float, account: str,
                     included: bool = True,
                     date: datetime.date | None = None) -> None:
        """
        Append a vat code to the list of available vat_codes.
        :param code: Str, Identifier for the vat definition.
        :param rate: Float, the VAT rate to apply.
        :param included: bool, specifies whether the VAT amount is included in
            the transaction amount?
        :param date: datetime.date or None, date, from which onward the vat
            definition is applied. The definition is valid until the same vat
            code is re-defined on a subsequent date.
        """

    @abstractmethod
    def delete_vat_code(self, code: str,
                        date: datetime.date | None = None) -> None:
        """
        Removes a vat definition.
        :code: Str, vat code to be removed.
        :date: datetime.date, date, on which the vat code is removed. If
            `None`, all entries with the given vat_code are removed.
        """

    @abstractmethod
    def _single_account_balance(self, account: int,
                                date: datetime.date = None) -> dict:
        """
        """

    def _account_balance_list(self, accounts, date):
        result = {}
        for account in accounts:
            account_balance = self._single_account_balance(account, date=date)
            for currency, value in account_balance.items():
                result[currency] = result.get(currency, 0) + value
        return result


    def _account_balance_range(self, accounts, date):
        result = {}
        add = self._account_balance_list(accounts['add'], date=date)
        subtract = self._account_balance_list(accounts['subtract'], date=date)
        for currency, value in add.items():
            result[currency] = result.get(currency, 0) + value
        for currency, value in subtract.items():
            result[currency] = result.get(currency, 0) - value
        return result


    def account_balance(self, account: int | str | dict,
                        date: datetime.date = None) -> dict:
        """
        Balance of a single account or a list of accounts.

        Parameters:
        account (int, str, dict): The account(s) to be evaluated. Can be a
            sequence of accounts separated by a column, e.g. "1000:1999", in
            which case the combined balance of all accounts within the
            specified range is returned. Multiple accounts and/or account
            sequences can be separated by a plus or minus sign,
            e.g. "1000+1020:1025", in which case the combined balance of all
            accounts is returned, or "1020:1025-1000", in which case the
            balance of account 1000 is deducted from the combined balance of
            accounts 1020:1025.
        date (datetime.date or str isoformat, optional): The date as of which
            the account balance is calculated. If None, the current date is
            used. Can be a single date or a time period.
        """
        start, end = parse_date_span(date)
        if start is None:
            # Account balance per a single point in time
            if represents_integer(account):
                result = self._single_account_balance(account=abs(int(account)),
                                                      date=end)
                if int(account) < 0:
                    result = {key: -1.0 * val for key, val in result.items()}
            elif (isinstance(account, dict)
                and ('add' in account.keys())
                and ('subtract' in account.keys())):
                result = self._account_balance_range(accounts=account, date=end)
            elif isinstance(account, str):
                accounts = self.account_range(account)
                result = self._account_balance_range(accounts=accounts, date=end)
            else:
                raise ValueError(f"Account(s) '{account}' of type "
                                f"{type(account).__name__} not identifiable.")
        else:
            # Account balance over a period
            at_start = self.account_balance(account=account,
                date=start-datetime.timedelta(days=1))
            at_end = self.account_balance(account=account, date=end)
            result = {
                currency: at_end.get(currency, 0) - at_start.get(currency, 0)
                for currency in (at_start | at_end).keys()}

        result = {
            ticker: self.round_to_precision(value, ticker=ticker, date=date)
            for ticker, value in result.items()
        }

        return result

    def account_history(self, account: int | str | dict,
                        period: datetime.date = None) -> pd.DataFrame:
        """
        Transaction and balance history of an account or a list of accounts.

        Parameters:
        account (int, str, dict): The account(s) to be evaluated. Can be a
            sequence of accounts separated by a column, e.g. "1000:1999", in
            which case the combined balance of all accounts within the
            specified range is returned. Multiple accounts and/or account
            sequences can be separated by a plus or minus sign,
            e.g. "1000+1020:1025", in which case the combined balance of all
            accounts is returned, or "1020:1025-1000", in which case the
            balance of account 1000 is deducted from the combined balance of
            accounts 1020:1025.
        period (datetime.date or str isoformat, optional): The date as of which
            the account balance is calculated. If None, the current date is
            used. Can be a single date or a time period.
        """
        start, end = parse_date_span(period)
        # Account balance per a single point in time
        if represents_integer(account):
            account = int(account)
            if not account in self.account_chart().index:
                raise ValueError(f"No account matching '{account}'.")
            out = self._fetch_account_history(account, start=start, end=end)
        elif (isinstance(account, dict)
            and ('add' in account.keys())
            and ('subtract' in account.keys())):
            accounts = list(set(accounts['add']) - set(accounts['subtract']))
            out = self._fetch_account_history(accounts, start=start, end=end)
        elif isinstance(account, str):
            accounts = self.account_range(account)
            accounts = list(set(accounts['add']) - set(accounts['subtract']))
            out = self._fetch_account_history(accounts, start=start, end=end)
        elif isinstance(account, list):
            not_integer = [i for i in account if not represents_integer(i)]
            if any(not_integer):
                raise ValueError(f"Non-integer list elements in `account`: "
                                 f"{not_integer}.")
            accounts = account=[int(i) for i in account]
            out = self._fetch_account_history(accounts, start=start, end=end)
        else:
            raise ValueError(f"Account(s) '{account}' of type "
                            f"{type(account).__name__} not identifiable.")

        return out

    def _fetch_account_history(self, account: int | list[int],
                               start: datetime.date = None,
                               end: datetime.date = None) -> pd.DataFrame:
        """
        Fetch transaction history of a list of accounts. Compute balance.
        """
        ledger = self.serialized_ledger()
        if isinstance(account, list):
            filter = ledger['account'].isin(account)
        else:
            filter = ledger['account'] == account
        if end is not None:
            filter = filter & (ledger['date'] <= end)
        df = ledger.loc[filter, :]
        df = df.sort_values('date')
        df['balance'] = df['amount'].cumsum()
        df['base_currency_balance'] = df['base_currency_amount'].cumsum()
        cols = [col for col in self.LEDGER_COLUMN_SEQUENCE if col in df.columns]
        if start is not None:
            df = df.loc[df['date'] >= start, :]
        df = df.reset_index(drop=True)
        return df[cols]

    def account_range(self, range: int | str) -> dict:
        add = []
        subtract = []
        if represents_integer(range):
            account = int(range)
            if abs(account) in self.account_chart().index:
                if account >= 0:
                    add = [account]
                else:
                    subtract = [abs(account)]
        elif isinstance(range, float):
            raise ValueError(f"`range` {range} is not an integer value.")
        elif isinstance(range, str):
            is_addition = True
            for element in  re.split(r"(-|\+)", range.strip()):
                accounts = []
                if element.strip() == "":
                    pass
                elif element.strip() == "+":
                    is_addition = True
                elif element.strip() == "-":
                    is_addition = False
                elif re.fullmatch("^[^:]*[:][^:]*$", element):
                    # `element` contains exactly one colon (':')
                    sequence = element.split(":")
                    first = int(sequence[0].strip())
                    last = int(sequence[1].strip())
                    chart = self.account_chart()
                    in_range = (chart.index >= first) & (chart.index <= last)
                    accounts = list(chart.index[in_range])
                elif re.match("[0-9]*", element.strip()):
                    accounts = [int(element.strip())]
                else:
                    raise ValueError(f"Accounts '{element}' not identifiable.")
                if len(accounts) > 0:
                    if is_addition:
                        add += accounts
                    else:
                        subtract += accounts
        else:
            raise ValueError(f"Expecting int, float, or str `range`, "
                             f"not {type(range).__name__} {range}.")
        if (len(add) == 0) and (len(subtract) == 0):
            raise ValueError(f"No account matching '{range}'.")
        return {'add': add, 'subtract': subtract}

    def sanitize_acount_chart(self, accounts: pd.DataFrame) -> pd.DataFrame:
        """
        Discards inconsistent entries in the account chart.
        Logs a warning for each discarded entry with reason for dropping.

        :param accounts: Account chart as a DataFrame.
        :return: DataFrame with sanitized account chart.
        """
        accounts = self.st

    def sanitize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """
        Discards inconsistent ledger entries and inconsistent vat codes.
        Logs a warning for each discarded entry with reason for dropping.

        :param ledger: Ledger data as a DataFrame.
        :return: DataFrame with sanitized ledger entries.
        """
        # Discard undefined VAT codes
        ledger['vat_code'] = ledger['vat_code'].str.strip()
        invalid = (ledger['vat_code'].notna()
                   & ~ledger['vat_code'].isin(self.vat_codes().index))
        if invalid.any():
            df = ledger.loc[invalid, ['id', 'vat_code']]
            df = df.groupby('id').agg({'vat_code': lambda x: x.unique()})
            for id, codes in zip(df.index, df['vat_code']):
                if len(codes) > 1:
                    self._logger.warning(f"Discard unknown VAT codes "
                        f"{', '.join([f"'{x}'" for x in codes])} at '{id}'.")
                else:
                    self._logger.warning(f"Discard unknown VAT code "
                        f"'{codes[0]}' at '{id}'.")
            ledger.loc[invalid, 'vat_code'] = None

        # Collect postings to be discarded as pd.DataFrame with columns 'id' and
        # 'text'. The latter specifies the reason(s) why the entry is discarded.
        to_discard = [] # List[pd.DataFrame['id', 'text']]

        # 1. Discard journal entries with non-unique dates
        grouped = ledger[['date', 'id']].groupby('id')
        df = grouped.filter(lambda x: x['date'].nunique() > 1)
        if df.shape[0] > 0:
            df = df.drop_duplicates()
            df['text'] = df['date'].astype(pd.StringDtype())
            df = df.groupby('id').agg({
                'text': lambda x: f"multiple dates {', '.join(x)}"})
            to_discard.append(df.reset_index()[['id', 'text']])

        # 2. Discard journal entries with undefined accounts
        valid_accounts = self.account_chart().index
        invalid_accounts = []
        for col in ['account', 'counter_account']:
            invalid = ledger[col].notna() & ~ledger[col].isin(valid_accounts)
            if invalid.any():
                df = ledger.loc[invalid, ['id', col]]
                df = df.rename(columns={col: 'account'})
                invalid_accounts.append(df)
        if len(invalid_accounts) > 0:
            df = pd.concat(invalid_accounts)
            df = df.groupby('id').agg({'account': lambda x: x.unique()})
            df['text'] = [
                f"Accounts {', '.join([str(y) for y in x])} not defined"
                if len(x) > 1  else f"Account {x[0]} not defined"
                for x in df['account']]
            to_discard.append(df.reset_index()[['id', 'text']])

        # 2. Discard journal entries without amount or balance or with both
        if 'target_balance' in ledger.columns:
            invalid = ledger['amount'].isna() & ledger['target_balance'].isna()
        else:
            invalid = ledger['amount'].isna()
        if invalid.any():
            df = pd.DataFrame({
                'id': ledger.loc[invalid, 'id'].unique(),
                'text': "amount missing"})
            to_discard.append(df)
        if 'target_balance' in ledger.columns:
            invalid = (ledger['amount'].notna() &
                       ledger['target_balance'].notna())
            if invalid.any():
                df = pd.DataFrame({
                    'id': ledger.loc[invalid, 'id'].unique(),
                    'text': "both amount and target amount defined"})
                to_discard.append(df)

        # Discard journal postings identified above
        if len(to_discard) > 0:
            df = pd.concat(to_discard)
            df = df.groupby('id').agg({'text': lambda x: ', '.join(x)})
            for id, text in zip(df.index, df['text']):
                self._logger.warning(f"Discard ledger entry '{id}': {text}.")
            ledger = ledger.loc[~ledger['id'].isin(df.index), :]

        # # Add account chart VAT codes for 'account' and 'counter_account'
        # account_chart = self.account_chart[['account', 'vat_code']]
        # df = df.merge(account_chart, on='account', how='left',
        #               suffixes=('', '_account'))
        # df = df.merge(account_chart, left_on='account',
        #               right_on='counter_account', how='left',
        #               suffixes=('', '_counter_account'))

        # # Check whether vat_codes match account definition
        # failed = (df['vat_code_account'].notna() &
        #           df['vat_code_counter_account'].notna())
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries where both "
        #                   f"account and counter_account have VAT codes in the "
        #                   f"account chart: {df.index[failed]}.", UserWarning)

        # failed = (df['vat_code'].notna()
        #           & df['vat_code_account'].isna()
        #           & df['vat_code_counter_account'].isna())
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries with VAT code "
        #                   f"where neither account and counter_account have VAT "
        #                   f"codes in the account chart: {df.index[failed]}.",
        #                   UserWarning)

        # failed = (df['vat_code'].isna()
        #           & (df['vat_code_account'].notna()
        #              | df['vat_code_counter_account'].notna()))
        # if failed.any():
        #     warnings.warn(f"Found {failed.sum()} ledger entries without VAT "
        #                   f"code where either account and counter_account "
        #                   f"requires a VAT code: {df.index[failed]}.",
        #                   UserWarning)

        return ledger

    @staticmethod
    def standardize_ledger_columns(ledger: pd.DataFrame | None) -> pd.DataFrame:
        """
        Standardizes and enforces type consistency for the ledger DataFrame.

        Ensures that the required columns are present in the ledger DataFrame,
        adds any missing optional columns with None values, and enforces
        specific data types for each column.

        Args:
            ledger (pd.DataFrame): A data frame with ledger transactions. Can
                be None, in which case an empty DataFrame with the required
                structure is returned.

        Returns:
            pd.DataFrame: A standardized DataFrame with both the required and
                optional columns with enforced data types.

        Raises:
            ValueError: If required columns are missing from the ledger DataFrame.
        """
        if ledger is None:
            # Return empty DataFrame with identical structure
            df = pd.DataFrame(columns=LedgerEngine.REQUIRED_LEDGER_COLUMNS)
        else:
            df = ledger.copy()
            # Standardize column names
            df = df.rename(columns=LedgerEngine.LEDGER_COLUMN_SHORTCUTS)

        # In empty DataFrames, add required columns if not present
        if isinstance(ledger, pd.DataFrame) and (len(ledger) == 0):
            for col in LedgerEngine.REQUIRED_LEDGER_COLUMNS:
                if col not in df.columns:
                    df[col] = None

        # Ensure all required columns are present
        missing = set(LedgerEngine.REQUIRED_LEDGER_COLUMNS) - set(df.columns)
        if len(missing) > 0:
            raise ValueError(f"Missing required columns: {missing}")

        # Add optional columns if not present
        if 'id' not in df.columns:
            df['id'] = df['date'].notna().cumsum().astype(pd.StringDtype())
        for col in LedgerEngine.OPTIONAL_LEDGER_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Enforce column data types
        df['id'] = df['id'].astype(pd.StringDtype())
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['account'] = df['account'].astype(pd.Int64Dtype())
        df['counter_account'] = df['counter_account'].astype(pd.Int64Dtype())
        df['currency'] = df['currency'].astype(pd.StringDtype())
        df['amount'] = df['amount'].astype(pd.Float64Dtype())
        df['base_currency_amount'] = (
            df['base_currency_amount'].astype(pd.Float64Dtype()))
        df['vat_code'] = df['vat_code'].astype(pd.StringDtype())
        df['text'] = df['text'].astype(pd.StringDtype())
        df['document'] = df['document'].astype(pd.StringDtype())

        # Order columns based on 'LEDGER_COLUMN_SEQUENCE'
        col_order = LedgerEngine.LEDGER_COLUMN_SEQUENCE
        cols = ([col for col in col_order if col in df.columns]
                + [col for col in df.columns if col not in col_order])
        df = df[cols]

        return df

    def standardize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """
        Convert ledger entries to a canonical representation.

        This method converts ledger entries into a standardized format. It
        ensures uniformity where transactions can be defined in various
        equivalent ways, allowing for easy identification of equivalent
        entries.

        Args:
            ledger (pd.DataFrame): A data frame with ledger transactions.

        Returns:
            pd.DataFrame: A DataFrame with ledger entries in canonical form.

        Notes:
            - The method removes redundant 'base_currency_amount' values for
            transactions in the base currency.
            - It fills missing dates in collective transactions with dates from
            other line items in the same collective transaction.

        This method can be overridden by derived classes to adapt to specific
        limitations of a ledger system. For example, some systems might allow
        specifying documents only for entire collective transactions instead
        of each line item.
        """
        df = self.standardize_ledger_columns(ledger)

        # Fill missing (NA) dates
        df['date'] = df.groupby('id')['date'].ffill()
        df['date'] = df.groupby('id')['date'].bfill()

        # Drop redundant base_currency_amount for transactions in base currency
        is_base_currency = df['currency'] == self.base_currency
        df.loc[is_base_currency, 'base_currency_amount'] = pd.NA
        return df

    def serialize_ledger(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Serializes the ledger into a long format. Simple journal postings with
        both credit and debit entries appear twice, one with account in the
        resulting data frame corresponding to the credit account and one
        corresponding to the debit account.

        :param df: DataFrame containing the original ledger entries in wide
            format.
        :return: Serialized DataFrame in long format.
        """
        # Create separate DataFrames for credit and debit accounts
        cols = ['id', 'date', 'account', 'counter_account', 'currency',
                'amount', 'base_currency_amount', 'vat_code', 'text',
                'document']
        credit = df[cols]
        debit = credit.copy()
        debit['amount'] *= -1.0
        debit['base_currency_amount'] *= -1.0
        debit['account'] = df['counter_account']
        debit['counter_account'] = df['account']
        # Comnbine credit and debit entries
        result = pd.concat([credit.loc[credit['account'].notna()],
                            debit.loc[debit['account'].notna()]])
        return result[cols]

    def export_account_sheets(self, file: str, root: str = None) -> None:
        """
        Export ledger as Excel file, with separate sheets containing
        transactions for each account.

        Args:
            file (str): The path where the Excel file will be saved.
            root (str | None): Root directory for document paths.
                If not None, valid document paths are formatted as hyperlinks
                to the file in the root folder.
        """
        # TODO: Move to accountbot
        out = {}
        for account in self.account_chart().index.sort_values():
            df = self._fetch_account_history(account)
            out[str(account)] = df.drop(columns='account')
        excel.write_sheets(out, path=file)

        if root is not None:
            # Add link to open documents locally
            root_dir = Path(root).expanduser()
            workbook = openpyxl.load_workbook(file)
            for sheet in workbook.worksheets:
                for cell in sheet['K']:
                    # Column K corresponds to 'documents'
                    if not cell.value is None:
                        document = root_dir.joinpath(cell.value)
                        if document.is_file():
                            cell.hyperlink = f"file://{str(document)}"
                            cell.style = "Hyperlink"
            workbook.save(file)

    # Hack: abstract functionality to compute balance from serialized ledger,
    # that is used in two different branches of the dependency tree
    # (TextLedger, CachedProffixLedger). Ideally these two classes would have
    # a common ancestor that could accommodate below method.
    def _balance_from_serialized_ledger(self, account: int,
                                        date: datetime.date = None) -> dict:
        df = self.serialized_ledger()
        rows = df['account'] == int(account)
        if date is not None:
            rows = rows & (df['date'] <= date)
        cols = ['amount', 'base_currency_amount', 'currency']
        if rows.sum() == 0:
            result = {'base_currency': 0.0}
            currency = self.account_currency(account)
            if currency is not None:
                result[currency] = 0.0
        else:
            sub = df.loc[rows, cols]
            base_amount = sub['base_currency_amount'].sum()
            amount = sub.groupby('currency').agg({'amount': "sum"})
            amount = {
                currency: amount
                for currency, amount in zip(amount.index, amount['amount'])}
            result = {'base_currency': base_amount} | amount
        return result

