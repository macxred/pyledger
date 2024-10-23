"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import collections
import datetime
from warnings import warn
import numpy as np
import pandas as pd
from .constants import DEFAULT_SETTINGS, LEDGER_SCHEMA
from .ledger_engine import LedgerEngine
from consistent_df import enforce_schema


class StandaloneLedger(LedgerEngine):
    """StandaloneLedger is a self-contained implementation of the LedgerEngine class,
    that provides an abstract interface for a double-entry accounting system.
    StandaloneLedger operates autonomously and does not connect to third-party
    accounting software. It serves as a base for any standalone ledger implementation
    with a specific data storage choice.

    Attributes:
        settings (dict): Accounting settings, such as beginning and end of the
            accounting period, rounding precision for currencies, etc.
        accounts (pd.DataFrame): Account chart.
        ledger (pd.DataFrame): General ledger data in original form entered
            into the accounting system, without automated enhancements such as
            reporting currency amounts, revaluations, or tax bookings.
        serialized_ledger (pd.DataFrame): Ledger in completed form, after
            automated enhancements, including reporting currency amounts,
            revaluations or tax bookings. Data is returned in long format,
            detailing accounts and counter-accounts in separate rows.
        prices (pd.DataFrame, optional): Prices data for foreign currencies,
            securities, commodities, inventory, etc.
        tax_codes (pd.DataFrame, optional): tax definitions.
        revaluations (pd.DataFrame, optional): Definitions for automated
            calculation of revaluations.
    """

    _settings = None
    _accounts = None
    _ledger = None
    _serialized_ledger = None
    _prices = None
    _tax_codes = None
    _revaluations = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(
        self,
        settings: dict = DEFAULT_SETTINGS,
        accounts: pd.DataFrame = None,
        ledger: pd.DataFrame = None,
        prices: pd.DataFrame = None,
        tax_codes: pd.DataFrame = None,
        revaluations: pd.DataFrame = None,
    ) -> None:
        """Initialize the StandaloneLedger with provided accounting data and settings.

        Args:
            settings (dict): Configuration settings for the ledger operations.
            accounts (pd.DataFrame): Account chart.
            ledger (pd.DataFrame, optional): General ledger data.
            prices (pd.DataFrame, optional): Prices data for various assets.
            tax_codes (pd.DataFrame, optional): tax definitions.
            revaluations (pd.DataFrame, optional): foreign exchange or other revaluations.
        """
        super().__init__()
        self.settings = self.standardize_settings(settings)
        self._accounts = self.standardize_accounts(accounts)
        self._ledger = self.standardize_ledger_columns(ledger)
        self._prices = self.standardize_prices(prices)
        self._tax_codes = self.standardize_tax_codes(tax_codes)
        self._revaluations = self.standardize_revaluations(revaluations)
        self.validate_accounts()

    def revaluations(self) -> pd.DataFrame:
        return self._revaluations

    # ----------------------------------------------------------------------
    # Settings

    @property
    def settings(self):
        return self._settings

    @settings.setter
    def settings(self, settings):
        self._settings = settings

    # ----------------------------------------------------------------------
    # Tax Codes

    def tax_codes(self) -> pd.DataFrame:
        return self._tax_codes

    def add_tax_code(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_tax_code is not implemented yet.")

    def modify_tax_code(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_tax_code is not implemented yet.")

    def delete_tax_codes(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_tax_codes is not implemented yet.")

    def tax_journal_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create journal entries to book tax according to tax_codes.

        Iterates through the provided DataFrame and calculates tax for entries
        that have a non-null tax_code. It generates a new journal entry for
        each tax account.

        Args:
            df (pd.DataFrame): A pandas DataFrame containing ledger entries.

        Returns:
            pd.DataFrame: A new DataFrame with tax journal entries.
            Returns empty DataFrame with the correct structure if no tax codes are present.
        """
        tax_definitions = self.tax_codes().set_index("id").to_dict("index")
        tax_journal_entries = []
        accounts = self.accounts()
        for _, row in df.loc[df["tax_code"].notna()].iterrows():
            tax = tax_definitions[row["tax_code"]]
            account_tax_code = (
                accounts.loc[
                    accounts["account"] == row["account"], "tax_code"
                ].values[0] if pd.notna(row["account"]) else None
            )
            contra_tax_code = (
                accounts.loc[
                    accounts["account"] == row["contra"], "tax_code"
                ].values[0] if pd.notna(row["contra"]) else None
            )
            if pd.isna(account_tax_code) and pd.isna(contra_tax_code):
                self._logger.warning(
                    f"Skip tax code '{row['tax_code']}' for {row['id']}: Neither account nor "
                    f"counter account have a tax_code."
                )
            elif pd.isna(account_tax_code) and pd.notna(contra_tax_code):
                multiplier = 1.0
                account = (row["contra"] if tax["is_inclusive"] else row["account"])
            elif pd.notna(account_tax_code) and pd.isna(contra_tax_code):
                multiplier = -1.0
                account = (row["account"] if tax["is_inclusive"] else row["contra"])
            else:
                self._logger.warning(
                    f"Skip tax code '{row['tax_code']}' for {row['id']}: Both account and "
                    f"counter accounts have tax_code."
                )

            # Calculate tax amount
            if tax["is_inclusive"]:
                amount = row["amount"] * tax["rate"] / (1 + tax["rate"])
            else:
                amount = row["amount"] * tax["rate"]
            amount = amount * multiplier
            amount = self.round_to_precision(amount, row["currency"])

            # Create a new journal entry for the tax amount
            if amount != 0:
                base_entry = {
                    "date": row["date"],
                    "description": "TAX: " + row["description"],
                    "account": account,
                    "document": row["document"],
                    "currency": row["currency"],
                    "report_amount": np.nan,
                    "tax_code": row["tax_code"]
                }
                if pd.notna(tax["account"]):
                    tax_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:tax",
                        "contra": tax["account"],
                        "amount": amount
                    })
                if pd.notna(tax["contra"]):
                    tax_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:tax",
                        "contra": tax["contra"],
                        "amount": -1 * amount
                    })
        result = enforce_schema(pd.DataFrame(tax_journal_entries), LEDGER_SCHEMA)

        return result

    # ----------------------------------------------------------------------
    # Accounts

    def _single_account_balance(self, account: int, date: datetime.date = None) -> dict:
        return self._balance_from_serialized_ledger(account=account, date=date)

    def accounts(self) -> pd.DataFrame:
        return self._accounts

    def add_account(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_account is not implemented yet.")

    def modify_account(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_account is not implemented yet.")

    def delete_accounts(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_accounts is not implemented yet.")

    def validate_accounts(self) -> None:
        """Validate coherence between account, tax and revaluation definitions."""
        # Ensure all tax code accounts are defined in accounts
        tax_codes = set(self._tax_codes["id"])
        missing = set(self._accounts["tax_code"].dropna()) - tax_codes
        if len(missing) > 0:
            raise ValueError(f"Some tax codes in accounts not defined: {missing}.")

        # Ensure all account tax_codes are defined in tax_codes
        accounts = set(self._accounts["account"])
        missing = set(self._tax_codes["account"].dropna()) - accounts
        if len(missing) > 0:
            raise ValueError(
                f"Some accounts in tax code definitions are not defined in the accounts: "
                f"{missing}."
            )

        # Ensure all credit and debit accounts in revaluations are defined in accounts
        df = self.revaluations()
        missing = (set(df["credit"]) | set(df["debit"])) - accounts
        if len(missing) > 0:
            raise ValueError(
                f"Some accounts in revaluation definitions are not defined in the accounts: "
                f"{missing}."
            )

    # ----------------------------------------------------------------------
    # Ledger

    def ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with all ledger transactions.

        Returns:
            pd.DataFrame: Combined DataFrame with ledger data.
        """
        return self._ledger

    def add_ledger_entry(self, data: dict) -> None:
        """Add one or more entries to the general ledger."""
        if isinstance(data, dict):
            # Transform one dict value to a list to avoid an error in
            # pd.DataFrame() when passing a dict of scalars:
            # ValueError: If using all scalar values, you must pass an index.
            first_key = next(iter(data))
            if not isinstance(data[first_key], collections.abc.Sequence):
                data[first_key] = [data[first_key]]
        df = pd.DataFrame(data)
        automated_id = "id" not in df.columns
        df = self.standardize_ledger_columns(df)

        # Ensure ID is not already in use
        duplicate = set(df["id"]).intersection(self._ledger["id"])
        if len(duplicate) > 0:
            if automated_id:
                # Replace ids by integers above the highest existing integer id
                min_id = df["id"].astype(pd.Int64Dtype()).min(skipna=True)
                max_id = self._ledger["id"].astype(pd.Int64Dtype()).max(skipna=True)
                offset = max_id - min_id + 1
                df["id"] = df["id"].astype(pd.Int64Dtype()) + offset
                df["id"] = df["id"].astype(pd.StringDtype())
            else:
                if len(duplicate) == 0:
                    message = f"Ledger id '{list(duplicate)[0]}' already used."
                else:
                    message = f"Ledger ids {duplicate} already used."
                raise ValueError(message)

        self._ledger = pd.concat([self._ledger, df], axis=0)

    def delete_ledger_entries(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_ledger_entries is not implemented yet.")

    def ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("ledger_entry is not implemented yet.")

    def modify_ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_ledger_entry is not implemented yet.")

    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with all ledger transactions in long format.

        Returns:
            pd.DataFrame: Combined DataFrame with ledger data.
        """
        if self._serialized_ledger is None:
            self.complete_ledger()
        return self._serialized_ledger

    def complete_ledger(self) -> None:
        # Ledger definition
        df = self.standardize_ledger(self._ledger)
        df = self.sanitize_ledger(df)
        df = df.sort_values(["date", "id"])

        # Calculate amount to match target balance
        # TODO: drop target_balance
        if "target_balance" in df.columns:
            warn(
                "`target_balance` is deprecated and will be removed. Specify an amount instead.",
                DeprecationWarning
            )
            new_amount = []
            for i in np.where(df["target_balance"].notna())[0]:
                date = df["date"].iloc[i]
                account = df["account"].iloc[i]
                currency = self.account_currency(account)
                self._serialized_ledger = self.serialize_ledger(df.loc[df["date"] <= date, :])
                balance = self.account_balance(account=account, date=date)
                balance = balance[currency]
                amount = df["target_balance"].iloc[i] - balance
                amount = self.round_to_precision(amount, ticker=currency, date=date)
                new_amount.append(amount)
                df.loc[range(df.shape[0]) == i, "amount"] = amount

        # Add automated journal entries for tax
        tax = self.tax_journal_entries(df)
        if tax.shape[0] > 0:
            df = pd.concat([df, tax])

        # Insert missing reporting currency amounts
        index = df["report_amount"].isna()
        df.loc[index, "report_amount"] = self._report_amount(
            amount=df.loc[index, "amount"],
            currency=df.loc[index, "currency"],
            date=df.loc[index, "date"]
        )

        # revaluations
        revaluations = self.revaluations()
        reporting_currency = self.reporting_currency
        for row in revaluations.to_dict("records"):
            self._serialized_ledger = self.serialize_ledger(df)
            date = row["date"]
            accounts = self.account_range(row["account"])
            accounts = set(accounts["add"]) - set(accounts["subtract"])
            revaluations = []
            for account in accounts:
                currency = self.account_currency(account)
                if currency != reporting_currency:
                    balance = self.account_balance(account, date=date)
                    fx_rate = self.price(currency, date=date, currency=reporting_currency)
                    if fx_rate[0] != reporting_currency:
                        raise ValueError(
                            f"FX rate currency mismatch: expected {reporting_currency}, got "
                            f"{fx_rate[0]}"
                        )
                    target = balance[currency] * fx_rate[1]
                    amount = target - balance["reporting_currency"]
                    amount = self.round_to_precision(amount, ticker=reporting_currency, date=date)
                    id = f"revaluation:{date}:{account}"
                    revaluations.append({
                        "id": id,
                        "date": date,
                        "account": account,
                        "currency": currency,
                        "amount": 0,
                        "report_amount": amount,
                        "description": row["description"]
                    })
                    revaluations.append({
                        "id": id,
                        "date": date,
                        "account": row["credit"] if amount > 0 else row["debit"],
                        "currency": reporting_currency,
                        "amount": -1 * amount,
                        "report_amount": -1 * amount,
                        "description": row["description"]
                    })
            if len(revaluations) > 0:
                revaluations = self.standardize_ledger_columns(pd.DataFrame(revaluations))
                df = pd.concat([df, revaluations])

        # Serializes ledger with separate credit and debit entries.
        result = self.serialize_ledger(df)
        self._serialized_ledger = self.standardize_ledger_columns(result)

    # Hack: abstract functionality to compute balance from serialized ledger,
    # that is used in two different branches of the dependency tree
    # (TextLedger, CachedProffixLedger). Ideally these two classes would have
    # a common ancestor that could accommodate below method.
    def _balance_from_serialized_ledger(self, account: int, date: datetime.date = None) -> dict:
        """Compute balance from serialized ledger.

        This method is used in different branches of the dependency tree.

        Args:
            account (int): The account number.
            date (datetime.date, optional): The date up to which the balance is computed.
                                            Defaults to None.

        Returns:
            dict: Dictionary containing the balance of the account in various currencies.
        """
        df = self.serialized_ledger()
        rows = df["account"] == int(account)
        if date is not None:
            rows = rows & (df["date"] <= date)
        cols = ["amount", "report_amount", "currency"]
        if rows.sum() == 0:
            result = {"reporting_currency": 0.0}
            currency = self.account_currency(account)
            if currency is not None:
                result[currency] = 0.0
        else:
            sub = df.loc[rows, cols]
            reporting_amount = sub["report_amount"].sum()
            amount = sub.groupby("currency").agg({"amount": "sum"})
            amount = {currency: amount for currency, amount in zip(amount.index, amount["amount"])}
            result = {"reporting_currency": reporting_amount} | amount
        return result

    # ----------------------------------------------------------------------
    # Currency

    @property
    def reporting_currency(self) -> str:
        return self.settings["reporting_currency"]

    def _report_amount(
        self, amount: list[float], currency: list[str], date: list[datetime.date]
    ) -> list[float]:
        reporting_currency = self.reporting_currency
        if not (len(amount) == len(currency) == len(date)):
            raise ValueError("Vectors 'amount', 'currency', and 'date' must have the same length.")
        result = [
            self.round_to_precision(
                a * self.price(t, date=d, currency=reporting_currency)[1],
                reporting_currency, date=d)
            for a, t, d in zip(amount, currency, date)]
        return result

    # ----------------------------------------------------------------------
    # Price

    def price(
        self,
        ticker: str,
        date: datetime.date = None,
        currency: str | None = None
    ) -> tuple[str, float]:
        """Retrieve price for a given ticker as of a specified date. If no price is available
        on the exact date, return latest price observation prior to the specified date.

        Args:
            ticker (str): Asset identifier.
            date (datetime.date): Date for which the price is required.
            currency (str, optional): Currency in which the price is desired.

        Returns:
            tuple: (currency, price) where 'currency' is a string indicating the currency
            of the price, and 'price' is a float representing the asset's price as of the
            specified date.
        """
        if currency is not None and str(ticker) == str(currency):
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
        prc = prc.loc[prc["date"].dt.normalize() <= pd.Timestamp(date), "price"]

        if prc.empty:
            raise ValueError(f"No {currency} prices available for '{ticker}' before {date}.")

        return (currency, prc.iloc[-1].item())

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        return self.settings["precision"][ticker]

    def add_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_price is not implemented yet.")

    def modify_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_price is not implemented yet.")

    def delete_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_price is not implemented yet.")

    def price_history(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_history is not implemented yet.")

    def price_increment(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_increment is not implemented yet.")

    def assets(self, *args, **kwargs) -> None:
        raise NotImplementedError("assets is not implemented yet.")

    def add_asset(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_asset is not implemented yet.")

    def modify_asset(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_asset is not implemented yet.")

    def delete_asset(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_asset is not implemented yet.")

    def add_revaluation(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_revaluation is not implemented yet.")

    def modify_revaluation(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_revaluation is not implemented yet.")

    def delete_revaluations(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_revaluations is not implemented yet.")
