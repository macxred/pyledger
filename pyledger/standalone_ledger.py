"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import collections
import datetime
from warnings import warn
import numpy as np
import pandas as pd
from .constants import (
    REQUIRED_LEDGER_COLUMNS,
    OPTIONAL_LEDGER_COLUMNS,
)
from .ledger_engine import LedgerEngine


class StandaloneLedger(LedgerEngine):
    """StandaloneLedger is a self-contained implementation of the LedgerEngine class,
    that provides an abstract interface for a double-entry accounting system.
    StandaloneLedger operates autonomously and does not connect to third-party
    accounting software. It handles accounting data primarily in pandas DataFrames and
    provides methods to enforce type consistency for these DataFrames. This class serves
    as a base for any standalone ledger implementation with a specific data storage choice.

    Attributes:
        settings (dict): Accounting settings, such as beginning and end of the
            accounting period, rounding precision for currencies, etc.
        accounts (pd.DataFrame): Account chart.
        ledger (pd.DataFrame): General ledger data in original form entered
            into the accounting system, without automated enhancements such as
            base currency amounts, FX adjustments, or VAT bookings.
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

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(
        self,
        settings: dict = None,
        accounts: pd.DataFrame = None,
        ledger: pd.DataFrame = None,
        prices: pd.DataFrame = None,
        vat_codes: pd.DataFrame = None,
        fx_adjustments: pd.DataFrame = None,
    ) -> None:
        """Initialize the StandaloneLedger with provided accounting data and settings.

        Args:
            settings (dict): Configuration settings for the ledger operations.
            accounts (pd.DataFrame): Account chart.
            ledger (pd.DataFrame, optional): General ledger data.
            prices (pd.DataFrame, optional): Prices data for various assets.
            vat_codes (pd.DataFrame, optional): VAT definitions.
            fx_adjustments (pd.DataFrame, optional): FX adjustment definitions.
        """
        super().__init__()
        self._settings = self.standardize_settings(settings)
        self._account_chart = self.standardize_account_chart(accounts)
        self._ledger = self.standardize_ledger_columns(ledger)
        self._prices = self.standardize_prices(prices)
        self._vat_codes = self.standardize_vat_codes(vat_codes)
        self._fx_adjustments = self.standardize_fx_adjustments(fx_adjustments)
        self.validate_accounts()

    def fx_adjustments(self) -> pd.DataFrame:
        return self._fx_adjustments

    # ----------------------------------------------------------------------
    # VAT Codes

    def vat_codes(self) -> pd.DataFrame:
        return self._vat_codes

    def vat_rate(self, vat_code: str) -> float:
        """Retrieve the VAT rate for a given VAT code.

        Args:
            vat_code (str): VAT code to look up.

        Returns:
            float: VAT rate associated with the specified code.

        Raises:
            KeyError: If the VAT code is not defined.
        """
        if vat_code not in self._vat_codes["id"].values:
            raise KeyError(f"VAT code not defined: {vat_code}")
        return self._vat_codes["rate"][vat_code]

    def vat_accounts(self, vat_code: str) -> list[int]:
        """Retrieve the accounts associated with a given VAT code.

        Args:
            vat_code (str): VAT code to look up.

        Returns:
            list[int]: List of accounts associated with the specified VAT code.

        Raises:
            KeyError: If the VAT code is not defined.
        """
        if vat_code not in self._vat_codes["id"].values:
            raise KeyError(f"VAT code not defined: {vat_code}")
        return self._vat_codes["accounts"][vat_code]

    def add_vat_code(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_vat_code is not implemented yet.")

    def modify_vat_code(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_vat_code is not implemented yet.")

    def delete_vat_code(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_vat_code is not implemented yet.")

    def mirror_vat_codes(self, *args, **kwargs) -> None:
        raise NotImplementedError("mirror_vat_code is not implemented yet.")

    def vat_journal_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create journal entries to book VAT according to vat_codes.

        Iterates through the provided DataFrame and calculates VAT for entries
        that have a non-null vat_code. It generates a new journal entry for
        each VAT account.

        Args:
            df (pd.DataFrame): A pandas DataFrame containing ledger entries.

        Returns:
            pd.DataFrame: A new DataFrame with VAT journal entries.
            Returns empty DataFrame with the correct structure if no VAT codes are present.
        """
        vat_definitions = self.vat_codes().set_index("id").to_dict("index")
        vat_journal_entries = []
        account_chart = self.account_chart()
        for _, row in df.loc[df["vat_code"].notna()].iterrows():
            vat = vat_definitions[row["vat_code"]]
            account_vat_code = (
                account_chart.loc[
                    account_chart["account"] == row["account"], "vat_code"
                ].values[0] if pd.notna(row["account"]) else None
            )
            counter_vat_code = (
                account_chart.loc[
                    account_chart["account"] == row["counter_account"], "vat_code"
                ].values[0] if pd.notna(row["counter_account"]) else None
            )
            if pd.isna(account_vat_code) and pd.isna(counter_vat_code):
                self._logger.warning(
                    f"Skip vat code '{row['vat_code']}' for {row['id']}: Neither account nor "
                    f"counter account have a vat_code."
                )
            elif pd.isna(account_vat_code) and pd.notna(counter_vat_code):
                multiplier = 1.0
                account = (row["counter_account"] if vat["inclusive"] else row["account"])
            elif pd.notna(account_vat_code) and pd.isna(counter_vat_code):
                multiplier = -1.0
                account = (row["account"] if vat["inclusive"] else row["counter_account"])
            else:
                self._logger.warning(
                    f"Skip vat code '{row['vat_code']}' for {row['id']}: Both account and "
                    f"counter accounts have vat_codes."
                )

            # Calculate VAT amount
            if vat["inclusive"]:
                amount = row["amount"] * vat["rate"] / (1 + vat["rate"])
            else:
                amount = row["amount"] * vat["rate"]
            amount = amount * multiplier
            amount = self.round_to_precision(amount, row["currency"])

            # Create a new journal entry for the VAT amount
            if amount != 0:
                base_entry = {
                    "date": row["date"],
                    "text": "VAT: " + row["text"],
                    "account": account,
                    "document": row["document"],
                    "currency": row["currency"],
                    "base_currency_amount": np.nan,
                    "vat_code": row["vat_code"]
                }
                if pd.notna(vat["account"]):
                    vat_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:vat",
                        "counter_account": vat["account"],
                        "amount": amount
                    })
                if pd.notna(vat["inverse_account"]):
                    vat_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:vat",
                        "counter_account": vat["inverse_account"],
                        "amount": -1 * amount
                    })

        # Return a DataFrame
        if len(vat_journal_entries) > 0:
            result = pd.DataFrame(vat_journal_entries)
        else:
            # Empty DataFrame with identical structure
            cols = {**REQUIRED_LEDGER_COLUMNS, **OPTIONAL_LEDGER_COLUMNS}
            result = pd.DataFrame(columns=cols)
        return result

    # ----------------------------------------------------------------------
    # Account chart

    def _single_account_balance(self, account: int, date: datetime.date = None) -> dict:
        return self._balance_from_serialized_ledger(account=account, date=date)

    def account_chart(self) -> pd.DataFrame:
        return self._account_chart

    def add_account(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_account is not implemented yet.")

    def modify_account(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_account is not implemented yet.")

    def delete_account(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_account is not implemented yet.")

    def mirror_account_chart(self, *args, **kwargs) -> None:
        raise NotImplementedError("mirror_account_chart is not implemented yet.")

    def validate_accounts(self) -> None:
        """Validate coherence between account, VAT and FX adjustment definitions."""
        # Ensure all vat code accounts are defined in account chart
        vat_codes = set(self._vat_codes["id"])
        missing = set(self._account_chart["vat_code"].dropna()) - vat_codes
        if len(missing) > 0:
            raise ValueError(f"Some VAT codes in account chart not defined: {missing}.")

        # Ensure all account vat_codes are defined in vat_codes
        accounts = set(self._account_chart["account"])
        missing = set(self._vat_codes["account"].dropna()) - accounts
        if len(missing) > 0:
            raise ValueError(
                f"Some accounts in VAT code definitions are not defined in the account chart: "
                f"{missing}."
            )

        # Ensure all credit and debit accounts in fx_adjustments are defined
        # in account chart
        df = self.fx_adjustments()
        missing = (set(df["credit"]) | set(df["debit"])) - accounts
        if len(missing) > 0:
            raise ValueError(
                f"Some accounts in FX adjustment definitions are not defined in the account chart: "
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

    def delete_ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_ledger_entry is not implemented yet.")

    def ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("ledger_entry is not implemented yet.")

    def modify_ledger_entry(self, *args, **kwargs) -> None:
        raise NotImplementedError("modify_ledger_entry is not implemented yet.")

    def mirror_ledger(self, *args, **kwargs) -> None:
        raise NotImplementedError("mirror_ledger is not implemented yet.")

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

        # Add automated journal entries for VAT
        vat = self.vat_journal_entries(df)
        if vat.shape[0] > 0:
            df = pd.concat([df, vat])

        # Insert missing base currency amounts
        index = df["base_currency_amount"].isna()
        df.loc[index, "base_currency_amount"] = self._base_currency_amount(
            amount=df.loc[index, "amount"],
            currency=df.loc[index, "currency"],
            date=df.loc[index, "date"]
        )

        # FX adjustments
        adjustment = self.fx_adjustments()
        base_currency = self.base_currency
        for row in adjustment.to_dict("records"):
            self._serialized_ledger = self.serialize_ledger(df)
            date = row["date"]
            accounts = self.account_range(row["adjust"])
            accounts = set(accounts["add"]) - set(accounts["subtract"])
            adjustments = []
            for account in accounts:
                currency = self.account_currency(account)
                if currency != base_currency:
                    balance = self.account_balance(account, date=date)
                    fx_rate = self.price(currency, date=date, currency=base_currency)
                    if fx_rate[0] != base_currency:
                        raise ValueError(
                            f"FX rate currency mismatch: expected {base_currency}, got {fx_rate[0]}"
                        )
                    target = balance[currency] * fx_rate[1]
                    amount = target - balance["base_currency"]
                    amount = self.round_to_precision(amount, ticker=base_currency, date=date)
                    id = f"fx_adjustment:{date}:{account}"
                    adjustments.append({
                        "id": id,
                        "date": date,
                        "account": account,
                        "currency": currency,
                        "amount": 0,
                        "base_currency_amount": amount,
                        "text": row["text"]
                    })
                    adjustments.append({
                        "id": id,
                        "date": date,
                        "account": row["credit"] if amount > 0 else row["debit"],
                        "currency": base_currency,
                        "amount": -1 * amount,
                        "base_currency_amount": -1 * amount,
                        "text": row["text"]
                    })
            if len(adjustments) > 0:
                adjustments = self.standardize_ledger_columns(pd.DataFrame(adjustments))
                df = pd.concat([df, adjustments])

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
        cols = ["amount", "base_currency_amount", "currency"]
        if rows.sum() == 0:
            result = {"base_currency": 0.0}
            currency = self.account_currency(account)
            if currency is not None:
                result[currency] = 0.0
        else:
            sub = df.loc[rows, cols]
            base_amount = sub["base_currency_amount"].sum()
            amount = sub.groupby("currency").agg({"amount": "sum"})
            amount = {currency: amount for currency, amount in zip(amount.index, amount["amount"])}
            result = {"base_currency": base_amount} | amount
        return result

    # ----------------------------------------------------------------------
    # Currency

    @property
    def base_currency(self) -> str:
        return self._settings["base_currency"]

    def _base_currency_amount(
        self, amount: list[float], currency: list[str], date: list[datetime.date]
    ) -> list[float]:
        base_currency = self.base_currency
        if not (len(amount) == len(currency) == len(date)):
            raise ValueError("Vectors 'amount', 'currency', and 'date' must have the same length.")
        result = [
            self.round_to_precision(
                a * self.price(t, date=d, currency=base_currency)[1],
                base_currency, date=d)
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
        return self._settings["precision"][ticker]

    def add_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("add_price is not implemented yet.")

    def delete_price(self, *args, **kwargs) -> None:
        raise NotImplementedError("delete_price is not implemented yet.")

    def price_history(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_history is not implemented yet.")

    def price_increment(self, *args, **kwargs) -> None:
        raise NotImplementedError("price_increment is not implemented yet.")
