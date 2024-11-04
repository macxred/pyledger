"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import datetime
from typing import Dict
import numpy as np
import pandas as pd
from pyledger.decorators import timed_cache
from .constants import LEDGER_SCHEMA
from .ledger_engine import LedgerEngine
from consistent_df import enforce_schema


class StandaloneLedger(LedgerEngine):
    """StandaloneLedger is a self-contained implementation of the LedgerEngine class,
    that provides an abstract interface for a double-entry accounting system.
    StandaloneLedger operates autonomously and does not connect to third-party
    accounting software. It serves as a base for any standalone ledger implementation
    with a specific data storage choice.
    """

    # ----------------------------------------------------------------------
    # Tax Codes

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

    # ----------------------------------------------------------------------
    # Ledger

    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with all ledger transactions in long format.

        Returns:
            pd.DataFrame: Combined DataFrame with ledger data.
        """
        return self.complete_ledger(self.ledger())

    def complete_ledger(self, ledger=None) -> pd.DataFrame:
        # Ledger definition
        df = self.standardize_ledger(ledger)
        df = self.sanitize_ledger(df)
        df = df.sort_values(["date", "id"])

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

        # Revaluations
        revaluations = self.revaluations()
        reporting_currency = self.reporting_currency
        for row in revaluations.to_dict("records"):
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
        return self.standardize_ledger_columns(result)

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
            rows = rows & (df["date"] <= pd.Timestamp(date))
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

        if ticker not in self._prices_as_dict_of_df:
            raise ValueError(f"No price data available for '{ticker}'.")

        if currency is None:
            # Assuming the first currency is the default if none specified
            currency = next(iter(self._prices_as_dict_of_df[ticker]))

        if currency not in self._prices_as_dict_of_df[ticker]:
            raise ValueError(f"No {currency} prices available for '{ticker}'.")

        prc = self._prices_as_dict_of_df[ticker][currency]
        prc = prc.loc[prc["date"].dt.normalize() <= pd.Timestamp(date), "price"]

        if prc.empty:
            raise ValueError(f"No {currency} prices available for '{ticker}' before {date}.")

        return (currency, prc.iloc[-1].item())

    @property
    @timed_cache(15)
    def _prices_as_dict_of_df(self) -> Dict[str, pd.DataFrame]:
        """Organizes price data by ticker and currency for quick access.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: Maps each asset ticker to
            a nested dictionary of DataFrames by currency, with its
            `price` history sorted by `date` with `NaT` values first.
        """
        result = {}
        for (ticker, currency), group in self.price_history().groupby(["ticker", "currency"]):
            group = group[["date", "price"]].sort_values("date", na_position="first")
            group = group.reset_index(drop=True)
            if ticker not in result.keys():
                result[ticker] = {}
            result[ticker][currency] = group
        return result

    # ----------------------------------------------------------------------
    # Assets

    @property
    @timed_cache(15)
    def _assets_as_dict_of_df(self) -> Dict[str, pd.DataFrame]:
        """Organize assets by ticker for quick access.

        Splits assets by ticker for efficient lookup of increments by ticker
        and date.

        Returns:
            Dict[str, pd.DataFrame]: Maps each asset ticker to a DataFrame of
            its `increment` history, sorted by `date` with `NaT` values first.
        """
        return {
            ticker: (
                group[["date", "increment"]]
                .sort_values("date", na_position="first")
                .reset_index(drop=True)
            )
            for ticker, group in self.assets().groupby("ticker")
        }

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        if ticker == "reporting_currency":
            ticker = self.reporting_currency

        if date is None:
            date = datetime.date.today()
        elif not isinstance(date, datetime.date):
            date = pd.to_datetime(date).date()

        increment = self._assets_as_dict_of_df.get(ticker)
        if increment is None:
            raise ValueError(f"No asset definition available for ticker '{ticker}'.")

        mask = increment["date"].isna() | (increment["date"] <= pd.Timestamp(date))
        if not mask.any():
            raise ValueError(f"No asset definition available for '{ticker}' on or before {date}.")
        return increment.loc[mask[mask].index[-1], "increment"]
