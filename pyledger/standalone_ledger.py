"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import datetime
import numpy as np
import pandas as pd
from .decorators import timed_cache
from .constants import JOURNAL_SCHEMA
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

    def tax_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create journal entries to book tax according to tax_codes.

        Iterates through the provided DataFrame and calculates tax for entries
        that have a non-null tax_code. It generates a new journal entry for
        each tax account.

        Args:
            df (pd.DataFrame): A pandas DataFrame containing journal entries.

        Returns:
            pd.DataFrame: A new DataFrame with tax journal entries.
            Returns empty DataFrame with the correct structure if no tax codes are present.
        """
        tax_definitions = self.tax_codes.list().set_index("id").to_dict("index")
        tax_journal_entries = []
        accounts = self.accounts.list()
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
                    "tax_code": row["tax_code"],
                    "profit_center": row["profit_center"]
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
        result = enforce_schema(pd.DataFrame(tax_journal_entries), JOURNAL_SCHEMA)

        return result

    # ----------------------------------------------------------------------
    # Accounts

    def _single_account_balance(
        self, account: int, profit_centers: list[str] | str = None,
        start: datetime.date = None, end: datetime.date = None,
    ) -> dict:
        return self._balance_from_serialized_ledger(
            self.serialized_ledger(), account=account, profit_centers=profit_centers,
            start=start, end=end,
        )

    # ----------------------------------------------------------------------
    # Journal

    @timed_cache(120)
    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with all ledger transactions in long format.

        Returns:
            pd.DataFrame: Combined DataFrame with ledger data.
        """
        return self.serialize_ledger(self.complete_ledger(self.journal.list()))

    def complete_ledger(self, journal=None) -> pd.DataFrame:
        # Journal definition
        df = self.journal.standardize(journal)
        df = self.sanitize_journal(df)
        df = df.sort_values(["date", "id"])

        # Add ledger entries for tax
        df = pd.concat([df, self.tax_entries(df)], ignore_index=True)

        # Insert missing reporting currency amounts
        index = df["report_amount"].isna()
        df.loc[index, "report_amount"] = self.report_amount(
            amount=df.loc[index, "amount"],
            currency=df.loc[index, "currency"],
            date=df.loc[index, "date"]
        )

        # Add ledger entries for (currency or other) revaluations
        revaluations = self.sanitize_revaluations(self.revaluations.list())
        revalue = self.revaluation_entries(ledger=df, revaluations=revaluations)
        return pd.concat([df, revalue], ignore_index=True)

    def revaluation_entries(
        self, ledger: pd.DataFrame, revaluations: pd.DataFrame
    ) -> pd.DataFrame:
        """Compute ledger entries for (currency or other) revaluations"""
        result = []
        reporting_currency = self.reporting_currency
        for row in revaluations.to_dict("records"):
            revalue = self.journal.standardize(pd.DataFrame(result))
            df = self.serialize_ledger(pd.concat([ledger, revalue]))
            date = row["date"]
            accounts = self.parse_account_range(row["account"])
            accounts = set(accounts["add"]) - set(accounts["subtract"])

            for account in accounts:
                currency = self.account_currency(account)
                if currency != reporting_currency:
                    balance = self._balance_from_serialized_ledger(df, account, end=date)
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
                    if amount != 0:
                        if pd.isna(row["credit"]) and pd.isna(row["debit"]):
                            raise ValueError("No account specified in revaluation entry {id}")
                        elif pd.isna(row["credit"]):
                            revaluation_account = row["debit"]
                        elif pd.isna(row["debit"]):
                            revaluation_account = row["credit"]
                        else:
                            revaluation_account = row["credit"] if amount >= 0 else row["debit"]
                        result.append({
                            "id": id,
                            "date": date,
                            "account": account,
                            "currency": currency,
                            "amount": 0,
                            "report_amount": amount,
                            "description": row["description"]
                        })
                        result.append({
                            "id": id,
                            "date": date,
                            "account": revaluation_account,
                            "currency": reporting_currency,
                            "amount": -1 * amount,
                            "report_amount": -1 * amount,
                            "description": row["description"]
                        })

        return self.journal.standardize(pd.DataFrame(result))

    def _balance_from_serialized_ledger(
        self, ledger: pd.DataFrame, account: int, profit_centers: list[str] | str = None,
        start: datetime.date = None, end: datetime.date = None,
    ) -> dict:
        """Compute balance from serialized ledger.

        Args:
            ledger (DataFrame): General ledger in long format following JOURNAL_SCHEMA.
            account (int): The account number.
            date (datetime.date, optional): The date up to which the balance is computed.
                                            Defaults to None.
            profit_centers: (list[str], str): Filter for ledger entries. If not None, the result is
                                              calculated only from ledger entries assigned to one
                                              of the profit centers in the filter.

        Returns:
            dict: Dictionary containing the balance of the account in various currencies.
        """
        rows = ledger["account"] == int(account)
        if profit_centers is not None:
            if isinstance(profit_centers, str):
                profit_centers = [profit_centers]
            valid_profit_centers = set(self.profit_centers.list()["profit_center"])
            invalid_profit_centers = set(profit_centers) - valid_profit_centers
            if invalid_profit_centers:
                raise ValueError(
                    f"Profit centers: {', '.join(invalid_profit_centers)} do not exist."
                )
            rows = rows & (ledger["profit_center"].isin(profit_centers))
        if start is not None:
            rows = rows & (ledger["date"] >= pd.Timestamp(start))
        if end is not None:
            rows = rows & (ledger["date"] <= pd.Timestamp(end))
        cols = ["amount", "report_amount", "currency"]
        if rows.sum() == 0:
            result = {"reporting_currency": 0.0}
            currency = self.account_currency(account)
            if currency is not None:
                result[currency] = 0.0
        else:
            sub = ledger.loc[rows, cols]
            report_amount = sub["report_amount"].sum()
            account_currency = self.account_currency(account)
            if pd.isna(account_currency):
                amount = sub.groupby("currency").agg({"amount": "sum"})
                amount = {currency: amount
                          for currency, amount in zip(amount.index, amount["amount"])}
            elif account_currency == self.reporting_currency:
                amount = {self.reporting_currency: report_amount}
            elif not all(sub["currency"] == account_currency):
                raise ValueError(f"Unexpected currencies in transactions for account {account}.")
            else:
                amount = {account_currency: sub["amount"].sum()}
            result = {"reporting_currency": report_amount} | amount
        return result
