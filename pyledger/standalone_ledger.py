"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import datetime
import zipfile
import numpy as np
import pandas as pd
from pyledger.storage_entity import AccountingEntity
from pyledger.time import parse_date_span
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

    def dump_to_zip(self, archive_path: str):
        """Extend dump_to_zip to include reconciliation and target balance data in the archive."""
        super().dump_to_zip(archive_path)
        with zipfile.ZipFile(archive_path, 'a') as archive:
            archive.writestr('reconciliation.csv', self.reconciliation.list().to_csv(index=False))
            archive.writestr('target_balance.csv', self.target_balance.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        """Extend restore_from_zip to also restore reconciliation and target balance data."""
        super().restore_from_zip(archive_path)
        with zipfile.ZipFile(archive_path, 'r') as archive:
            if 'reconciliation.csv' in archive.namelist():
                self.restore(reconciliation=pd.read_csv(archive.open('reconciliation.csv')))
            if 'target_balance.csv' in archive.namelist():
                self.restore(target_balance=pd.read_csv(archive.open('target_balance.csv')))

    def restore(
        self, *args, reconciliation: pd.DataFrame | None = None,
        target_balance: pd.DataFrame | None = None, **kwargs
    ):
        """Extend restore to reconciliation and target balance data after base restoration."""
        super().restore(*args, **kwargs)
        if reconciliation is not None:
            self.reconciliation.mirror(reconciliation, delete=True)
        if target_balance is not None:
            self.target_balance.mirror(target_balance, delete=True)

    def clear(self):
        """Extend clear() to also delete reconciliation and target balance records."""
        super().clear()
        self.reconciliation.mirror(None, delete=True)
        self.target_balance.mirror(None, delete=True)

    # ----------------------------------------------------------------------
    # Storage entities

    @property
    def reconciliation(self) -> AccountingEntity:
        return self._reconciliation

    @property
    def target_balance(self) -> AccountingEntity:
        return self._target_balance

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
                        "amount": amount,
                        "report_amount": self.report_amount(
                            amount=[amount], currency=[row["currency"]], date=[row["date"]]
                        )[0]
                    })
                if pd.notna(tax["contra"]):
                    amount = -1 * amount
                    tax_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:tax",
                        "contra": tax["contra"],
                        "amount": amount,
                        "report_amount": self.report_amount(
                            amount=[amount], currency=[row["currency"]], date=[row["date"]]
                        )[0]
                    })
        result = enforce_schema(pd.DataFrame(tax_journal_entries), JOURNAL_SCHEMA)

        return result

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

        corrections = self.correcting_entries(df)
        if not corrections.empty:
            df = pd.concat([df, corrections], ignore_index=True)

        return df

    def correcting_entries(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Compute all ledger correction entries (target balances and revaluations),
        in historical order. Target balances are applied before revaluations on same date.

        Args:
            ledger (pd.DataFrame): The initial ledger.

        Returns:
            pd.DataFrame: Correction entries to append to the ledger.
        """
        revaluations = self.sanitize_revaluations(self.revaluations.list())
        target_balances = self.sanitize_target_balance(self.target_balance.list())
        dates = pd.Series(
            list(revaluations["date"]) + list(target_balances["date"])
        ).dropna().drop_duplicates().sort_values()
        result = []

        for date in dates:
            combined = pd.concat([ledger] + result)

            # Calculate target balance entries
            target_balance_rows = target_balances.query("date == @date")
            if not target_balance_rows.empty:
                target_balance_entries = self.target_balance_entries(
                    ledger=combined,
                    target_balance=target_balance_rows,
                )
                result.append(target_balance_entries)
            else:
                target_balance_entries = None

            # Calculate revaluation entries
            revaluation_rows = revaluations.query("date == @date")
            if not revaluation_rows.empty:
                revaluation_ledger = (
                    pd.concat([combined, target_balance_entries])
                    if target_balance_entries is not None else combined
                )
                revaluation_entries = self.revaluation_entries(
                    ledger=revaluation_ledger,
                    revaluations=revaluation_rows,
                )
                result.append(revaluation_entries)

        return (
            pd.concat(result, ignore_index=True)
            if result else pd.DataFrame(columns=ledger.columns)
        )

    def target_balance_entries(
        self, ledger: pd.DataFrame, target_balance: pd.DataFrame
    ) -> pd.DataFrame:
        """Compute ledger entries to enforce target balances."""
        result = []
        reporting_currency = self.reporting_currency

        for row in target_balance.to_dict("records"):
            entries = self.journal.standardize(pd.DataFrame(result))
            df = self.serialize_ledger(pd.concat([ledger, entries]))
            if pd.isna(row["lookup_profit_centers"]):
                profit_centers = None
            else:
                profit_centers = self.parse_profit_centers(row["lookup_profit_centers"])
            balance = self._account_balance(
                ledger=df, account=row["lookup_accounts"],
                period=row["lookup_period"], profit_centers=profit_centers
            )
            current = balance.get("reporting_currency", 0.0)
            delta = row["balance"] - current
            delta = self.round_to_precision(delta, ticker=reporting_currency, date=row["date"])
            report_delta = self.report_amount(
                amount=[delta], currency=[reporting_currency], date=[row["date"]]
            )[0]

            if delta != 0:
                entry_id = (
                    f"target_balance:{row['lookup_period']}:{row['lookup_accounts']}:"
                    f"{row['lookup_profit_centers']}"
                )
                base_entry = {
                    "id": entry_id,
                    "date": row["date"],
                    "currency": reporting_currency,
                    "description": row["description"],
                    "document": row["document"],
                }
                result.append({
                    **base_entry,
                    "account": row["account"],
                    "contra": row["contra"],
                    "amount": delta,
                    "report_amount": report_delta,
                })
                result.append({
                    **base_entry,
                    "account": row["contra"],
                    "contra": row["account"],
                    "amount": delta,
                    "report_amount": -report_delta,
                })

        return self.journal.standardize(pd.DataFrame(result))

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
                    balance = self._account_balance(ledger=df, account=account, period=date)
                    fx_rate = self.price(currency, date=date, currency=reporting_currency)
                    if fx_rate[0] != reporting_currency:
                        raise ValueError(
                            f"FX rate currency mismatch: expected {reporting_currency}, got "
                            f"{fx_rate[0]}"
                        )
                    target = balance.get(currency, 0.0) * fx_rate[1]
                    amount = target - balance.get("reporting_currency", 0.0)
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

    def _account_balance(
        self, account: str | int | dict | list, ledger: pd.DataFrame = None,
        profit_centers: list[str] | str = None, period: datetime.date | str = None,
    ) -> dict:
        """Compute the balance of one or more accounts from the serialized ledger.

        Args:
            ledger (pd.DataFrame, optional): Ledger entries to compute balance from.
                If None, defaults to the result of `self.serialized_ledger()`.
            account (int, str, dict): The account(s) to be evaluated. Can be a
                a single account, e.g. 1020, a sequence of accounts separated
                by a column, e.g. "1000:1999", in which case the combined
                balance of all accounts within that range is returned. Multiple
                accounts and/or account sequences can be separated by a plus or
                minus sign, e.g. "1000+1020:1025", in which case the combined
                balance of all accounts is returned, or "1020:1025-1000", in
                which case the balance of account 1000 is subtracted from the
                combined balance of accounts 1020:1025.
            period (datetime.date, str, int, optional): The period for which or
                date as of which the account balance is calculated. Periods can
                be defined as string, e.g. "2024" (the year 2024), "2024-01"
                (January 2024), "2024-Q1" (first quarter 2024), or as a tuple
                with start and end date. Defaults to None.
            profit_centers: (list[str], str): Filter for journal entries. If
                not None, the result is calculated only from journal entries
                assigned to one of the profit centers in the filter.

        Returns:
            dict: Dictionary containing the balance of the account(s) in all
                currencies, in which transactions were recorded plus in
                "reporting_currency". Keys denote currencies and values the
                balance amounts in each currency.
        """
        if ledger is None:
            ledger = self.serialized_ledger()

        multipliers = self.account_multipliers(self.parse_account_range(account))
        multipliers = pd.DataFrame(list(multipliers.items()), columns=["account", "multiplier"])
        rows = ledger["account"].isin(multipliers["account"])

        if profit_centers is not None and profit_centers is not pd.NA:
            if isinstance(profit_centers, str):
                profit_centers = [profit_centers]
            valid_profit_centers = set(self.profit_centers.list()["profit_center"])
            invalid_profit_centers = set(profit_centers) - valid_profit_centers
            if invalid_profit_centers:
                raise ValueError(
                    f"Profit centers: {', '.join(invalid_profit_centers)} do not exist."
                )
            rows = rows & (ledger["profit_center"].isin(profit_centers))
        start, end = parse_date_span(period)
        if start is not None:
            rows = rows & (ledger["date"] >= pd.Timestamp(start))
        if end is not None:
            rows = rows & (ledger["date"] <= pd.Timestamp(end))

        if rows.sum() == 0:
            return {"reporting_currency": 0.0}

        sub = ledger.loc[rows, ["account", "amount", "report_amount", "currency"]]
        sub = sub.merge(multipliers, on="account", how="inner")
        sub["amount"] *= sub["multiplier"]
        sub["report_amount"] *= sub["multiplier"]

        account_currencies = sub["account"].map(lambda acc: self.account_currency(acc))
        sub.loc[account_currencies == self.reporting_currency, "amount"] = sub["report_amount"]
        sub["currency"] = account_currencies.combine_first(sub["currency"])

        grouped = sub.groupby("currency", sort=False)["amount"].sum().reset_index()
        rounded_amounts = dict(zip(
            grouped["currency"],
            self.round_to_precision(grouped["amount"], grouped["currency"], end)
        ))
        report_total = self.round_to_precision(
            [sub["report_amount"].sum()], [self.reporting_currency], end
        )[0]

        return {
            "reporting_currency": report_total,
            **rounded_amounts
        }
