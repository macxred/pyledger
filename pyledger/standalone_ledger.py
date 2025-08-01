"""This module defines the StandaloneLedger class, a self-contained implementation of the
LedgerEngine abstract base class, designed for managing double-entry accounting systems
independently of third-party software.
"""

import datetime
import zipfile
import numpy as np
import pandas as pd
from pyledger.helpers import first_elements_as_str
from pyledger.storage_entity import AccountingEntity
from pyledger.time import parse_date_span
from .decorators import timed_cache
from .constants import JOURNAL_SCHEMA, TARGET_BALANCE_SCHEMA
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
        account_tax_map = accounts.set_index("account")["tax_code"].to_dict()

        for row in df.loc[df["tax_code"].notna()].to_dict("records"):
            tax = tax_definitions[row["tax_code"]]
            account_tax_code = account_tax_map.get(row["account"])
            contra_tax_code = account_tax_map.get(row["contra"])
            if pd.isna(account_tax_code) and pd.isna(contra_tax_code):
                self._logger.warning(
                    f"Skip tax code '{row['tax_code']}' for {row['id']}: Neither account nor "
                    f"counter account have a tax_code."
                )
            elif pd.isna(account_tax_code) and pd.notna(contra_tax_code):
                multiplier = 1.0
                account = row["contra"] if tax["is_inclusive"] else row["account"]
            elif pd.notna(account_tax_code) and pd.isna(contra_tax_code):
                multiplier = -1.0
                account = row["account"] if tax["is_inclusive"] else row["contra"]
            else:
                self._logger.warning(
                    f"Skip tax code '{row['tax_code']}' for {row['id']}: Both account and "
                    f"counter accounts have tax_code."
                )
                continue

            # Calculate tax amount
            if tax["is_inclusive"]:
                amount = row["amount"] * tax["rate"] / (1 + tax["rate"])
            else:
                amount = row["amount"] * tax["rate"]
            amount = amount * multiplier

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
                    })
                if pd.notna(tax["contra"]):
                    amount = -1 * amount
                    tax_journal_entries.append(base_entry | {
                        "id": f"{row['id']}:tax",
                        "contra": tax["contra"],
                        "amount": amount,
                    })

        # Round amounts and remove balanced entries after rounding
        result = enforce_schema(pd.DataFrame(tax_journal_entries), JOURNAL_SCHEMA)
        result["amount"] = self.round_to_precision(result["amount"], result["currency"])
        result = result.loc[result["amount"] != 0]
        result["report_amount"] = self.report_amount(
            result["amount"], result["currency"], result["date"]
        )
        return result

    # ----------------------------------------------------------------------
    # Journal

    @timed_cache(120)
    def serialized_ledger(self) -> pd.DataFrame:
        """Retrieves a DataFrame with all ledger transactions in long format.

        Returns:
            pd.DataFrame: Combined DataFrame with ledger data.
        """
        return self.accounting_ledger(
            journal=self.journal.list(), target_balances=self.target_balance.list(),
            revaluations=self.revaluations.list()
        )

    def accounting_ledger(
        self, journal: pd.DataFrame, target_balances: pd.DataFrame, revaluations: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Compute the full accounting ledger.

        Generate a fully processed ledger from sanitized journal entries, target balance
        definitions, and revaluation instructions. The journal is first converted into
        ledger entries with taxes applied. Then, revaluation and target balance entries
        are automatically and recursively generated and inserted in chronological order.

        The final result is a complete, date-ordered ledger with all adjustments applied,
        suitable for accurate financial reporting.

        Args:
            journal (pd.DataFrame): The raw journal entries.
            target_balance (pd.DataFrame): A DataFrame defining target balance rules,
                with TARGET_BALANCE_SCHEMA.
            revaluations (pd.DataFrame): A DataFrame defining revaluation rules,
                with REVALUATIONS_SCHEMA.

        Returns:
            pd.DataFrame: The completed ledger, fully adjusted and ordered by date.
        """
        df = self.journal.standardize(journal)
        df = self.sanitize_journal(df)
        df = df.sort_values(["date", "id"])

        # Add ledger entries for tax
        df = pd.concat([df, self.tax_entries(df)], ignore_index=True)

        automated_entries = self.generate_automated_entries(
            df, target_balances=target_balances, revaluations=revaluations
        )
        df = self.serialize_ledger(df)

        if not automated_entries.empty:
            df = pd.concat([df, automated_entries], ignore_index=True)

        return df

    def generate_automated_entries(
        self, ledger: pd.DataFrame, target_balances: pd.DataFrame, revaluations: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Generate all automated ledger entries, including target balance adjustments
        and currency revaluations, applied in strict historical order.

        For each unique date, this method generates:
        1. Revaluation entries to reflect updated reporting currency values for foreign
        currency balances.
        2. Target balance entries to adjust specified accounts to configured target values.

        Revaluation entries are always applied before target balance entries on the same date,
        as they may influence account balances that require revaluation. The resulting
        corrections are suitable for appending to the original ledger to achieve a
        fully adjusted view.

        Args:
            ledger (pd.DataFrame): The initial ledger with JOURNAL_SCHEMA.

        Returns:
            pd.DataFrame: A DataFrame of all generated entries, in chronological order.
        """
        revaluations = self.sanitize_revaluations(revaluations)
        target_balances = self.sanitize_target_balance(target_balances)
        dates = pd.Series(
            list(revaluations["date"]) + list(target_balances["date"])
        ).dropna().drop_duplicates().sort_values()
        result = []

        for date in dates:
            combined = pd.concat([ledger] + result)

            # Calculate revaluation entries
            revaluation_rows = revaluations.query("date == @date")
            if not revaluation_rows.empty:
                revaluation_entries = self.revaluation_entries(
                    ledger=combined, revaluations=revaluation_rows,
                )
                result.append(revaluation_entries)

            # Calculate target balance entries
            target_balance_rows = target_balances.query("date == @date")
            if not target_balance_rows.empty:
                target_balance_ledger = (
                    pd.concat([combined, revaluation_entries])
                    if not revaluation_rows.empty else combined
                )
                target_balance_entries = self.target_balance_entries(
                    ledger=target_balance_ledger, target_balance=target_balance_rows,
                )
                result.append(self.serialize_ledger(target_balance_entries))

        return (
            pd.concat(result, ignore_index=True)
            if result else pd.DataFrame(columns=ledger.columns)
        )

    def target_balance_entries(
        self, ledger: pd.DataFrame, target_balance: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Generate automated journal entries based on target balance specifications.

        In accounting, certain accounts must be periodically cleared or closed—such as
        P&L at year-end or tax accounts after filing. Defining target balance rules
        allows this process to be automated, eliminating manual closing entries and
        ensuring consistency across periods.

        All rules are processed in ascending order of their booking dates, as each
        generated entry may influence the balances of subsequent rules. For each rule,
        the method computes the current balance based on the specified lookup filters
        (e.g., account ranges, periods, profit centers). If the balance differs from
        the target, it creates journal entries that adjust the specified account to the
        desired value—typically zero—offsetting the difference to a designated
        contra account.

        Args:
            ledger (pd.DataFrame): The existing ledger with JOURNAL_SCHEMA.
            target_balance (pd.DataFrame): A DataFrame defining target balance rules,
                with TARGET_BALANCE_SCHEMA.

        Returns:
            pd.DataFrame: A DataFrame of generated journal entries that enforce
                the target balances.
        """

        result = []
        reporting_currency = self.reporting_currency
        df = self.serialize_ledger(ledger)

        for idx, row in enumerate(target_balance.to_dict("records")):
            balance = self._account_balance(
                ledger=df, account=row["lookup_accounts"],
                period=row["lookup_period"], profit_centers=row["lookup_profit_centers"]
            )
            current_balance = balance.get(row["currency"], 0.0)
            currency = (
                reporting_currency if row["currency"] == "reporting_currency" else row["currency"]
            )
            delta = row["balance"] - current_balance
            delta = self.round_to_precision(delta, ticker=currency, date=row["date"])
            report_delta = self.report_amount(
                amount=[delta], currency=[currency], date=[row["date"]]
            )[0]

            if delta == 0 and report_delta == 0:
                continue

            base_entry = {
                "id": f"target_balance:{idx}",
                "date": row["date"],
                "currency": currency,
                "description": row["description"],
                "document": row["document"],
            }
            result.append({
                **base_entry,
                "account": row["account"],
                "contra": row["contra"],
                "amount": -report_delta,
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
            df = self.serialize_ledger(pd.concat([ledger, pd.DataFrame(result)]))
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
            profit_centers = self.parse_profit_centers(profit_centers)
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

    # ----------------------------------------------------------------------
    # Target Balance

    def sanitize_target_balance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Discard incoherent target balance data.

        This method applies the following validation rules:
        1. The `balance` column must not be NA, otherwise discard the row.
        2. `lookup_period` should follow formats supported by `parse_date_span()`,
            otherwise discard the row.
        3. `lookup_accounts` should follow the format of `parse_account_range()`,
            otherwise discard the row.
        4. `lookup_profit_centers` must reference valid profit centers,
            otherwise discard the row.
        5. Discard any row that violates journal-level integrity rules
            (see `sanitize_journal()`), except for rules involving `tax_code`,
            `amount`, and `report_amount`.

        A warning is logged for each dropped entry with the specific reason.

        Args:
            df (pd.DataFrame): Target balance data to sanitize.

        Returns:
            pd.DataFrame: A sanitized DataFrame containing only valid target balance entries.
        """

        df = enforce_schema(df, TARGET_BALANCE_SCHEMA, keep_extra_columns=True)

        invalid_ids = set()
        invalid_ids = self._invalid_balance(df, invalid_ids)
        invalid_ids = self._invalid_lookup_period(df, invalid_ids)
        invalid_ids = self._invalid_accounts_range(df, invalid_ids)
        invalid_ids = self._invalid_lookup_profit_centers(df, invalid_ids)
        invalid_ids = self._invalid_accounts(df, invalid_ids)
        precision = self.precision_vectorized(df["currency"], dates=df["date"], allow_missing=True)
        invalid_ids = self._invalid_assets(df, invalid_ids, precision)
        # TODO: validate currency and price including possible "reporting_currency" value
        invalid_ids = self._invalid_profit_centers(df, invalid_ids)

        return df.query("id not in @invalid_ids").reset_index(drop=True)

    def _invalid_balance(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with missing balance values."""
        invalid_mask = df["balance"].isna()
        missing_ids = set(df.loc[invalid_mask, "id"]) - invalid_ids

        if missing_ids:
            self._logger.warning(
                f"Discarding {len(missing_ids)} target balance entries with missing 'balance': "
                f"{first_elements_as_str(missing_ids)}"
            )
            invalid_ids = invalid_ids.union(missing_ids)

        return invalid_ids

    def _invalid_lookup_period(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with invalid 'lookup_period' values."""

        def is_invalid(span: str) -> bool:
            if pd.isna(span):
                return True
            try:
                parse_date_span(span)
                return False
            except Exception:
                return True

        invalid_mask = df["lookup_period"].apply(is_invalid).astype(bool)
        new_ids = set(df.loc[invalid_mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with invalid 'lookup_period': "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids

    def _invalid_accounts_range(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with unresolvable 'lookup_accounts' values."""

        def is_invalid(val: str) -> bool:
            if pd.isna(val):
                return True
            try:
                accounts = self.parse_account_range(val)
                return (len(accounts["add"]) == 0) and (len(accounts["subtract"]) == 0)
            except Exception:
                return True

        mask = df["lookup_accounts"].apply(is_invalid).astype(bool)
        new_ids = set(df.loc[mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with unresolvable 'lookup_accounts': "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids

    def _invalid_lookup_profit_centers(self, df: pd.DataFrame, invalid_ids: set) -> set:
        """Mark target balance entries with unresolvable 'lookup_profit_centers' values."""

        valid_profit_centers = set(self.profit_centers.list()["profit_center"])

        if not valid_profit_centers:
            mask = df["lookup_profit_centers"].notna()
            reason = "assigned to a profit center, while no profit centers are defined"
        else:
            mask = (
                df["lookup_profit_centers"].notna()
                & ~df["lookup_profit_centers"].isin(valid_profit_centers)
            )
            reason = "unresolvable profit centers"

        new_ids = set(df.loc[mask, "id"]) - invalid_ids

        if new_ids:
            self._logger.warning(
                f"Discarding {len(new_ids)} entries with `{reason}`: "
                f"{first_elements_as_str(new_ids)}"
            )
            invalid_ids = invalid_ids.union(new_ids)

        return invalid_ids
