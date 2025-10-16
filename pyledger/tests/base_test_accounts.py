"""Definition of abstract base class for testing accounts operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal, enforce_schema
from pyledger.constants import ACCOUNT_BALANCE_SCHEMA, AGGREGATED_BALANCE_SCHEMA
from .base_test import BaseTest
from io import StringIO
from pyledger.constants import ACCOUNT_HISTORY_SCHEMA, JOURNAL_SCHEMA


class BaseTestAccounts(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    @pytest.fixture()
    def restored_engine(self, engine):
        """Accounting engine populated with tax codes, accounts, and configuration"""
        tax_accounts = pd.concat([
            self.TAX_CODES["account"], self.TAX_CODES["contra"]
        ]).dropna().unique()
        tax_accounts = self.ACCOUNTS.query("`account` in @tax_accounts")
        engine.restore(accounts=tax_accounts, tax_codes=self.TAX_CODES,
                       configuration=self.CONFIGURATION, assets=self.ASSETS)
        return engine

    def test_account_accessor_mutators(self, restored_engine, ignore_row_order=False):
        engine = restored_engine
        existing = engine.accounts.list()
        new_accounts = self.ACCOUNTS.query("`account` not in @existing['account']")
        new_accounts = new_accounts.sample(frac=1).reset_index(drop=True)

        # Add accounts one by one and with multiple rows
        for account in new_accounts.head(-3).to_dict('records'):
            engine.accounts.add([account])
        engine.accounts.add(new_accounts.tail(3))
        accounts = pd.concat([existing, new_accounts], ignore_index=True)
        assert_frame_equal(
            engine.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        accounts.loc[0, "description"] = "Modify only one column test"
        engine.accounts.modify([{
            "account": accounts.loc[0, "account"],
            "description": "Modify only one column test"
        }])
        assert_frame_equal(
            engine.accounts.list(), accounts,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        accounts.loc[3, "description"] = "Modify with all columns test"
        engine.accounts.modify([accounts.loc[3]])
        assert_frame_equal(
            engine.accounts.list(), accounts,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify multiple rows
        accounts.loc[accounts.index[[1, -1]], "description"] = "Modify multiple rows"
        engine.accounts.modify(accounts.loc[accounts.index[[1, -1]]])
        assert_frame_equal(
            engine.accounts.list(), accounts,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Get list of accounts that are assigned to a Tax Code
        tax_accounts = pd.concat([  # noqa: F841
            self.TAX_CODES["account"], self.TAX_CODES["contra"]
        ]).dropna().unique()

        # Delete a single row
        to_delete = accounts.query("`account` not in @tax_accounts")['account'].iloc[0]
        engine.accounts.delete([{"account": to_delete}])
        accounts = accounts.query("`account` != @to_delete").reset_index(drop=True)
        assert_frame_equal(
            engine.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        to_delete = accounts.query("`account` not in @tax_accounts")['account'].iloc[[1, -1]]
        engine.accounts.delete(to_delete)
        accounts = accounts.query("`account` not in @to_delete").reset_index(drop=True)
        assert_frame_equal(
            engine.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_account_raise_error(
        self, restored_engine, error_class=ValueError, error_message="already exist"
    ):
        new_account = {"account": 77777, "currency": "CHF", "description": "test account"}
        restored_engine.accounts.add([new_account])
        with pytest.raises(error_class, match=error_message):
            restored_engine.accounts.add([new_account])

    def test_modify_nonexistent_account_raise_error(
        self, restored_engine, error_class=ValueError,
        error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            restored_engine.accounts.modify([{
                "account": 77777, "currency": "CHF", "description": "test"
            }])

    def test_delete_account_allow_missing(
        self, restored_engine, error_class=ValueError,
        error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            restored_engine.accounts.delete([{"account": 77777}], allow_missing=False)
        restored_engine.accounts.delete([{"account": 77777}], allow_missing=True)

    def test_mirror_accounts(self, restored_engine):
        engine = restored_engine
        target = pd.concat(
            [self.ACCOUNTS, engine.accounts.list()], ignore_index=True
        ).drop_duplicates(["account"])
        original_target = target.copy()
        engine.accounts.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(target, engine.accounts.list(), ignore_row_order=True, check_like=True)

        # Mirror with delete=False shouldn't change the data
        target = self.ACCOUNTS.query("account not in [1000, 1005]")
        engine.accounts.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.accounts.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.accounts.mirror(target, delete=True)
        assert_frame_equal(target, engine.accounts.list(), ignore_row_order=True, check_like=True)

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == 2000, "description"] = "Test mirror description"
        target.loc[target["account"] == 5000, "tax_code"] = pd.NA
        engine.accounts.mirror(target, delete=True)
        assert_frame_equal(target, engine.accounts.list(), ignore_row_order=True, check_like=True)

    def test_mirror_empty_accounts(self, restored_engine):
        restored_engine.restore(
            accounts=self.ACCOUNTS.assign(tax_code=pd.NA),
            configuration=self.CONFIGURATION, tax_codes=[]
        )
        assert not restored_engine.accounts.list().empty, "Accounts were not populated"
        restored_engine.accounts.mirror(restored_engine.accounts.standardize(None), delete=True)
        assert restored_engine.accounts.list().empty, (
            "Mirroring empty df should erase all accounts"
        )

    def test_account_balance(self, restored_engine):
        restored_engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS,
            target_balance=self.TARGET_BALANCE
        )
        columns_to_drop = ["period", "account", "profit_center"]

        def drop_zero_balances(balance_dict):
            return {k: v for k, v in balance_dict.items() if v != 0.0}

        # Test account balance with specified profit centers
        expected_with_pc = self.EXPECTED_BALANCES.query("profit_center.notna()")
        balances = restored_engine.account_balances(expected_with_pc)
        expected_with_pc = expected_with_pc.drop(columns=columns_to_drop)
        expected_with_pc["balance"] = expected_with_pc["balance"].apply(drop_zero_balances)
        balances["balance"] = balances["balance"].apply(drop_zero_balances)
        assert_frame_equal(balances, expected_with_pc, ignore_index=True, check_like=True)

        # Test account balance without specified profit centers
        expected_without_pc = self.EXPECTED_BALANCES.query("profit_center.isna()")
        balances = restored_engine.account_balances(expected_without_pc)
        expected_without_pc = expected_without_pc.drop(columns=columns_to_drop)
        expected_without_pc["balance"] = expected_without_pc["balance"].apply(drop_zero_balances)
        balances["balance"] = balances["balance"].apply(drop_zero_balances)
        assert_frame_equal(balances, expected_without_pc, ignore_index=True, check_like=True)

    def test_individual_account_balances(self, restored_engine):
        restored_engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS
        )

        # Extract unique test cases
        df = self.EXPECTED_INDIVIDUAL_BALANCES.copy()
        argument_cols = ["period", "accounts", "profit_center"]
        df[argument_cols] = df[argument_cols].ffill()
        cases = df.drop_duplicates(subset=argument_cols).sort_values("period")

        # Test account balances with specified profit centers
        cases_with_profit_centers = cases.query("profit_center.notna()")[argument_cols]
        for period, accounts, profit_centers in cases_with_profit_centers.itertuples(index=False):
            expected = df.query(
                "period == @period and accounts == @accounts and profit_center == @profit_centers"
            ).drop(columns=argument_cols)
            expected = enforce_schema(expected, ACCOUNT_BALANCE_SCHEMA)
            profit_centers = [pc.strip() for pc in profit_centers.split(",")]
            actual = restored_engine.individual_account_balances(
                period=period, accounts=accounts, profit_centers=profit_centers
            )
            assert_frame_equal(expected, actual, ignore_index=True, check_like=True)

        # Test account balances without specified profit centers
        cases_without_profit_centers = cases.query("profit_center.isna()")[argument_cols]
        for period, accounts, _ in cases_without_profit_centers.itertuples(index=False):
            expected = df.query(
                "period == @period and accounts == @accounts and profit_center.isna()"
            ).drop(columns=argument_cols)
            expected = enforce_schema(expected, ACCOUNT_BALANCE_SCHEMA)
            actual = restored_engine.individual_account_balances(period=period, accounts=accounts)
            assert_frame_equal(expected, actual, ignore_index=True, check_like=True)

    def test_aggregate_account_balances(self, restored_engine):
        restored_engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS
        )

        account_balances = restored_engine.individual_account_balances(
            period="2024", accounts="1000:9999"
        )
        actual = restored_engine.aggregate_account_balances(account_balances, n=2)
        expected = enforce_schema(self.EXPECTED_AGGREGATED_BALANCES, AGGREGATED_BALANCE_SCHEMA)
        assert_frame_equal(actual, expected, ignore_index=True)

    def test_account_history_schema_extends_journal_schema(self):
        extra_cols = ["balance", "report_balance"]  # noqa: F841
        account_history_schema = ACCOUNT_HISTORY_SCHEMA.query("column not in @extra_cols")
        assert_frame_equal(
            JOURNAL_SCHEMA, account_history_schema, ignore_columns=["mandatory"], ignore_index=True
        )

    def test_account_history(self, restored_engine):
        restored_engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS
        )

        def format_expected_df(df_str: str, drop: bool = False) -> pd.DataFrame:
            """Convert expected account history CSV string into a properly formatted DataFrame."""
            df = pd.read_csv(StringIO(df_str), skipinitialspace=True)
            df = enforce_schema(df, schema=ACCOUNT_HISTORY_SCHEMA)
            if drop:
                mandatory = ACCOUNT_HISTORY_SCHEMA.query("mandatory == True")["column"].tolist()
                drop = [col for col in df.columns.difference(mandatory) if df[col].isna().all()]
                df = df.drop(columns=drop)
            return df

        # Test cases with profit centers
        for case in filter(lambda c: c["profit_centers"] is not None, self.EXPECTED_HISTORY):
            df = restored_engine.account_history(
                account=case["account"], period=case["period"],
                profit_centers=case["profit_centers"], drop=case["drop"],
                opening_balance=case.get("opening_balance", False)
            )
            expected_df = format_expected_df(case["account_history"], drop=case["drop"])
            assert_frame_equal(df, expected_df, check_like=True, ignore_columns=["id"])

        # Test cases without profit centers
        JOURNAL = self.JOURNAL.copy().assign(profit_center=pd.NA)
        restored_engine.restore(profit_centers=[], journal=JOURNAL)
        for case in filter(lambda c: c["profit_centers"] is None, self.EXPECTED_HISTORY):
            df = restored_engine.account_history(
                account=case["account"], period=case["period"], drop=case["drop"],
                opening_balance=case.get("opening_balance", False)
            )
            expected_df = format_expected_df(case["account_history"], drop=case["drop"])
            assert_frame_equal(df, expected_df, check_like=True, ignore_columns=["id"])

    def test_account_history_entry_in_both_accounts(self, engine):
        """This test verifies that transactions are correctly reflected in both the main
        account and the contra account, as expected from a double-entry bookkeeping system.

        See: #193 — related issue discussing missing entries in contra account history.
        """

        ACCOUNT_CSV = """
            group,                account, currency, tax_code, description
            /Revenue/Sales,          2200,      USD,         , VAT Payable (Output VAT)
            /Revenue/Sales,          4001,      USD,         , Sales Revenue - EUR
        """
        ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
        accounts = pd.concat(
            [ACCOUNTS, engine.accounts.list()], ignore_index=True
        ).drop_duplicates(["account"])
        JOURNAL_CSV = """
        id,       date, account, contra, currency,    amount, report_amount, tax_code, description,
        1, 2024-07-01,    4001,   2200,      USD,       100,              ,         ,    2024 txn,
        """
        JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)
        engine.restore(
            configuration={"REPORTING_CURRENCY": "USD"}, accounts=accounts, journal=JOURNAL
        )

        assert not engine.account_history(2200).empty, (
            "No entries found in account history for account 2200 (the contra account)."
        )
        assert not engine.account_history(4001).empty, (
            "No entries found in account history for account 4001."
        )
