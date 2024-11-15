"""Definition of abstract base class for testing accounts operations."""

import pytest
import pandas as pd
import datetime
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestAccounts(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_account_accessor_mutators(self, engine, ignore_row_order=False):
        ACCOUNTS = self.ACCOUNTS.query("account not in [4000, 5000]")
        tax_accounts = self.ACCOUNTS.query("account in [4000, 5000]")
        engine.restore(tax_codes=self.TAX_CODES, accounts=tax_accounts, settings=self.SETTINGS)

        # Add accounts one by one and with multiple rows
        for account in ACCOUNTS.head(-3).to_dict('records'):
            engine.accounts.add([account])
        engine.accounts.add(ACCOUNTS.tail(3))
        accounts = pd.concat([tax_accounts, ACCOUNTS], ignore_index=True)
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

        # Modify with a multiple rows
        accounts.loc[accounts.index[[1, -1]], "description"] = "Modify multiple rows"
        engine.accounts.modify(accounts.loc[accounts.index[[1, -1]]])
        assert_frame_equal(
            engine.accounts.list(), accounts,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.accounts.delete([{"account": accounts['account'].iloc[0]}])
        accounts = accounts.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.accounts.delete(accounts.iloc[[1, -1]])
        accounts = accounts.drop(accounts.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_account_raise_error(
        self, engine, error_class=ValueError, error_message="already exist"
    ):
        new_account = {"account": 77777, "currency": "CHF", "description": "test account"}
        engine.accounts.add([new_account])
        with pytest.raises(error_class, match=error_message):
            engine.accounts.add([new_account])

    def test_modify_nonexistent_account_raise_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.accounts.modify([{
                "account": 77777, "currency": "CHF", "description": "test account"
            }])

    def test_delete_account_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.accounts.delete([{
                "account": 77777, "currency": "CHF", "description": "test account"
            }], allow_missing=False)
        engine.accounts.delete([{
            "account": 77777, "currency": "CHF", "description": "test account"
        }], allow_missing=True)

    def test_mirror_accounts(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.ACCOUNTS, engine.accounts.list()], ignore_index=True)
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
        target.loc[target["account"] == "2000", "description"] = "Test mirror description"
        engine.accounts.mirror(target, delete=True)
        assert_frame_equal(target, engine.accounts.list(), ignore_row_order=True, check_like=True)

    def test_mirror_empty_accounts(self, engine):
        engine.restore(accounts=self.ACCOUNTS, settings=self.SETTINGS)
        assert not engine.accounts.list().empty, "Accounts were not populated"
        engine.accounts.mirror(engine.accounts.standardize(None), delete=True)
        assert engine.accounts.list().empty, "Mirroring empty df should erase all accounts"

    def test_account_balance(self, engine):
        engine.restore(
            accounts=self.ACCOUNTS, settings=self.SETTINGS, tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS
        )
        for _, row in self.EXPECTED_BALANCE.iterrows():
            date = datetime.datetime.strptime(row['date'], "%Y-%m-%d").date()
            account = row['account']
            expected = row['balance']
            actual = engine.account_balance(date=date, account=row['account'])
            assert expected == actual, (
                f"Account balance for {account} on {date} of {actual} differs from {expected}."
            )
