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
    def ledger(self):
        pass

    def test_account_accessor_mutators(self, ledger, ignore_row_order=False):
        tax_accounts = self.ACCOUNTS.query("account in [4000, 5000]")
        ledger.restore(tax_codes=self.TAX_CODES, accounts=tax_accounts, settings=self.SETTINGS)

        # Add accounts
        accounts = self.ACCOUNTS.query("account not in [4000, 5000]").sample(frac=1)
        for account in accounts.to_dict('records'):
            ledger.accounts.add([account])
        accounts = pd.concat([tax_accounts, accounts], ignore_index=True)
        assert_frame_equal(
            ledger.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify accounts
        rows = [0, 3, len(accounts) - 1]
        for i in rows:
            accounts.loc[i, "description"] = f"New Description: {i}"
            ledger.accounts.modify([accounts.loc[i]])
            assert_frame_equal(
                ledger.accounts.list(), accounts,
                check_like=True, ignore_row_order=ignore_row_order
            )

        # Modify method receive only one needed field to modify
        rows = [0, 3, len(accounts) - 1]
        for i in rows:
            accounts.loc[i, "description"] = f"Modify Description: {i}"
            ledger.accounts.modify({
                "account": [accounts.loc[i, "account"]],
                "description": [f"Modify Description: {i}"]
            })
            assert_frame_equal(
                ledger.accounts.list(), accounts,
                check_like=True, ignore_row_order=ignore_row_order
            )

        # Delete accounts
        ledger.accounts.delete([{"account": accounts['account'].iloc[rows[0]]}])
        accounts = accounts.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.accounts.list(), accounts, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_already_existed_raise_error(
        self, ledger, error_class=ValueError, error_message="already exist"
    ):
        new_account = {
            "account": 77777,
            "currency": "CHF",
            "description": "test account",
            "tax_code": None,
            "group": "/Assets",
        }
        ledger.accounts.add([new_account])
        with pytest.raises(error_class, match=error_message):
            ledger.accounts.add([new_account])

    def test_modify_non_existed_raise_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.accounts.modify([{
                "account": 77777,
                "currency": "CHF",
                "description": "test account",
                "tax_code": None,
                "group": "/Assets",
            }])

    def test_delete_accounts_allow_missing(
        self, ledger, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.accounts.delete([{
                "account": 77777, "currency": "CHF", "description": "test account"
            }], allow_missing=False)
        ledger.accounts.delete([{
            "account": 77777, "currency": "CHF", "description": "test account"
        }], allow_missing=True)

    def test_mirror_accounts(self, ledger):
        ledger.restore(settings=self.SETTINGS)
        target = pd.concat([self.ACCOUNTS, ledger.accounts.list()], ignore_index=True)
        original_target = target.copy()
        ledger.accounts.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(target, ledger.accounts.list(), ignore_row_order=True, check_like=True)

        # Mirror with delete=False shouldn't change the data
        target = self.ACCOUNTS.query("account not in [1000, 1005]")
        ledger.accounts.mirror(target, delete=False)
        assert_frame_equal(
            original_target, ledger.accounts.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        ledger.accounts.mirror(target, delete=True)
        assert_frame_equal(target, ledger.accounts.list(), ignore_row_order=True, check_like=True)

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == "2000", "description"] = "Test mirror description"
        ledger.accounts.mirror(target, delete=True)
        assert_frame_equal(target, ledger.accounts.list(), ignore_row_order=True, check_like=True)

    def test_mirror_empty_accounts(self, ledger):
        ledger.restore(accounts=self.ACCOUNTS, settings=self.SETTINGS)
        assert not ledger.accounts.list().empty, "Accounts were not populated"
        ledger.accounts.mirror(ledger.accounts.standardize(None), delete=True)
        assert ledger.accounts.list().empty, "Mirroring empty df should erase all accounts"

    def test_account_balance(self, ledger):
        ledger.restore(
            accounts=self.ACCOUNTS, settings=self.SETTINGS, tax_codes=self.TAX_CODES,
            ledger=self.LEDGER_ENTRIES, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS
        )
        for _, row in self.EXPECTED_BALANCE.iterrows():
            date = datetime.datetime.strptime(row['date'], "%Y-%m-%d").date()
            account = row['account']
            expected = row['balance']
            actual = ledger.account_balance(date=date, account=row['account'])
            assert expected == actual, (
                f"Account balance for {account} on {date} of {actual} differs from {expected}."
            )
