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

    def test_mirror_accounts(self, ledger):
        initial_accounts = ledger.accounts.list()
        accounts = ledger.accounts.standardize(self.ACCOUNTS)
        target_df = pd.concat([accounts, initial_accounts], ignore_index=True)
        target_df = ledger.accounts.standardize(target_df)
        initial = target_df.copy()
        ledger.accounts.mirror(target_df, delete=False)
        mirrored_df = ledger.accounts.list()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True, check_like=True)
        # Mirroring should not change the initial df
        assert_frame_equal(initial, target_df, ignore_row_order=True, check_like=True)

        target_df = target_df[~target_df["account"].isin([9995, 9996])]
        ledger.accounts.mirror(target_df.copy(), delete=True)
        mirrored_df = ledger.accounts.list()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True, check_like=True)

        target_df = target_df.sample(frac=1).reset_index(drop=True)
        target_account = "2222"
        target_df.loc[target_df["account"] == target_account, "description"] = "Updated Text"
        ledger.accounts.mirror(target_df.copy(), delete=True)
        mirrored_df = ledger.accounts.list()
        assert_frame_equal(target_df, mirrored_df, ignore_row_order=True, check_like=True)

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
