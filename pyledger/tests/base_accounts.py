"""Definition of abstract base class for testing accounts operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestAccounts(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_account_mutators(self, ledger):
        tax_codes = self.TAX_CODES[self.TAX_CODES["id"] == "Test"]
        accounts = self.ACCOUNTS[self.ACCOUNTS["account"] == 9999]
        ledger.restore(tax_codes=tax_codes, accounts=accounts, settings=self.SETTINGS)
        new_account = {
            "account": 1145,
            "currency": "CHF",
            "description": "test create account",
            "tax_code": "Test",
            "group": "/Assets",
        }
        accounts = ledger.accounts.list()
        ledger.accounts.add([new_account])
        updated_accounts = ledger.accounts.list()
        outer_join = pd.merge(accounts, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["description"].item() == new_account["description"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert created_accounts["tax_code"].item() == "Test"
        assert created_accounts["group"].item() == new_account["group"]

        accounts = ledger.accounts.list()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "description": "test create account",
            "tax_code": None,
            "group": "/Assets",
        }
        ledger.accounts.add([new_account])
        updated_accounts = ledger.accounts.list()
        outer_join = pd.merge(accounts, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["description"].item() == new_account["description"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert pd.isna(created_accounts["tax_code"].item())
        assert created_accounts["group"].item() == new_account["group"]

        accounts = ledger.accounts.list()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "description": "test update account",
            "tax_code": "Test",
            "group": "/Assets",
        }
        ledger.accounts.modify([new_account])
        updated_accounts = ledger.accounts.list()
        outer_join = pd.merge(accounts, updated_accounts, how="outer", indicator=True)
        modified_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(modified_accounts) == 1, "Expected exactly one updated row"
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["description"].item() == new_account["description"]
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["currency"].item() == new_account["currency"]
        assert modified_accounts["tax_code"].item() == "Test"
        assert modified_accounts["group"].item() == new_account["group"]

        accounts = ledger.accounts.list()
        new_account = {
            "account": 1145,
            "currency": "USD",
            "description": "test update account without tax",
            "tax_code": None,
            "group": "/Assets",
        }
        ledger.accounts.modify([new_account])
        updated_accounts = ledger.accounts.list()
        outer_join = pd.merge(accounts, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(accounts) == len(updated_accounts), (
            "Expected accounts to have same length before and after modification"
        )
        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["description"].item() == new_account["description"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert pd.isna(created_accounts["tax_code"].item())
        assert created_accounts["group"].item() == new_account["group"]

        ledger.accounts.delete({"account": [1145]})
        ledger.accounts.delete({"account": [1146]})
        updated_accounts = ledger.accounts.list()
        assert 1145 not in updated_accounts["account"].values
        assert 1146 not in updated_accounts["account"].values

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
