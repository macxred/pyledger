"""Definition of abstract base class for testing accounts operations."""

import pytest
import pandas as pd
from abc import ABC, abstractmethod
from io import StringIO
from consistent_df import assert_frame_equal


VAT_CSV = """
    id,                 account, rate,  inclusive, text
    TestCodeAccounts,   2200,    0.02,  True,      VAT 2%
"""
ACCOUNT_CSV = """
    account, currency, text,                 vat_code,         group
    9995,    CHF,      Cash Account,         TestCodeAccounts, /Assets
    9996,    USD,      Bank Account USD,     TestCodeAccounts, /Assets
    9997,    EUR,      Bank Account EUR,     TestCodeAccounts, /Assets
    9998,    CHF,      Inventory,            ,                 /Assets
    9999,    CHF,      Accounts Receivable,  ,                 /Assets
"""
VAT_CODES = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


class BaseTestAccountCharts(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_account_mutators(self, ledger):
        initial_accounts = ledger.account_chart()
        new_account = {
            "account": 1145,
            "currency": "CHF",
            "text": "test create account",
            "vat_code": "TestCodeAccounts",
            "group": "/Assets/Anlagevermögen",
        }
        account_chart = ledger.account_chart()
        ledger.add_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(account_chart, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["text"].item() == new_account["text"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert created_accounts["vat_code"].item() == "TestCodeAccounts"
        assert created_accounts["group"].item() == new_account["group"]

        account_chart = ledger.account_chart()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "text": "test create account",
            "vat_code": None,
            "group": "/Assets/Anlagevermögen",
        }
        ledger.add_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(account_chart, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["text"].item() == new_account["text"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert pd.isna(created_accounts["vat_code"].item())
        assert created_accounts["group"].item() == new_account["group"]

        account_chart = ledger.account_chart()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "text": "test update account",
            "vat_code": "TestCodeAccounts",
            "group": "/Assets/Anlagevermögen",
        }
        ledger.modify_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(account_chart, updated_accounts, how="outer", indicator=True)
        modified_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(modified_accounts) == 1, "Expected exactly one updated row"
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["text"].item() == new_account["text"]
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["currency"].item() == new_account["currency"]
        assert modified_accounts["vat_code"].item() == "TestCodeAccounts"
        assert modified_accounts["group"].item() == new_account["group"]

        account_chart = ledger.account_chart()
        new_account = {
            "account": 1145,
            "currency": "USD",
            "text": "test update account without VAT",
            "vat_code": None,
            "group": "/Assets/Anlagevermögen",
        }
        ledger.modify_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(account_chart, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(account_chart) == len(updated_accounts), (
            "Expected account chart to have same length before and after modification"
        )
        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["text"].item() == new_account["text"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert pd.isna(created_accounts["vat_code"].item())
        assert created_accounts["group"].item() == new_account["group"]

        ledger.delete_account(account=1145)
        ledger.delete_account(account=1146)
        updated_accounts = ledger.account_chart()
        assert 1145 not in initial_accounts["account"].values
        assert 1146 not in initial_accounts["account"].values

    def test_add_already_existed_raise_error(self, ledger):
        new_account = {
            "account": 77777,
            "currency": "CHF",
            "text": "test account",
            "vat_code": None,
            "group": "/Assets/Anlagevermögen",
        }
        ledger.add_account(**new_account)
        with pytest.raises(ValueError, match=r"already exists"):
            ledger.add_account(**new_account)

    def test_modify_non_existed_raise_error(self, ledger):
        with pytest.raises(ValueError, match=r"not found or duplicated"):
            ledger.modify_account(
                account=1200,
                currency="EUR",
                text="test account",
                vat_code="TestCodeAccounts",
                group="/Assets/Anlagevermögen",
            )

    def test_mirror_accounts(self, ledger):
        initial_accounts = ledger.account_chart()
        accounts = ledger.standardize_account_chart(TEST_ACCOUNTS)

        target_df = pd.concat([accounts, initial_accounts], ignore_index=True)
        ledger.mirror_account_chart(target_df, delete=False)
        mirrored_df = ledger.account_chart()
        assert_frame_equal(target_df, mirrored_df, ignore_index=True, check_like=True)

        target_df = target_df[~target_df["account"].isin([9995, 9996])]
        ledger.mirror_account_chart(target_df, delete=True)
        mirrored_df = ledger.account_chart()
        assert_frame_equal(target_df, mirrored_df, ignore_index=True, check_like=True)

        target_df = target_df.sample(frac=1).reset_index(drop=True)
        target_account = "2222"
        target_df.loc[target_df["account"] == target_account, "text"] = "Updated Account Text"
        ledger.mirror_account_chart(target_df, delete=True)
        mirrored_df = ledger.account_chart()
        assert_frame_equal(target_df, mirrored_df, ignore_index=True, check_like=True)

    def test_mirror_empty_accounts(self, ledger):
        ledger.mirror_account_chart(ledger.standardize_account_chart(None), delete=True)
        assert ledger.account_chart().empty, "Mirroring empty df should erase all accounts"
