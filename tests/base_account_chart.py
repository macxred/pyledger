"""Definition of abstract base class for testing accounts operations."""

import pytest
import pandas as pd
from abc import ABC, abstractmethod
from io import StringIO


VAT_CSV = """
    id,                 account, rate,  inclusive, text
    TestCodeAccounts,   2200,    0.02,  True,      VAT 2%
"""
VAT_CODES = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


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
        account = pd.DataFrame(
            {
                "account": [1, 2],
                "currency": ["CHF", "EUR"],
                "text": ["Test Account 1", "Test Account 2"],
                "vat_code": ["TestCodeAccounts", None],
                "group": ["/Assets", "/Assets/Anlagevermögen/xyz"],
            }
        )
        target_df = pd.concat([account, initial_accounts])

        ledger.mirror_account_chart(target_df, delete=False)
        mirrored_df = ledger.account_chart()
        m = target_df.merge(mirrored_df, how="left", indicator=True)
        assert (m["_merge"] == "both").all(), "Some target accounts were not mirrored"

        ledger.mirror_account_chart(target_df, delete=True)
        mirrored_df = ledger.account_chart()
        m = target_df.merge(mirrored_df, how="outer", indicator=True)
        assert (m["_merge"] == "both").all(), "Some target accounts were not mirrored"

        target_df = target_df.sample(frac=1)
        target_df.loc[target_df["account"] == 2, "text"] = "New_Test_Text"
        ledger.mirror_account_chart(target_df, delete=True)
        mirrored_df = ledger.account_chart()
        m = target_df.merge(mirrored_df, how="outer", indicator=True)
        assert (m["_merge"] == "both").all(), "Some target accounts were not mirrored"

        ledger.mirror_account_chart(initial_accounts, delete=True)
        mirrored_df = ledger.account_chart()
        m = initial_accounts.merge(mirrored_df, how="outer", indicator=True)
        assert (m["_merge"] == "both").all(), "Some target accounts were not mirrored"
