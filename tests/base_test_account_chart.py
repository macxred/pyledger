import pytest
import pandas as pd
from abc import ABC, abstractmethod


class BaseTestAccountCharts(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_account_mutators(self, ledger):
        ledger.delete_account(1145)
        ledger.delete_account(1146)
        account_chart = ledger.account_chart()
        assert 1145 not in account_chart["account"].values
        assert 1146 not in account_chart["account"].values

        initial_accounts = ledger.account_chart()
        new_account = {
            "account": 1145,
            "currency": "CHF",
            "text": "test create account",
            "vat_code": "TestCodeAccounts",
            "group": "/Assets/Anlagevermögen",
        }
        ledger.add_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["text"].item() == new_account["text"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert created_accounts["vat_code"].item() == "TestCodeAccounts"
        assert created_accounts["group"].item() == new_account["group"]

        initial_accounts = ledger.account_chart()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "text": "test create account",
            "vat_code": None,
            "group": "/Assets/Anlagevermögen",
        }
        ledger.add_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
        created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_accounts) == 1, "Expected exactly one row to be added"
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["text"].item() == new_account["text"]
        assert created_accounts["account"].item() == new_account["account"]
        assert created_accounts["currency"].item() == new_account["currency"]
        assert pd.isna(created_accounts["vat_code"].item())
        assert created_accounts["group"].item() == new_account["group"]

        initial_accounts = ledger.account_chart()
        new_account = {
            "account": 1146,
            "currency": "CHF",
            "text": "test update account",
            "vat_code": "TestCodeAccounts",
            "group": "/Assets/Anlagevermögen",
        }
        ledger.modify_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
        modified_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(modified_accounts) == 1, "Expected exactly one updated row"
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["text"].item() == new_account["text"]
        assert modified_accounts["account"].item() == new_account["account"]
        assert modified_accounts["currency"].item() == new_account["currency"]
        assert modified_accounts["vat_code"].item() == "TestCodeAccounts"
        assert modified_accounts["group"].item() == new_account["group"]

        initial_accounts = ledger.account_chart()
        new_account = {
            "account": 1145,
            "currency": "USD",
            "text": "test update account without VAT",
            "vat_code": None,
            "group": "/Assets/Anlagevermögen",
        }
        ledger.modify_account(**new_account)
        updated_accounts = ledger.account_chart()
        outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
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
        assert 1145 not in account_chart["account"].values
        assert 1146 not in account_chart["account"].values
