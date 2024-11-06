"""Test suite for accounts operations."""

import datetime
import pytest
from .base_accounts import BaseTestAccounts
from pyledger import MemoryLedger


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def ledger(self):
        return MemoryLedger()

    # TODO: This test is only implemented for MemoryLedger for now.
    # We will create an integration test 'BaseTextAccount' for all ledger
    # classes once the restore method can handle prices and revaluations.
    def test_account_balance(self, ledger):
        ledger.restore(accounts=self.ACCOUNTS, settings=self.SETTINGS, tax_codes=self.TAX_CODES,
                       ledger=self.LEDGER_ENTRIES, assets=self.ASSETS)
        # HACK: Add prices and revaluations DataFrames directly to MemoryLedger.
        #       Replace by ledger.restore(prices=self.PRICES, revaluations=self.REVALUATIONS)
        #       once accessors and mutators for prices are implemented.
        ledger._prices = self.PRICES
        ledger._prices = ledger._prices_as_dict_of_df
        ledger._revaluations = ledger.standardize_revaluations(self.REVALUATIONS)
        for _, row in self.EXPECTED_BALANCE.iterrows():
            date = datetime.datetime.strptime(row['date'], "%Y-%m-%d").date()
            account = row['account']
            expected = row['balance']
            actual = ledger.account_balance(date=date, account=row['account'])
            assert expected == actual, (
                f"Account balance for {account} on {date} of {actual} differs from {expected}."
            )
