"""Test suite for TextLedger accounts operations."""

import pytest
from .base_accounts import BaseTestAccounts
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestAccounts(BaseTestAccounts):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_account_mutators_does_not_change_order(self, ledger):
        """Test to ensure that mutator functions make minimal invasive changes to accounts file,
        preserving the original row order so that Git diffs show only the intended modifications.
        """
        account_numbers = self.ACCOUNTS["account"].tolist()
        for account in account_numbers:
            target = self.ACCOUNTS.query(f"account == {account}").iloc[0].to_dict()
            ledger.add_account(**target)

        expected = ledger.standardize_accounts(
            self.ACCOUNTS[self.ACCOUNTS["account"].isin(account_numbers)]
        )
        assert_frame_equal(ledger.accounts(), expected, check_like=True)

        expected.loc[expected["account"] == account_numbers[2], "description"] = "New description"
        target = expected.query(f"account == {account_numbers[2]}").iloc[0].to_dict()
        ledger.modify_account(**target)
        expected.loc[expected["account"] == account_numbers[6], "description"] = "New description"
        target = expected.query(f"account == {account_numbers[6]}").iloc[0].to_dict()
        ledger.modify_account(**target)
        assert_frame_equal(ledger.accounts(), expected, check_like=True)

        to_delete = [account_numbers[2], account_numbers[6]]
        expected = expected.query(f"account not in {to_delete}")
        ledger.delete_accounts(to_delete)
        assert_frame_equal(ledger.accounts(), expected, ignore_index=True, check_like=True)
