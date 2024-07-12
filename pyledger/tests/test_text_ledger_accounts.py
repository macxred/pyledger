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
        accounts = self.ACCOUNTS.sample(frac=1).reset_index(drop=True)
        for account in accounts.to_dict('records'):
            ledger.add_account(**account)
        assert_frame_equal(ledger.accounts(), accounts, check_like=True)

        rows = [0, 3, len(accounts) - 1]
        for i in rows:
            accounts.loc[i, "description"] = f"New description {i + 1}"
            ledger.modify_account(**accounts.loc[i].to_dict())
            assert_frame_equal(ledger.accounts(), accounts, check_like=True)

        ledger.delete_accounts(accounts['account'].iloc[rows])
        expected = accounts.drop(rows).reset_index(drop=True)
        assert_frame_equal(ledger.accounts(), expected, check_like=True)