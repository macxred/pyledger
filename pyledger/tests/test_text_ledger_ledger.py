"""Test suite for TextLedger ledger operations."""

import time
import pytest
from pyledger import TextLedger
from consistent_df import assert_frame_equal
from .base_test_ledger import BaseTestLedger


class TestLedger(BaseTestLedger):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_ledger_invalidation(self, ledger):
        assert ledger._is_expired(ledger._ledger_time)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_time)
        ledger._invalidate_ledger()
        assert ledger._is_expired(ledger._ledger_time)

    def test_ledger_timeout(self, tmp_path):
        ledger = TextLedger(tmp_path, cache_timeout=1)
        ledger.ledger()
        assert not ledger._is_expired(ledger._ledger_time)
        time.sleep(1)
        assert ledger._is_expired(ledger._ledger_time)

    def test_ledger_mutators_does_not_change_order(self, ledger):
        """Test to ensure that mutator functions make minimal invasive changes to ledger files,
        preserving the original row order so that Git diffs show only the intended modifications.
        """

        ledger_ids = [str(id) for id in range(1, 15)]
        for id in ledger_ids:
            ledger.add_ledger_entry(self.LEDGER_ENTRIES.query(f"id == '{id}'"))
        expected = ledger.standardize_ledger(self.LEDGER_ENTRIES.query(f"id in {ledger_ids}"))
        expected["id"] = "default.csv:" + expected["id"]
        assert_frame_equal(ledger.ledger(), expected)

        expected.loc[expected["id"] == "default.csv:4", "amount"] = 8888888888
        df = expected.query("id == 'default.csv:4'")
        ledger.modify_ledger_entry(df)
        expected.loc[expected["id"] == "default.csv:9", "amount"] = 7777777777
        df = expected.query("id == 'default.csv:9'")
        ledger.modify_ledger_entry(df)
        assert_frame_equal(ledger.ledger(), expected)

        to_delete = ['default.csv:3', 'default.csv:10']
        expected = expected.query(f"id not in {to_delete}")
        ledger.delete_ledger_entries(to_delete)
        assert_frame_equal(ledger.ledger(), expected, ignore_columns=["id"], ignore_index=True)
