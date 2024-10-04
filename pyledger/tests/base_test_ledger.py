"""Definition of abstract base class for testing ledger operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestLedger(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    @pytest.fixture()
    def ledger_engine(self, ledger):
        ledger.restore(accounts=self.ACCOUNTS, tax_codes=self.TAX_CODES)
        self.LEDGER_ENTRIES = ledger.standardize_ledger(self.LEDGER_ENTRIES)
        return ledger

    @pytest.mark.parametrize("ledger_id", BaseTest.LEDGER_ENTRIES["id"].astype(str).unique())
    def test_add_ledger_entry(self, ledger_engine, ledger_id):
        target = self.LEDGER_ENTRIES.query("id == @ledger_id")
        id = ledger_engine.add_ledger_entry(target)
        remote = ledger_engine.ledger()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(
            created, expected, ignore_columns=["id"], ignore_row_order=True, check_exact=True
        )

    def test_accessor_mutators_single_transaction(self, ledger_engine):
        # Test adding a ledger entry
        target = self.LEDGER_ENTRIES.query("id == '1'")
        id = ledger_engine.add_ledger_entry(target)
        remote = ledger_engine.ledger()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(
            created, expected, ignore_row_order=True, ignore_columns=["id"]
        )

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger()
        target = self.LEDGER_ENTRIES.query("id == '4'").copy()
        target["id"] = id
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test replacing with a collective ledger entry
        target = self.LEDGER_ENTRIES.query("id == '2'").copy()
        target["id"] = id
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the created ledger entry
        ledger_engine.delete_ledger_entries([id])
        remote = ledger_engine.ledger()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_single_transaction_without_tax(self, ledger_engine):
        # Test adding a ledger entry without tax code
        target = self.LEDGER_ENTRIES.query("id == '4'").copy()
        target["tax_code"] = None
        id = ledger_engine.add_ledger_entry(target)
        remote = ledger_engine.ledger()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger()
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.delete_ledger_entries([id])
        remote = ledger_engine.ledger()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction(self, ledger_engine):
        # Test adding a collective ledger entry
        target = self.LEDGER_ENTRIES.query("id == '2'")
        id = ledger_engine.add_ledger_entry(target)
        remote = ledger_engine.ledger()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger()
        target = self.LEDGER_ENTRIES.query("id == '3'").copy()
        target["id"] = id
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test replacing with an individual ledger entry
        target = self.LEDGER_ENTRIES.iloc[[0]].copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.delete_ledger_entries([id])
        remote = ledger_engine.ledger()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction_without_tax(self, ledger_engine):
        # Test adding a collective ledger entry without tax code
        target = self.LEDGER_ENTRIES.query("id == '2'").copy()
        target["tax_code"] = None
        id = ledger_engine.add_ledger_entry(target)
        remote = ledger_engine.ledger()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger()
        target = self.LEDGER_ENTRIES.query("id == '3'").copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.modify_ledger_entry(target)
        remote = ledger_engine.ledger()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.standardize_ledger(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.delete_ledger_entries([id])
        remote = ledger_engine.ledger()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def add_already_existed_raise_error(
        self, ledger_engine, error_class=ValueError, error_message="already exists"
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        ledger_engine.add_ledger(target)
        with pytest.raises(error_class, match=error_message):
            ledger_engine.add_ledger(target)

    def add_with_ambiguous_id_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="Id needs to be unique"
    ):
        target = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        with pytest.raises(error_class, match=error_message):
            ledger_engine.add_ledger(target)

    def test_modify_non_existed_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="not found"
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        target["id"] = 999999
        with pytest.raises(error_class, match=error_message):
            ledger_engine.modify_ledger_entry(target)

    def add_modify_with_ambiguous_id_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="Id needs to be unique"
    ):
        target = self.LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(error_class, match=error_message):
            ledger_engine.modify_ledger_entry(target)

    def test_mirror_ledger(self, ledger_engine):
        ledger_engine.mirror_accounts(self.ACCOUNTS, delete=False)
        # Mirror with one single and one collective transaction
        target = self.LEDGER_ENTRIES.query("id in [1, 2]")
        ledger_engine.mirror_ledger(target=target, delete=True)
        expected = ledger_engine.standardize_ledger(target)
        mirrored = ledger_engine.ledger()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with duplicate transactions and delete=False
        target = pd.concat(
            [
                self.LEDGER_ENTRIES.query("id == 1"),
                self.LEDGER_ENTRIES.query("id == 1").assign(id=5),
                self.LEDGER_ENTRIES.query("id == 2").assign(id=6),
                self.LEDGER_ENTRIES.query("id == 2"),
            ]
        )
        ledger_engine.mirror_ledger(target=target, delete=True)
        expected = ledger_engine.standardize_ledger(target)
        mirrored = ledger_engine.ledger()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with complex transactions and delete=False
        target = self.LEDGER_ENTRIES.query("id in [15, 16, 17, 18]")
        ledger_engine.mirror_ledger(target=target, delete=False)
        expected = ledger_engine.standardize_ledger(target)
        expected = ledger_engine.sanitize_ledger(expected)
        expected = pd.concat([mirrored, expected])
        mirrored = ledger_engine.ledger()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror existing transactions with delete=False has no impact
        target = self.LEDGER_ENTRIES.query("id in [1, 2]")
        ledger_engine.mirror_ledger(target=target, delete=False)
        mirrored = ledger_engine.ledger()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with delete=True
        target = self.LEDGER_ENTRIES.query("id in [1, 2]")
        ledger_engine.mirror_ledger(target=target, delete=True)
        mirrored = ledger_engine.ledger()
        expected = ledger_engine.standardize_ledger(target)
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror an empty target state
        ledger_engine.mirror_ledger(target=pd.DataFrame({}), delete=True)
        assert ledger_engine.ledger().empty, "Mirroring empty DF should erase all ledgers"
