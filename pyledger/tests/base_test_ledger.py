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
        return ledger

    @pytest.mark.parametrize("ledger_id", BaseTest.LEDGER_ENTRIES["id"].astype(str).unique())
    def test_add_ledger_entry(self, ledger_engine, ledger_id):
        target = self.LEDGER_ENTRIES.query("id == @ledger_id")
        id = ledger_engine.ledger.add(target)["id"]
        remote = ledger_engine.ledger.list()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(
            created, expected, ignore_columns=["id"], ignore_row_order=True, check_exact=True
        )

    def test_accessor_mutators_single_transaction(self, ledger_engine):
        # Test adding a ledger entry
        target = self.LEDGER_ENTRIES.query("id == '1'")
        id = ledger_engine.ledger.add(target)["id"]
        remote = ledger_engine.ledger.list()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(
            created, expected, ignore_row_order=True, ignore_columns=["id"]
        )

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger.list()
        target = self.LEDGER_ENTRIES.query("id == '4'").copy()
        target["id"] = id
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test replacing with a collective ledger entry
        target = self.LEDGER_ENTRIES.query("id == '2'").copy()
        target["id"] = id
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the created ledger entry
        ledger_engine.ledger.delete({"id": [id]})
        remote = ledger_engine.ledger.list()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_single_transaction_without_tax(self, ledger_engine):
        # Test adding a ledger entry without tax code
        target = self.LEDGER_ENTRIES.query("id == '4'").copy()
        target["tax_code"] = None
        id = ledger_engine.ledger.add(target)["id"]
        remote = ledger_engine.ledger.list()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger.list()
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.ledger.delete({"id": [id]})
        remote = ledger_engine.ledger.list()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction(self, ledger_engine):
        # Test adding a collective ledger entry
        target = self.LEDGER_ENTRIES.query("id == '2'")
        id = ledger_engine.ledger.add(target)["id"]
        remote = ledger_engine.ledger.list()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger.list()
        target = self.LEDGER_ENTRIES.query("id == '3'").copy()
        target["id"] = id
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test replacing with an individual ledger entry
        target = self.LEDGER_ENTRIES.iloc[[0]].copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.ledger.delete({"id": [id]})
        remote = ledger_engine.ledger.list()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction_without_tax(self, ledger_engine):
        # Test adding a collective ledger entry without tax code
        target = self.LEDGER_ENTRIES.query("id == '2'").copy()
        target["tax_code"] = None
        id = ledger_engine.ledger.add(target)["id"]
        remote = ledger_engine.ledger.list()
        created = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert_frame_equal(created, expected, ignore_row_order=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger_engine.ledger.list()
        target = self.LEDGER_ENTRIES.query("id == '3'").copy()
        target["id"] = id
        target["tax_code"] = None
        ledger_engine.ledger.modify(target)
        remote = ledger_engine.ledger.list()
        updated = remote.loc[remote["id"] == id]
        expected = ledger_engine.ledger.standardize(target)
        assert initial_ledger["id"].nunique() == remote["id"].nunique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_row_order=True)

        # Test deleting the updated ledger entry
        ledger_engine.ledger.delete({"id": [id]})
        remote = ledger_engine.ledger.list()
        assert all(remote["id"] != id), f"Ledger entry {id} was not deleted"

    def add_already_existed_raise_error(
        self, ledger_engine, error_class=ValueError, error_message="already exists"
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        ledger_engine.ledger.add(target)
        with pytest.raises(error_class, match=error_message):
            ledger_engine.ledger.add(target)

    def add_with_ambiguous_id_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="Id needs to be unique"
    ):
        target = self.LEDGER_ENTRIES.query("id in ['1', '2']").copy()
        with pytest.raises(error_class, match=error_message):
            ledger_engine.ledger.add(target)

    def test_modify_non_existed_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="not present in the data."
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        target["id"] = 999999
        with pytest.raises(error_class, match=error_message):
            ledger_engine.ledger.modify(target)

    def add_modify_with_ambiguous_id_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="Id needs to be unique"
    ):
        target = self.LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(error_class, match=error_message):
            ledger_engine.ledger.modify(target)

    def test_delete_entry_allow_missing(
        self, ledger, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.ledger.delete({"id": ["FAKE_ID"]}, allow_missing=False)
        ledger.ledger.delete({"id": ["FAKE_ID"]}, allow_missing=True)

    def test_mirror_ledger(self, ledger_engine):
        ledger_engine.accounts.mirror(self.ACCOUNTS, delete=False)
        # Mirror with one single and one collective transaction
        target = self.LEDGER_ENTRIES.query("id in ['1', '2']")
        ledger_engine.ledger.mirror(target=target, delete=True)
        expected = ledger_engine.ledger.standardize(target)
        mirrored = ledger_engine.ledger.list()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with duplicate transactions and delete=False
        target = pd.concat(
            [
                self.LEDGER_ENTRIES.query("id == '1'").assign(id='4'),
                self.LEDGER_ENTRIES.query("id == '1'").assign(id='5'),
                self.LEDGER_ENTRIES.query("id == '2'").assign(id='6'),
                self.LEDGER_ENTRIES.query("id == '2'").assign(id='7'),
            ]
        )
        ledger_engine.ledger.mirror(target=target, delete=False)
        expected = ledger_engine.ledger.standardize(target)
        mirrored = ledger_engine.ledger.list()

        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with complex transactions and delete=False
        target = self.LEDGER_ENTRIES.query("id in ['15', '16', '17', '18']")
        ledger_engine.ledger.mirror(target=target, delete=False)
        expected = ledger_engine.ledger.standardize(target)
        expected = ledger_engine.sanitize_ledger(expected)
        expected = pd.concat([mirrored, expected])
        mirrored = ledger_engine.ledger.list()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror existing transactions with delete=False has no impact
        target = self.LEDGER_ENTRIES.query("id in ['1', '2']")
        ledger_engine.ledger.mirror(target=target, delete=False)
        mirrored = ledger_engine.ledger.list()
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror with delete=True
        target = self.LEDGER_ENTRIES.query("id in ['1', '2']")
        ledger_engine.ledger.mirror(target=target, delete=True)
        mirrored = ledger_engine.ledger.list()
        expected = ledger_engine.ledger.standardize(target)
        assert sorted(ledger_engine.txn_to_str(mirrored).values()) == \
               sorted(ledger_engine.txn_to_str(expected).values())

        # Mirror an empty target state
        ledger_engine.ledger.mirror(target=pd.DataFrame({}), delete=True)
        assert ledger_engine.ledger.list().empty, "Mirroring empty DF should erase all ledgers"
