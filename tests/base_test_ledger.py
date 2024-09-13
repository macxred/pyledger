"""Definition of abstract base class for testing ledger operations."""

import pytest
import pandas as pd
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal
from .constants import LEDGER_ENTRIES, ACCOUNTS


class BaseTestLedger(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    @pytest.mark.parametrize(
        "ledger_id", set(LEDGER_ENTRIES["id"].unique())
    )
    def test_add_ledger_entry(self, ledger, ledger_id):
        target = LEDGER_ENTRIES.query("id == @ledger_id")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(
            created, expected, ignore_index=True, ignore_columns=["id"], check_exact=True
        )

    def test_accessor_mutators_single_transaction(self, ledger):
        # Test adding a ledger entry
        target = LEDGER_ENTRIES.query("id == 1")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger.ledger()
        target = LEDGER_ENTRIES.query("id == 4").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test replacing with a collective ledger entry
        target = LEDGER_ENTRIES.query("id == 2").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert initial_ledger["id"].unique() == remote["id"].unique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the created ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_single_transaction_without_VAT(self, ledger):
        # Test adding a ledger entry without VAT code
        target = LEDGER_ENTRIES.query("id == 4").copy()
        target["vat_code"] = None
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger.ledger()
        target = LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert initial_ledger["id"].unique() == remote["id"].unique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction(self, ledger):
        # Test adding a collective ledger entry
        target = LEDGER_ENTRIES.query("id == 2")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger.ledger()
        target = LEDGER_ENTRIES.query("id == 3").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test replacing with an individual ledger entry
        target = LEDGER_ENTRIES.iloc[[0]].copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert initial_ledger["id"].unique() == remote["id"].unique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction_without_vat(self, ledger):
        # Test adding a collective ledger entry without VAT code
        target = LEDGER_ENTRIES.query("id == 2").copy()
        target["vat_code"] = None
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        initial_ledger = ledger.ledger()
        target = LEDGER_ENTRIES.query("id == 3").copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert initial_ledger["id"].unique() == remote["id"].unique(), (
            "The number of unique 'id' values should be the same."
        )
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def add_already_existed_raise_error(self, ledger):
        target = LEDGER_ENTRIES.query("id == 1").copy()
        ledger.add_ledger(target)
        with pytest.raises(ValueError, match=r"already exists"):
            ledger.add_ledger(target)

    def add_with_ambiguous_id_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(ValueError, match=r"Id needs to be unique and present"):
            ledger.add_ledger(target)

    def test_modify_non_existed_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = 999999
        with pytest.raises(ValueError, match=r"not found"):
            ledger.modify_ledger_entry(target)

    def add_modify_with_ambiguous_id_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(ValueError, match=r"Id needs to be unique and present"):
            ledger.modify_ledger_entry(target)

    def test_mirror_ledger(self, ledger):
        ledger.mirror_account_chart(ACCOUNTS, delete=False)
        # Mirror with one single and one collective transaction
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=True)
        expected = ledger.standardize_ledger(target)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with duplicate transactions and delete=False
        target = pd.concat(
            [
                LEDGER_ENTRIES.query("id == 1"),
                LEDGER_ENTRIES.query("id == 1").assign(id=5),
                LEDGER_ENTRIES.query("id == 2").assign(id=6),
                LEDGER_ENTRIES.query("id == 2"),
            ]
        )
        ledger.mirror_ledger(target=target, delete=True)
        expected = ledger.standardize_ledger(target)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with complex transactions and delete=False
        target = LEDGER_ENTRIES.query("id in [15, 16, 17, 18]")
        ledger.mirror_ledger(target=target, delete=False)
        expected = ledger.standardize_ledger(target)
        expected = ledger.sanitize_ledger(expected)
        expected = pd.concat([mirrored, expected])
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror existing transactions with delete=False has no impact
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=False)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with delete=True
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=True)
        mirrored = ledger.ledger()
        expected = ledger.standardize_ledger(target)
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror an empty target state
        ledger.mirror_ledger(target=pd.DataFrame({}), delete=True)
        assert ledger.ledger().empty
