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

    def test_ledger_accessor_mutators(self, ledger, ignore_row_order=False):
        # Add ledger entries one by one and with multiple rows
        expected = self.LEDGER_ENTRIES.copy()
        for id in expected.head(-7)["id"].unique():
            ledger.ledger.add(expected.query(f"id == '{id}'"))
        ledger.ledger.add(self.LEDGER_ENTRIES.tail(7))
        assert_frame_equal(
            ledger.ledger.list(), expected,
            ignore_columns=["id"], ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        expected = ledger.ledger.list()
        id = expected.iloc[0]["id"]
        expected.loc[expected["id"] == id, "description"] = "Modify with all columns"
        ledger.ledger.modify(expected.loc[expected["id"] == id])
        assert_frame_equal(
            ledger.ledger.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with multiple rows
        ids = expected.tail(3)["id"].unique()
        expected.loc[expected["id"].isin(ids), "description"] = "Modify multiple rows"
        ledger.ledger.modify(expected.loc[expected["id"].isin(ids)])
        assert_frame_equal(
            ledger.ledger.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify single transaction with a collective
        current = ledger.ledger.list()
        single_txn_id = current[~current["id"].duplicated(keep=False)].iloc[0]["id"]
        collective_txn = self.LEDGER_ENTRIES.query("id == '1'").copy()
        collective_txn.loc[:, "id"] = single_txn_id
        single_txn_index = current[current["id"] == single_txn_id].index[0]
        rows_before = current.loc[:single_txn_index - 1]
        rows_after = current.loc[single_txn_index + 1:]
        expected = pd.concat([rows_before, collective_txn, rows_after], ignore_index=True)
        ledger.ledger.modify(collective_txn)
        assert_frame_equal(
            ledger.ledger.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify collective transaction with a single
        current = ledger.ledger.list()
        collective_txn_id = current[current["id"].duplicated(keep=False)].iloc[0]["id"]
        collective_txn_indices = current[current["id"] == collective_txn_id].index
        single_txn = self.LEDGER_ENTRIES.query("id == '2'").copy()
        single_txn.loc[:, "id"] = collective_txn_id
        rows_before = current.loc[:collective_txn_indices[0] - 1]
        rows_after = current.loc[collective_txn_indices[-1] + 1:]
        expected = pd.concat([rows_before, single_txn, rows_after], ignore_index=True)
        ledger.ledger.modify(single_txn)
        assert_frame_equal(
            ledger.ledger.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        current = ledger.ledger.list()
        id_to_drop = current.loc[0]["id"]
        ledger.ledger.delete([{"id": id_to_drop}])
        ledger_entries = current[~current["id"].isin([id_to_drop])].reset_index(drop=True)
        assert_frame_equal(
            ledger.ledger.list(), ledger_entries, ignore_columns=["id"],
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        current = ledger.ledger.list()
        ids_to_drop = current["id"].iloc[[1, -1]]
        ledger.ledger.delete(current.iloc[[1, -1]])
        ledger_entries = current[~current["id"].isin(ids_to_drop)].reset_index(drop=True)
        assert_frame_equal(
            ledger.ledger.list(), ledger_entries, ignore_columns=["id"],
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_already_existed_raise_error(
        self, ledger_engine, error_class=ValueError, error_message="identifiers already exist."
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        ledger_engine.ledger.add(target)
        with pytest.raises(error_class, match=error_message):
            ledger_engine.ledger.add(target)

    def test_modify_non_existed_raises_error(
        self, ledger_engine, error_class=ValueError, error_message="not present in the data."
    ):
        target = self.LEDGER_ENTRIES.query("id == '1'").copy()
        target["id"] = 999999
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
