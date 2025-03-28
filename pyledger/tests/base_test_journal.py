"""Definition of abstract base class for testing journal operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestJournal(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    @pytest.fixture()
    def restored_engine(self, engine):
        """Accounting engine populated with accounts and tax codes,
        clear of any journal entries."""
        accounts = engine.accounts.list()
        accounts = pd.concat([accounts, self.ACCOUNTS]).drop_duplicates(["account"])
        engine.restore(
            accounts=accounts, tax_codes=self.TAX_CODES, journal=[],
            configuration=self.CONFIGURATION, price_history=self.PRICES, assets=self.ASSETS,
            profit_centers=self.PROFIT_CENTERS,
        )
        return engine

    def test_journal_accessor_mutators(self, restored_engine, ignore_row_order=False):
        engine = restored_engine

        # Add journal entries one by one and with multiple rows
        expected = self.JOURNAL.copy()
        txn_ids = expected["id"].unique()
        for id in txn_ids[:10]:
            engine.journal.add(expected.query(f"id == '{id}'"))
        remaining_ids = txn_ids[10:]  # noqa: F841
        engine.journal.add(expected.query("`id` in @remaining_ids"))
        assert_frame_equal(
            engine.journal.list(), expected,
            ignore_columns=["id"], ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a journal entry with a specific id
        expected = engine.journal.list()
        id = expected.iloc[0]["id"]
        expected.loc[expected["id"] == id, "description"] = "Modify with all columns"
        engine.journal.modify(expected.loc[expected["id"] == id])
        assert_frame_equal(
            engine.journal.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with multiple rows
        ids = expected["id"].unique()[[0, -1, -4]]
        assert len(ids) > 1, "Expecting several ids, got a single one."
        expected.loc[expected["id"].isin(ids), "description"] = "Modify multiple rows"
        to_modify = expected.loc[expected["id"].isin(ids)]
        to_modify = pd.DataFrame({"id": ids}).merge(to_modify, on="id", how="left", validate="1:m")
        engine.journal.modify(to_modify)
        assert_frame_equal(
            engine.journal.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Replace an individual transaction by a collective transaction
        current = engine.journal.list()
        single_txn_id = current[~current["id"].duplicated(keep=False)].iloc[0]["id"]
        collective_txn = self.JOURNAL.query("id == '8'").copy()
        collective_txn.loc[:, "id"] = single_txn_id
        single_txn_index = current[current["id"] == single_txn_id].index[0]
        rows_before = current.loc[:single_txn_index - 1]
        rows_after = current.loc[single_txn_index + 1:]
        expected = pd.concat([rows_before, collective_txn, rows_after], ignore_index=True)
        engine.journal.modify(collective_txn)
        assert_frame_equal(
            engine.journal.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Replace a collective transaction by an individual transaction
        current = engine.journal.list()
        collective_txn_id = current[current["id"].duplicated(keep=False)].iloc[0]["id"]
        collective_txn_indices = current[current["id"] == collective_txn_id].index
        single_txn = self.JOURNAL.query("id == '2'").copy()
        single_txn.loc[:, "id"] = collective_txn_id
        rows_before = current.loc[:collective_txn_indices[0] - 1]
        rows_after = current.loc[collective_txn_indices[-1] + 1:]
        expected = pd.concat([rows_before, single_txn, rows_after], ignore_index=True)
        engine.journal.modify(single_txn)
        assert_frame_equal(
            engine.journal.list(), expected,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single entry
        current = engine.journal.list()
        id_to_drop = current.loc[0]["id"]
        engine.journal.delete([{"id": id_to_drop}])
        journal_entries = current[~current["id"].isin([id_to_drop])].reset_index(drop=True)
        assert_frame_equal(
            engine.journal.list(), journal_entries, ignore_columns=["id"],
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple entries
        current = engine.journal.list()
        ids_to_drop = current["id"].iloc[[1, -1]]
        engine.journal.delete(current.iloc[[1, -1]])
        journal_entries = current[~current["id"].isin(ids_to_drop)].reset_index(drop=True)
        assert_frame_equal(
            engine.journal.list(), journal_entries, ignore_columns=["id"],
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_already_existed_raise_error(
        self, restored_engine, error_class=ValueError,
        error_message="identifiers already exist."
    ):
        target = self.JOURNAL.query("id == '1'").copy()
        restored_engine.journal.add(target)
        with pytest.raises(error_class, match=error_message):
            restored_engine.journal.add(target)

    def test_modify_non_existed_raises_error(
        self, restored_engine, error_class=ValueError, error_message="not present in the data."
    ):
        target = self.JOURNAL.query("id == '2'").copy()
        target["id"] = 999999
        with pytest.raises(error_class, match=error_message):
            restored_engine.journal.modify(target)

    def test_delete_entry_allow_missing(
        self, restored_engine, error_class=ValueError,
        error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            restored_engine.journal.delete({"id": ["FAKE_ID"]}, allow_missing=False)
        restored_engine.journal.delete({"id": ["FAKE_ID"]}, allow_missing=True)

    def test_mirror_journal(self, restored_engine):
        engine = restored_engine

        # Mirror with one single and one collective transaction
        target = engine.sanitize_journal(self.JOURNAL.query("id in ['8', '2']"))
        engine.journal.mirror(target=target, delete=True)
        expected = engine.journal.standardize(target)
        mirrored = engine.journal.list()
        assert sorted(engine.txn_to_str(mirrored).values()) == \
               sorted(engine.txn_to_str(expected).values())

        # Mirror with duplicate transactions and delete=False
        target = engine.sanitize_journal(pd.concat(
            [
                self.JOURNAL.query("id == '8'").assign(id='4'),
                self.JOURNAL.query("id == '8'").assign(id='5'),
                self.JOURNAL.query("id == '2'").assign(id='6'),
                self.JOURNAL.query("id == '2'").assign(id='7'),
            ]
        ))
        engine.journal.mirror(target=target, delete=False)
        expected = engine.journal.standardize(target)
        mirrored = engine.journal.list()

        assert sorted(engine.txn_to_str(mirrored).values()) == \
               sorted(engine.txn_to_str(expected).values())

        # Mirror with complex transactions and delete=False
        target = engine.sanitize_journal(self.JOURNAL.query("id in ['15', '16', '17', '22']"))
        engine.journal.mirror(target=target, delete=False)
        expected = engine.journal.standardize(target)
        expected = engine.sanitize_journal(expected)
        expected = pd.concat([mirrored, expected])
        mirrored = engine.journal.list()
        assert sorted(engine.txn_to_str(mirrored).values()) == \
               sorted(engine.txn_to_str(expected).values())

        # Mirror existing transactions with delete=False has no impact
        target = engine.sanitize_journal(self.JOURNAL.query("id in ['8', '2']"))
        engine.journal.mirror(target=target, delete=False)
        mirrored = engine.journal.list()
        assert sorted(engine.txn_to_str(mirrored).values()) == \
               sorted(engine.txn_to_str(expected).values())

        # Mirror with delete=True
        target = engine.sanitize_journal(self.JOURNAL.query("id in ['8', '2']"))
        engine.journal.mirror(target=target, delete=True)
        mirrored = engine.journal.list()
        expected = engine.journal.standardize(target)
        assert sorted(engine.txn_to_str(mirrored).values()) == \
               sorted(engine.txn_to_str(expected).values())

        # Mirror an empty target state
        engine.journal.mirror(target=pd.DataFrame({}), delete=True)
        assert engine.journal.list().empty, "Mirroring empty DF should erase all journal entries"
