"""Definition of abstract base class for testing reconciliation operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestReconciliation(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_reconciliation_accessor_mutators(self, engine, ignore_row_order=False):
        # Add reconciliation one by one and with multiple rows
        reconciliation = self.RECONCILIATION.sample(frac=1).reset_index(drop=True)
        for revaluation in reconciliation.head(-3).to_dict('records'):
            engine.reconciliation.add([revaluation])
        engine.reconciliation.add(reconciliation.tail(3))
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        reconciliation.loc[0, "tolerance"] = 0.0001
        engine.reconciliation.modify([{
            "source": reconciliation.loc[0, "source"],
            "period": reconciliation.loc[0, "period"],
            "account": reconciliation.loc[0, "account"],
            "currency": reconciliation.loc[0, "currency"],
            "profit_center": reconciliation.loc[0, "profit_center"],
            "tolerance": 0.0001
        }])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        reconciliation.loc[3, "document"] = "Modify with all columns test"
        engine.reconciliation.modify([reconciliation.loc[3]])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        reconciliation.loc[reconciliation.index[[1, -1]], "document"] = "Modify multiple rows"
        engine.reconciliation.modify(reconciliation.loc[reconciliation.index[[1, -1]]])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.reconciliation.delete([{
            "source": reconciliation["source"].iloc[0],
            "period": reconciliation["period"].iloc[0],
            "account": reconciliation["account"].iloc[0],
            "currency": reconciliation["currency"].iloc[0],
            "profit_center": reconciliation.loc[0, "profit_center"],
        }])
        reconciliation = reconciliation.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.reconciliation.delete(reconciliation.iloc[[1, -1]])
        reconciliation = reconciliation.drop(reconciliation.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_reconciliation_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_reconciliation = {
            "account": "1000:5000", "period": datetime.date(2023, 1, 1),
            "profit_center": "General",
        }
        engine.reconciliation.add([new_reconciliation])
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.add([new_reconciliation])

    def test_modify_nonexistent_reconciliation_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.modify([{
                "account": "1000", "period": datetime.date(2023, 1, 1),
                "profit_center": "General",
            }])

    def test_delete_reconciliation_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.delete([{
                "account": "1000:5000", "period": datetime.date(2023, 1, 1),
                "profit_center": "General",
            }], allow_missing=False)
        engine.reconciliation.delete([{
            "account": "1000:5000", "period": datetime.date(2023, 1, 1),
            "profit_center": "General",
        }], allow_missing=True)

    def test_mirror_reconciliations(self, engine):
        engine.restore(configuration=self.CONFIGURATION)
        target = pd.concat([self.RECONCILIATION, engine.reconciliation.list()], ignore_index=True)
        original_target = target.copy()
        engine.reconciliation.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to reconciliation.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.RECONCILIATION.query("account not in [1000]")
        engine.reconciliation.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.reconciliation.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == "1000:9999", "document"] = "New Document"
        engine.reconciliation.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_reconciliations(self, engine):
        engine.restore(reconciliation=self.RECONCILIATION, configuration=self.CONFIGURATION)
        assert not engine.reconciliation.list().empty
        engine.reconciliation.mirror(engine.reconciliation.standardize(None), delete=True)
        assert engine.reconciliation.list().empty
