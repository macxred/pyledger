"""Definition of abstract base class for testing revaluation operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestRevaluations(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_revaluations_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(settings=self.SETTINGS, accounts=self.ACCOUNTS)

        # Add revaluations one by one and with multiple rows
        revaluations = self.REVALUATIONS.sample(frac=1).reset_index(drop=True)
        for revaluation in revaluations.head(-3).to_dict('records'):
            engine.revaluations.add([revaluation])
        engine.revaluations.add(revaluations.tail(3))
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        revaluations.loc[0, "debit"] = 1005
        engine.revaluations.modify([{
            "account": revaluations.loc[0, "account"],
            "date": revaluations.loc[0, "date"],
            "debit": 1005
        }])
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        revaluations.loc[3, "description"] = "Modify with all columns test"
        engine.revaluations.modify([revaluations.loc[3]])
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        revaluations.loc[revaluations.index[[1, -1]], "description"] = "Modify multiple rows"
        engine.revaluations.modify(revaluations.loc[revaluations.index[[1, -1]]])
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.revaluations.delete([{
            "account": revaluations['account'].iloc[0],
            "date": revaluations['date'].iloc[0],
        }])
        revaluations = revaluations.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.revaluations.delete(revaluations.iloc[[1, -1]])
        revaluations = revaluations.drop(revaluations.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_revaluation_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_revaluation = {
            "account": "1000:5000", "date": datetime.date(2023, 1, 1),
            "debit": 1111, "credit": 2222, "description": "Revaluation description"
        }
        engine.revaluations.add([new_revaluation])
        with pytest.raises(error_class, match=error_message):
            engine.revaluations.add([new_revaluation])

    def test_modify_nonexistent_revaluation_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.revaluations.modify([{
                "account": "1000:5000", "date": datetime.date(2023, 1, 1),
                "debit": 1111, "credit": 2222, "description": "Revaluation description"
            }])

    def test_delete_revaluation_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.revaluations.delete([{
                "account": "1000:5000", "date": datetime.date(2023, 1, 1),
                "debit": 1111, "credit": 2222, "description": "Revaluation description"
            }], allow_missing=False)
        engine.revaluations.delete([{
            "account": "1000:5000", "date": datetime.date(2023, 1, 1),
            "debit": 1111, "credit": 2222, "description": "Revaluation description"
        }], allow_missing=True)

    def test_mirror_revaluations(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.REVALUATIONS, engine.revaluations.list()], ignore_index=True)
        original_target = target.copy()
        engine.revaluations.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to revaluations.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, engine.revaluations.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.REVALUATIONS.query("credit not in [8050]")
        engine.revaluations.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.revaluations.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.revaluations.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.revaluations.list(), ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == "1000:2999", "description"] = "New Description"
        engine.revaluations.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.revaluations.list(), ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_revaluations(self, engine):
        engine.restore(revaluations=self.REVALUATIONS, settings=self.SETTINGS)
        assert not engine.revaluations.list().empty
        engine.revaluations.mirror(engine.revaluations.standardize(None), delete=True)
        assert engine.revaluations.list().empty
