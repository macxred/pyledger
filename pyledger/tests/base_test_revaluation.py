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
    def ledger(self):
        pass

    def test_revaluations_accessor_mutators(self, ledger, ignore_row_order=False):
        ledger.restore(settings=self.SETTINGS)
        revaluations = self.REVALUATIONS.sample(frac=1).reset_index(drop=True)
        for revaluation in revaluations.to_dict('records'):
            ledger.revaluations.add([revaluation])
        assert_frame_equal(
            ledger.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

        rows = [0, 3, len(revaluations) - 1]
        for i in rows:
            revaluations.loc[i, "description"] = "New description"
            ledger.revaluations.modify([revaluations.loc[i]])
            assert_frame_equal(
                ledger.revaluations.list(), revaluations,
                check_like=True, ignore_row_order=ignore_row_order
            )

        ledger.revaluations.delete([{
            "account": revaluations['account'].iloc[rows[0]],
            "date": revaluations['date'].iloc[rows[0]],
        }])
        revaluations = revaluations.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.revaluations.list(), revaluations,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_revaluation_raises_error(
        self, ledger, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_revaluation = {
            "account": "1000:5000", "date": datetime.date(2023, 1, 1),
            "debit": 1111, "credit": 2222, "description": "Revaluation description"
        }
        ledger.revaluations.add([new_revaluation])
        with pytest.raises(error_class, match=error_message):
            ledger.revaluations.add([new_revaluation])

    def test_modify_nonexistent_revaluation_raises_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.revaluations.modify([{
                "account": "1000:5000", "date": datetime.date(2023, 1, 1),
                "debit": 1111, "credit": 2222, "description": "Revaluation description"
            }])

    def test_delete_revaluation_allow_missing(self, ledger):
        ledger.restore(revaluations=self.REVALUATIONS, settings=self.SETTINGS)
        with pytest.raises(ValueError, match="Some ids are not present in the data."):
            ledger.revaluations.delete([{
                "account": "1000:5000", "date": datetime.date(2023, 1, 1),
                "debit": 1111, "credit": 2222, "description": "Revaluation description"
            }], allow_missing=False)
        ledger.revaluations.delete([{
            "account": "1000:5000", "date": datetime.date(2023, 1, 1),
            "debit": 1111, "credit": 2222, "description": "Revaluation description"
        }], allow_missing=True)

    def test_mirror_revaluations(self, ledger):
        ledger.restore(settings=self.SETTINGS)
        target = pd.concat([self.REVALUATIONS, ledger.revaluations.list()], ignore_index=True)
        original_target = target.copy()
        ledger.revaluations.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to revaluations.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, ledger.revaluations.list(),
            ignore_row_order=True, check_like=True
        )

        target = self.REVALUATIONS.query("credit not in [8050]")
        ledger.revaluations.mirror(target, delete=True)
        assert_frame_equal(
            target, ledger.revaluations.list(),
            ignore_row_order=True, check_like=True
        )

        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == "1000:2999", "description"] = "New Description"
        ledger.revaluations.mirror(target, delete=True)
        assert_frame_equal(
            target, ledger.revaluations.list(),
            ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_revaluations(self, ledger):
        ledger.restore(revaluations=self.REVALUATIONS, settings=self.SETTINGS)
        assert not ledger.revaluations.list().empty
        ledger.revaluations.mirror(ledger.revaluations.standardize(None), delete=True)
        assert ledger.revaluations.list().empty
