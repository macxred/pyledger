"""Definition of abstract base class for testing cost center operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestCostCenters(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_cost_center_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(settings=self.SETTINGS)

        # Add cost center one by one and with multiple rows
        cost_centers = self.COST_CENTERS.sample(frac=1).reset_index(drop=True)
        for cost_center in cost_centers.head(-3).to_dict('records'):
            engine.cost_centers.add([cost_center])
        engine.cost_centers.add(cost_centers.tail(3))
        assert_frame_equal(
            engine.cost_centers.list(), cost_centers, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.cost_centers.delete([{"cost_center": cost_centers['cost_center'].iloc[0]}])
        cost_centers = cost_centers.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.cost_centers.list(), cost_centers, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.cost_centers.delete(cost_centers.iloc[[1, -1]])
        cost_centers = cost_centers.drop(cost_centers.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.cost_centers.list(), cost_centers, ignore_row_order=ignore_row_order
        )

    def test_add_existing_cost_center_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_cost_center = {"cost_center": "Bank"}
        engine.cost_centers.add([new_cost_center])
        with pytest.raises(error_class, match=error_message):
            engine.cost_centers.add([new_cost_center])

    def test_modify_nonexistent_cost_center_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.cost_centers.modify([{"cost_center": "Bank"}])

    def test_delete_cost_center_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.cost_centers.delete([{"cost_center": "Bank"}], allow_missing=False)
        engine.cost_centers.delete([{"cost_center": "Bank"}], allow_missing=True)

    def test_mirror_cost_centers(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.COST_CENTERS, engine.cost_centers.list()], ignore_index=True)
        original_target = target.copy()
        engine.cost_centers.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror_cost_centers() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            engine.cost_centers.standardize(target), engine.cost_centers.list(),
            ignore_row_order=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.COST_CENTERS.query("cost_center not in ['Bank', 'Shop']")
        engine.cost_centers.mirror(target, delete=False)
        assert_frame_equal(original_target, engine.cost_centers.list(), ignore_row_order=True)

        # Mirror with delete=True should change the data
        engine.cost_centers.mirror(target, delete=True)
        assert_frame_equal(target, engine.cost_centers.list(), ignore_row_order=True)

    def test_mirror_empty_assets(self, engine):
        engine.restore(cost_centers=self.COST_CENTERS, settings=self.SETTINGS)
        assert not engine.cost_centers.list().empty
        engine.cost_centers.mirror(engine.cost_centers.standardize(None), delete=True)
        assert engine.cost_centers.list().empty
