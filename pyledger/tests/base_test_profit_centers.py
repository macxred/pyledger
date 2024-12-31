"""Definition of abstract base class for testing profit center operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestProfitCenters(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_profit_center_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(settings=self.SETTINGS)

        # Add profit center one by one and with multiple rows
        profit_centers = self.PROFIT_CENTERS.sample(frac=1).reset_index(drop=True)
        for profit_center in profit_centers.head(-3).to_dict('records'):
            engine.profit_centers.add([profit_center])
        engine.profit_centers.add(profit_centers.tail(3))
        assert_frame_equal(
            engine.profit_centers.list(), profit_centers, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.profit_centers.delete([{"profit_center": profit_centers['profit_center'].iloc[0]}])
        profit_centers = profit_centers.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.profit_centers.list(), profit_centers, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.profit_centers.delete(profit_centers.iloc[[1, -1]])
        profit_centers = profit_centers.drop(profit_centers.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.profit_centers.list(), profit_centers, ignore_row_order=ignore_row_order
        )

    def test_add_existing_profit_center_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_profit_center = {"profit_center": "Bank"}
        engine.profit_centers.add([new_profit_center])
        with pytest.raises(error_class, match=error_message):
            engine.profit_centers.add([new_profit_center])

    def test_modify_nonexistent_profit_center_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.profit_centers.modify([{"profit_center": "Bank"}])

    def test_delete_profit_center_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.profit_centers.delete([{"profit_center": "Bank"}], allow_missing=False)
        engine.profit_centers.delete([{"profit_center": "Bank"}], allow_missing=True)

    def test_mirror_profit_centers(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.PROFIT_CENTERS, engine.profit_centers.list()], ignore_index=True)
        original_target = target.copy()
        engine.profit_centers.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror_profit_centers() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            engine.profit_centers.standardize(target), engine.profit_centers.list(),
            ignore_row_order=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.PROFIT_CENTERS.query("profit_center not in ['Bank', 'Shop']")
        engine.profit_centers.mirror(target, delete=False)
        assert_frame_equal(original_target, engine.profit_centers.list(), ignore_row_order=True)

        # Mirror with delete=True should change the data
        engine.profit_centers.mirror(target, delete=True)
        assert_frame_equal(target, engine.profit_centers.list(), ignore_row_order=True)

    def test_mirror_empty_assets(self, engine):
        engine.restore(profit_centers=self.PROFIT_CENTERS, settings=self.SETTINGS)
        assert not engine.profit_centers.list().empty
        engine.profit_centers.mirror(engine.profit_centers.standardize(None), delete=True)
        assert engine.profit_centers.list().empty
