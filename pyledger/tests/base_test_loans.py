"""Definition of abstract base class for testing loan operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestLoans(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_loans_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(configuration=self.CONFIGURATION, accounts=self.ACCOUNTS)

        # Add loans one by one and with multiple rows
        loans = self.LOANS.sample(frac=1).reset_index(drop=True)
        for loan in loans.head(-2).to_dict('records'):
            engine.loans.add([loan])
        engine.loans.add(loans.tail(2))
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        loans.loc[0, "interest_rate"] = 0.06
        engine.loans.modify([{
            "account": loans.loc[0, "account"],
            "start": loans.loc[0, "start"],
            "interest_rate": 0.06
        }])
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        loans.loc[2, "description"] = "Modified loan description"
        engine.loans.modify([loans.loc[2]])
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with multiple rows
        loans.loc[loans.index[[1, -1]], "description"] = "Modified multiple loans"
        engine.loans.modify(loans.loc[loans.index[[1, -1]]])
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.loans.delete([{
            "account": loans['account'].iloc[0],
            "start": loans['start'].iloc[0],
        }])
        loans = loans.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.loans.delete(loans.iloc[[0, -1]])
        loans = loans.drop(loans.index[[0, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.loans.list(), loans,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_loan_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_loan = {
            "account": 2000, "start": datetime.date(2024, 1, 1),
            "interest_rate": 0.05, "capitalize": True, "frequency": "monthly",
            "description": "Test loan"
        }
        engine.loans.add([new_loan])
        with pytest.raises(error_class, match=error_message):
            engine.loans.add([new_loan])

    def test_modify_nonexistent_loan_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.loans.modify([{
                "account": 9999, "start": datetime.date(2024, 1, 1),
                "interest_rate": 0.05, "capitalize": True, "frequency": "monthly",
                "description": "Nonexistent loan"
            }])

    def test_delete_loan_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.loans.delete([{
                "account": 9999, "start": datetime.date(2024, 1, 1),
            }], allow_missing=False)
        engine.loans.delete([{
            "account": 9999, "start": datetime.date(2024, 1, 1),
        }], allow_missing=True)

    def test_mirror_loans(self, engine):
        engine.restore(configuration=self.CONFIGURATION, accounts=self.ACCOUNTS)
        target = pd.concat([self.LOANS, engine.loans.list()], ignore_index=True)
        original_target = target.copy()
        engine.loans.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to loans.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, engine.loans.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.LOANS.query("account != 2010")
        engine.loans.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.loans.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.loans.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.loans.list(), ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == 2000, "description"] = "Updated loan description"
        engine.loans.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.loans.list(), ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_loans(self, engine):
        engine.restore(loans=self.LOANS, configuration=self.CONFIGURATION, accounts=self.ACCOUNTS)
        assert not engine.loans.list().empty
        engine.loans.mirror(engine.loans.standardize(None), delete=True)
        assert engine.loans.list().empty
