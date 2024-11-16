"""Definition of abstract base class for testing tax operations."""

import pandas as pd
import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestTaxCodes(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    @pytest.fixture()
    def restored_engine(self, engine):
        """Accounting engine populated with accounts, tax codes, and settings"""
        tax_codes = self.TAX_CODES.query("id in ['OUT_STD', 'IN_STD']")
        engine.restore(accounts=self.ACCOUNTS, tax_codes=tax_codes, settings=self.SETTINGS)
        return engine

    def test_tax_codes_accessor_mutators(self, restored_engine, ignore_row_order=False):
        engine = restored_engine
        remote = engine.tax_codes.list()
        tax_codes = self.TAX_CODES.sample(frac=1).reset_index(drop=True)
        tax_codes = tax_codes[~tax_codes["id"].isin(remote["id"])]

        # Add tax codes one by one and with multiple rows
        engine.tax_codes.add(tax_codes.head(1))
        engine.tax_codes.add(tax_codes.tail(len(tax_codes) - 1))
        tax_codes = pd.concat([remote, tax_codes], ignore_index=True)
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        tax_codes.loc[0, "description"] = "Modify only one column test"
        engine.tax_codes.modify([{
            "id": tax_codes.loc[0, "id"],
            "description": "Modify only one column test"
        }])
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        tax_codes.loc[3, "description"] = "Modify with all columns test"
        engine.tax_codes.modify([tax_codes.loc[3]])
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        tax_codes.loc[tax_codes.index[[1, -1]], "description"] = "Modify multiple rows"
        engine.tax_codes.modify(tax_codes.loc[tax_codes.index[[1, -1]]])
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.tax_codes.delete([{"id": tax_codes["id"].loc[0]}])
        tax_codes = tax_codes.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.tax_codes.delete(tax_codes.iloc[[1, -1]])
        tax_codes = tax_codes.drop(tax_codes.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_create_existing__tax_code_raise_error(
        self, restored_engine, error_class=ValueError,
        error_message="Unique identifiers already exist."
    ):
        new_tax_code = {
            "id": "TestCode",
            "description": "tax 2%",
            "account": 9990,
            "rate": 0.02,
            "is_inclusive": True,
        }
        restored_engine.tax_codes.add([new_tax_code])
        with pytest.raises(error_class, match=error_message):
            restored_engine.tax_codes.add([new_tax_code])

    def test_update_nonexistent_tax_code_raise_error(
        self, restored_engine, error_class=ValueError,
        error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            restored_engine.tax_codes.modify([{
                "id": "TestCode", "description": "tax 20%",
                "account": 9990, "rate": 0.02, "is_inclusive": True
            }])

    def test_delete_tax_code_allow_missing(
        self, restored_engine, error_class=ValueError,
        error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            restored_engine.tax_codes.delete([{"id": "TestCode"}], allow_missing=False)
        restored_engine.tax_codes.delete([{"id": "TestCode"}], allow_missing=True)

    def test_mirror_tax_codes(self, restored_engine):
        engine = restored_engine
        engine.restore(settings=self.SETTINGS)
        target = pd.concat(
            [self.TAX_CODES, engine.tax_codes.list()], ignore_index=True
        ).drop_duplicates()
        original_target = target.copy()
        engine.tax_codes.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(target, engine.tax_codes.list(), ignore_row_order=True, check_like=True)

        # Mirror with delete=False shouldn't change the data
        target = self.TAX_CODES.query("account not in ['OutStd', 'OutRed']")
        engine.tax_codes.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.tax_codes.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.tax_codes.mirror(target, delete=True)
        assert_frame_equal(target, engine.tax_codes.list(), ignore_row_order=True, check_like=True)

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["id"] == "OutStdEx", "rate"] = 0.9
        engine.tax_codes.mirror(target, delete=True)
        assert_frame_equal(target, engine.tax_codes.list(), ignore_row_order=True, check_like=True)

    def test_mirror_empty_tax_codes(self, restored_engine):
        engine = restored_engine
        engine.restore(tax_codes=self.TAX_CODES, accounts=self.ACCOUNTS, settings=self.SETTINGS)
        assert not engine.tax_codes.list().empty, "Tax codes were not populated"
        engine.tax_codes.mirror(engine.tax_codes.standardize(None), delete=True)
        assert engine.tax_codes.list().empty, "Mirroring empty df should erase all tax codes"
