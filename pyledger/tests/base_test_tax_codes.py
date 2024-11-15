"""Definition of abstract base class for testing tax operations."""

import pandas as pd
import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestTaxCodes(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_tax_codes_accessor_mutators(self, ledger, ignore_row_order=False):
        ledger.restore(accounts=self.ACCOUNTS, settings=self.SETTINGS)

        # Add tax codes one by one and with multiple rows
        tax_codes = self.TAX_CODES.sample(frac=1).reset_index(drop=True)
        for tax_code in tax_codes.head(-3).to_dict('records'):
            ledger.tax_codes.add([tax_code])
        ledger.tax_codes.add(tax_codes.tail(3))
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        tax_codes.loc[0, "description"] = "Modify only one column test"
        ledger.tax_codes.modify([{
            "id": tax_codes.loc[0, "id"],
            "description": "Modify only one column test"
        }])
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        tax_codes.loc[3, "description"] = "Modify with all columns test"
        ledger.tax_codes.modify([tax_codes.loc[3]])
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        tax_codes.loc[tax_codes.index[[1, -1]], "description"] = "Modify multiple rows"
        ledger.tax_codes.modify(tax_codes.loc[tax_codes.index[[1, -1]]])
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        ledger.tax_codes.delete([{"id": tax_codes["id"].loc[0]}])
        tax_codes = tax_codes.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        ledger.tax_codes.delete(tax_codes.iloc[[1, -1]])
        tax_codes = tax_codes.drop(tax_codes.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_create_existing__tax_code_raise_error(
        self, ledger, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_tax_code = {
            "id": "TestCode",
            "description": "tax 2%",
            "account": 9990,
            "rate": 0.02,
            "is_inclusive": True,
        }
        ledger.tax_codes.add([new_tax_code])
        with pytest.raises(error_class, match=error_message):
            ledger.tax_codes.add([new_tax_code])

    def test_update_nonexistent_tax_code_raise_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.tax_codes.modify([{
                "id": "TestCode", "description": "tax 20%",
                "account": 9990, "rate": 0.02, "is_inclusive": True
            }])

    def test_delete_tax_code_allow_missing(
        self, ledger, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.tax_codes.delete([{
                "id": "TestCode", "description": "tax 20%",
                "account": 9990, "rate": 0.02, "is_inclusive": True
            }], allow_missing=False)
        ledger.tax_codes.delete([{
            "id": "TestCode", "description": "tax 20%",
            "account": 9990, "rate": 0.02, "is_inclusive": True
        }], allow_missing=True)

    def test_mirror_tax_codes(self, ledger):
        ledger.restore(settings=self.SETTINGS)
        target = pd.concat([self.TAX_CODES, ledger.tax_codes.list()], ignore_index=True)
        original_target = target.copy()
        ledger.tax_codes.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(target, ledger.tax_codes.list(), ignore_row_order=True, check_like=True)

        # Mirror with delete=False shouldn't change the data
        target = self.TAX_CODES.query("account not in ['OutStd', 'OutRed']")
        ledger.tax_codes.mirror(target, delete=False)
        assert_frame_equal(
            original_target, ledger.tax_codes.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        ledger.tax_codes.mirror(target, delete=True)
        assert_frame_equal(target, ledger.tax_codes.list(), ignore_row_order=True, check_like=True)

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["id"] == "OutStdEx", "rate"] = 0.9
        ledger.tax_codes.mirror(target, delete=True)
        assert_frame_equal(target, ledger.tax_codes.list(), ignore_row_order=True, check_like=True)

    def test_mirror_empty_tax_codes(self, ledger):
        ledger.restore(tax_codes=self.TAX_CODES, accounts=self.ACCOUNTS, settings=self.SETTINGS)
        assert not ledger.tax_codes.list().empty, "Tax codes were not populated"
        ledger.tax_codes.mirror(ledger.tax_codes.standardize(None), delete=True)
        assert ledger.tax_codes.list().empty, "Mirroring empty df should erase all tax codes"
