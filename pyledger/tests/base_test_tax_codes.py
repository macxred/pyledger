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

        # Add tax codes
        tax_codes = self.TAX_CODES.sample(frac=1).reset_index(drop=True)
        for tax_code in tax_codes.to_dict('records'):
            ledger.tax_codes.add([tax_code])
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify tax codes
        rows = [0, 3, len(tax_codes) - 1]
        for i in rows:
            tax_codes.loc[i, "description"] = f"New Description: {i}"
            ledger.tax_codes.modify([tax_codes.loc[i]])
            assert_frame_equal(
                ledger.tax_codes.list(), tax_codes,
                check_like=True, ignore_row_order=ignore_row_order
            )

        # Modify method receive only one needed field to modify
        rows = [0, 3, len(tax_codes) - 1]
        for i in rows:
            tax_codes.loc[i, "rate"] = 0.99
            ledger.tax_codes.modify({"id": [tax_codes.loc[i, "id"]], "rate": [0.99]})
            assert_frame_equal(
                ledger.tax_codes.list(), tax_codes,
                check_like=True, ignore_row_order=ignore_row_order
            )

        # Delete tax codes
        ledger.tax_codes.delete([{"id": tax_codes['id'].iloc[rows[0]]}])
        tax_codes = tax_codes.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.tax_codes.list(), tax_codes, check_like=True, ignore_row_order=ignore_row_order
        )

    def test_create_already_existed_raise_error(
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

    def test_update_non_existent_raise_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.tax_codes.modify([{
                "id": "TestCode", "description": "tax 20%",
                "account": 9990, "rate": 0.02, "is_inclusive": True
            }])

    def test_delete_tax_codes_allow_missing(
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