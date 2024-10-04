"""Definition of abstract base class for testing tax operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest


class BaseTestTaxCodes(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_accessors_mutators(self, ledger):
        # Ensure there is no 'TestCode' tax_code
        ledger.delete_tax_codes(["TestCode"], allow_missing=True)
        assert "TestCode" not in ledger.tax_codes()["id"].values
        ledger.restore(accounts=self.ACCOUNTS, settings=self.SETTINGS)

        # Test adding a valid tax_code
        initial_tax_codes = ledger.tax_codes()
        new_tax_code = {
            "id": "TestCode",
            "description": "tax 2%",
            "account": 9990,
            "rate": 0.02,
            "is_inclusive": True,
        }
        ledger.add_tax_code(**new_tax_code)
        updated_tax_codes = ledger.tax_codes()
        outer_join = pd.merge(initial_tax_codes, updated_tax_codes, how="outer", indicator=True)
        created_tax_codes = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_tax_codes) == 1, "Expected exactly one row to be added"
        assert created_tax_codes["id"].item() == new_tax_code["id"]
        assert created_tax_codes["description"].item() == new_tax_code["description"]
        assert created_tax_codes["account"].item() == new_tax_code["account"]
        assert created_tax_codes["rate"].item() == new_tax_code["rate"]
        assert created_tax_codes["is_inclusive"].item() == new_tax_code["is_inclusive"]

        # Test updating a tax code with valid inputs.
        initial_tax_codes = ledger.tax_codes()
        new_tax_code = {
            "id": "TestCode",
            "description": "tax 20%",
            "account": 9990,
            "rate": 0.20,
            "is_inclusive": True,
        }
        ledger.modify_tax_code(**new_tax_code)
        updated_tax_codes = ledger.tax_codes()
        outer_join = pd.merge(initial_tax_codes, updated_tax_codes, how="outer", indicator=True)
        modified_tax_codes = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(initial_tax_codes) == len(updated_tax_codes), (
            "Expected tax codes to have same length before and after modification"
        )
        assert len(modified_tax_codes) == 1, "Expected exactly one updated row"
        assert modified_tax_codes["id"].item() == new_tax_code["id"]
        assert modified_tax_codes["description"].item() == new_tax_code["description"]
        assert modified_tax_codes["account"].item() == new_tax_code["account"]
        assert modified_tax_codes["rate"].item() == new_tax_code["rate"]
        assert modified_tax_codes["is_inclusive"].item() == new_tax_code["is_inclusive"]

        # Test deleting an existent tax code.
        ledger.delete_tax_codes(codes=["TestCode"])
        updated_tax_codes = ledger.tax_codes()

        assert "TestCode" not in updated_tax_codes["id"].values

    def test_create_already_existed_raise_error(
        self, ledger, error_class=ValueError, error_message="already exists"
    ):
        new_tax_code = {
            "id": "TestCode",
            "description": "tax 2%",
            "account": 9990,
            "rate": 0.02,
            "is_inclusive": True,
        }
        ledger.add_tax_code(**new_tax_code)
        with pytest.raises(error_class, match=error_message):
            ledger.add_tax_code(**new_tax_code)

    def test_update_non_existent_raise_error(
        self, ledger, error_class=ValueError, error_message="not found or duplicated"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.modify_tax_code(
                id="TestCode", description="tax 20%", account=9990, rate=0.02, is_inclusive=True
            )

    def test_mirror(self, ledger):
        # Standardize TAX_CODES before testing
        standardized_df = ledger.standardize_tax_codes(self.TAX_CODES)

        # Mirror test tax codes onto server with delete=False
        initial = standardized_df.copy()
        ledger.mirror_tax_codes(standardized_df, delete=False)
        mirrored_df = ledger.tax_codes()
        assert_frame_equal(standardized_df, mirrored_df, ignore_row_order=True, check_like=True)
        # Mirroring should not change initial df
        assert_frame_equal(initial, mirrored_df, ignore_row_order=True, check_like=True)

        # Mirror target tax codes onto server with delete=True
        tax_codes = self.TAX_CODES[~self.TAX_CODES["id"].isin(["OutStd", "OutRed"])]
        ledger.mirror_tax_codes(tax_codes, delete=True)
        mirrored_df = ledger.tax_codes()
        expected = ledger.standardize_tax_codes(tax_codes)
        assert_frame_equal(
            expected, mirrored_df, ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the tax rates
        tax_codes_shuffled = self.TAX_CODES.sample(frac=1).reset_index(drop=True)
        tax_codes_shuffled.loc[tax_codes_shuffled["id"] == "OutStdEx", "rate"] = 0.9
        tax_codes_shuffled = ledger.standardize_tax_codes(tax_codes_shuffled)

        # Mirror target tax codes onto server with updating
        ledger.mirror_tax_codes(tax_codes_shuffled, delete=True)
        mirrored_df = ledger.tax_codes()
        assert_frame_equal(tax_codes_shuffled, mirrored_df, ignore_row_order=True, check_like=True)

    def test_mirror_empty_tax_codes(self, ledger):
        ledger.restore(tax_codes=self.TAX_CODES, accounts=self.ACCOUNTS, settings=self.SETTINGS)
        assert not ledger.tax_codes().empty, "Tax codes were not populated"
        ledger.mirror_tax_codes(ledger.standardize_tax_codes(None), delete=True)
        assert ledger.tax_codes().empty, "Mirroring empty df should erase all tax codes"
