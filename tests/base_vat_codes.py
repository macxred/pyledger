"""Definition of abstract base class for testing VAT code operations."""

import pytest
import pandas as pd
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal
from .constants import VAT_CODES


class BaseTestVatCode(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_accessors_mutators(self, ledger):
        # Ensure there is no 'TestCode' vat_code
        ledger.delete_vat_code("TestCode", allow_missing=True)
        assert "TestCode" not in ledger.vat_codes()["id"].values

        # Test adding a valid vat_code
        initial_vat_codes = ledger.vat_codes()
        new_vat_code = {
            "code": "TestCode",
            "text": "VAT 2%",
            "account": 2200,
            "rate": 0.02,
            "inclusive": True,
        }
        ledger.add_vat_code(**new_vat_code)
        updated_vat_codes = ledger.vat_codes()
        outer_join = pd.merge(initial_vat_codes, updated_vat_codes, how="outer", indicator=True)
        created_vat_codes = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_vat_codes) == 1, "Expected exactly one row to be added"
        assert created_vat_codes["id"].item() == new_vat_code["code"]
        assert created_vat_codes["text"].item() == new_vat_code["text"]
        assert created_vat_codes["account"].item() == new_vat_code["account"]
        assert created_vat_codes["rate"].item() == new_vat_code["rate"]
        assert created_vat_codes["inclusive"].item() == new_vat_code["inclusive"]

        # Test updating a VAT code with valid inputs.
        initial_vat_codes = ledger.vat_codes()
        new_vat_code = {
            "code": "TestCode",
            "text": "VAT 20%",
            "account": 2000,
            "rate": 0.20,
            "inclusive": True,
        }
        ledger.modify_vat_code(**new_vat_code)
        updated_vat_codes = ledger.vat_codes()
        outer_join = pd.merge(initial_vat_codes, updated_vat_codes, how="outer", indicator=True)
        modified_vat_codes = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(initial_vat_codes) == len(updated_vat_codes), (
            "Expected vat codes to have same length before and after modification"
        )
        assert len(modified_vat_codes) == 1, "Expected exactly one updated row"
        assert modified_vat_codes["id"].item() == new_vat_code["code"]
        assert modified_vat_codes["text"].item() == new_vat_code["text"]
        assert modified_vat_codes["account"].item() == new_vat_code["account"]
        assert modified_vat_codes["rate"].item() == new_vat_code["rate"]
        assert modified_vat_codes["inclusive"].item() == new_vat_code["inclusive"]

        # Test deleting an existent VAT code.
        ledger.delete_vat_code(code="TestCode")
        updated_vat_codes = ledger.vat_codes()

        assert "TestCode" not in updated_vat_codes["id"].values

    def test_create_already_existed_raise_error(self, ledger):
        new_vat_code = {
            "code": "TestCode",
            "text": "VAT 2%",
            "account": 2200,
            "rate": 0.02,
            "inclusive": True,
        }
        ledger.add_vat_code(**new_vat_code)
        with pytest.raises(ValueError, match=r"already exists"):
            ledger.add_vat_code(**new_vat_code)

    def test_update_non_existent_raise_error(self, ledger):
        with pytest.raises(ValueError, match=r"not found or duplicated"):
            ledger.modify_vat_code(
                code="TestCode", text="VAT 20%", account=2200, rate=0.02, inclusive=True
            )

    def test_mirror(self, ledger):
        # Standardize VAT_CODES before testing
        standardized_df = ledger.standardize_vat_codes(VAT_CODES)

        # Mirror test VAT codes onto server with delete=False
        ledger.mirror_vat_codes(VAT_CODES, delete=False)
        mirrored_df = ledger.vat_codes()
        assert_frame_equal(standardized_df, mirrored_df, ignore_index=True, check_like=True)

        # Mirror target VAT codes onto server with delete=True
        vat_codes = VAT_CODES[~VAT_CODES["id"].isin(["OutStd", "OutRed"])]
        ledger.mirror_vat_codes(vat_codes, delete=True)
        mirrored_df = ledger.vat_codes()
        assert_frame_equal(
            ledger.standardize_vat_codes(vat_codes), mirrored_df, ignore_index=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the VAT rates
        vat_codes_shuffled = VAT_CODES.sample(frac=1).reset_index(drop=True)
        vat_codes_shuffled.loc[vat_codes_shuffled["id"] == "OutStdEx", "rate"] = 0.9
        vat_codes_shuffled = ledger.standardize_vat_codes(vat_codes_shuffled)

        # Mirror target VAT codes onto server with updating
        ledger.mirror_vat_codes(vat_codes_shuffled, delete=True)
        mirrored_df = ledger.vat_codes()
        assert_frame_equal(vat_codes_shuffled, mirrored_df, ignore_index=True, check_like=True)

    def test_mirror_empty_vat_codes(self, ledger):
        ledger.mirror_vat_codes(ledger.standardize_vat_codes(None), delete=True)
        assert ledger.vat_codes().empty, "Mirroring empty df should erase all VAT codes"
