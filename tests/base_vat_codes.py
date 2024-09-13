"""Definition of abstract base class for testing VAT code operations."""

from io import StringIO
import pytest
import pandas as pd
from abc import ABC, abstractmethod


VAT_CSV = """
    id,       account, rate,  inclusive, text
    OutStd,   2200,    0.038, True,      VAT at the regular 7.7% rate on goods or services
    OutRed,   2200,    0.025, True,      VAT at the reduced 2.5% rate on goods or services
    OutAcc,   2200,    0.038, True,      XXXXX
    OutStdEx, 2200,    0.077, False,     VAT at the regular 7.7% rate on goods or services
    InStd,    1170,    0.077, True,      Input Tax (Vorsteuer) at the regular 7.7% rate on
    InRed,    1170,    0.025, True,      Input Tax (Vorsteuer) at the reduced 2.5% rate on
    InAcc,    1170,    0.038, True,      YYYYY
"""
VAT_CODES = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


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

    def test_mirror(self, ledger):
        # Mirror test vat codes onto server with delete=False
        ledger.mirror_vat_codes(VAT_CODES, delete=False)
        mirrored_df = ledger.vat_codes()
        standardized_df = ledger.standardize_vat_codes(VAT_CODES)
        m = standardized_df.merge(mirrored_df, how="left", indicator=True)
        assert (m["_merge"] == "both").all(), (
            "Mirroring error: Some target VAT codes were not mirrored"
        )

        # Mirror target vat codes onto server with delete=True
        ledger.mirror_vat_codes(VAT_CODES, delete=True)
        mirrored_df = ledger.vat_codes()
        m = standardized_df.merge(mirrored_df, how="outer", indicator=True)
        assert (m["_merge"] == "both").all(), (
            "Mirroring error: Some target VAT codes were not mirrored"
        )

        # Reshuffle target data randomly
        vat_codes_shuffled = VAT_CODES.sample(frac=1)

        # Mirror target vat codes onto server with updating
        vat_codes_shuffled.loc[vat_codes_shuffled["id"] == "OutStdEx", "rate"] = 0.9
        ledger.mirror_vat_codes(vat_codes_shuffled, delete=True)
        mirrored_df = ledger.vat_codes()
        vat_codes_shuffled = ledger.standardize_vat_codes(vat_codes_shuffled)
        m = vat_codes_shuffled.merge(mirrored_df, how='outer', indicator=True)
        assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target VAT codes were not mirrored'
        )

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
