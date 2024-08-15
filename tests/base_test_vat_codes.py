"""This module provides an abstract base class for testing VAT code operations.
It defines common test cases that can be inherited and used by specific
ledger implementations. The actual ledger implementation must be provided
by subclasses through the abstract ledger fixture.
"""

import pytest
import pandas as pd
from abc import ABC, abstractmethod


class BaseTestVatCode(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_vat_code_accessors_mutators(self, ledger):
        # Ensure there is no 'TestCode' vat_code on the remote account
        ledger.delete_vat_code("TestCode", allow_missing=True)
        assert "TestCode" not in ledger.vat_codes().index

        # Test adding a valid vat_code
        initial_vat_codes = ledger.vat_codes().reset_index()
        new_vat_code = {
            "code": "TestCode",
            "text": "VAT 2%",
            "account": 2200,
            "rate": 0.02,
            "inclusive": True,
        }
        ledger.add_vat_code(**new_vat_code)
        updated_vat_codes = ledger.vat_codes().reset_index()
        outer_join = pd.merge(initial_vat_codes, updated_vat_codes, how="outer", indicator=True)
        created_vat_codes = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

        assert len(created_vat_codes) == 1, "Expected exactly one row to be added"
        assert created_vat_codes["id"].item() == new_vat_code["code"]
        assert created_vat_codes["text"].item() == new_vat_code["text"]
        assert created_vat_codes["account"].item() == new_vat_code["account"]
        assert created_vat_codes["rate"].item() == new_vat_code["rate"]
        assert created_vat_codes["inclusive"].item() == new_vat_code["inclusive"]

        # Test updating a VAT code with valid inputs.
        initial_vat_codes = ledger.vat_codes().reset_index()
        new_vat_code = {
            "code": "TestCode",
            "text": "VAT 20%",
            "account": 2000,
            "rate": 0.20,
            "inclusive": True,
        }
        ledger.update_vat_code(**new_vat_code)
        updated_vat_codes = ledger.vat_codes().reset_index()
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

        assert "TestCode" not in updated_vat_codes.index
