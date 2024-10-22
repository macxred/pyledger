"""Test suite for TextLedger tax code operations"""

import pytest
from .base_tax_codes import BaseTestTaxCodes
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def ledger(self, tmp_path):
        return TextLedger(tmp_path)

    def test_tax_code_mutators_does_not_change_order(self, ledger):
        """Test to ensure that mutator functions make minimal invasive changes to tax codes file,
        preserving the original row order so that Git diffs show only the intended modifications.
        """
        tax_ids = self.TAX_CODES["id"].tolist()
        for id in tax_ids:
            target = self.TAX_CODES.query(f"id == '{id}'").iloc[0].to_dict()
            target.pop("contra", None)
            ledger.add_tax_code(**target)
        expected = ledger.standardize_tax_codes(self.TAX_CODES[self.TAX_CODES["id"].isin(tax_ids)])
        assert_frame_equal(ledger.tax_codes(), expected, check_like=True)

        expected.loc[expected["id"] == tax_ids[2], "description"] = "New description"
        target = expected.query(f"id == '{tax_ids[2]}'").iloc[0].to_dict()
        target.pop("contra", None)
        ledger.modify_tax_code(**target)
        expected.loc[expected["id"] == tax_ids[6], "description"] = "New description"
        target = expected.query(f"id == '{tax_ids[6]}'").iloc[0].to_dict()
        target.pop("contra", None)
        ledger.modify_tax_code(**target)
        assert_frame_equal(ledger.tax_codes(), expected, check_like=True)

        to_delete = [tax_ids[2], tax_ids[6]]
        expected = expected.query(f"id not in {to_delete}")
        ledger.delete_tax_codes(to_delete)
        assert_frame_equal(ledger.tax_codes(), expected, ignore_index=True, check_like=True)
