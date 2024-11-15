"""Test suite for TextLedger tax code operations"""

import pytest
from .base_test_tax_codes import BaseTestTaxCodes
from pyledger import TextLedger
from consistent_df import assert_frame_equal


class TestTaxCodes(BaseTestTaxCodes):

    @pytest.fixture
    def engine(self, tmp_path):
        return TextLedger(tmp_path)

    def test_tax_code_mutators_does_not_change_order(self, engine):
        """Test to ensure that mutator functions make minimal invasive changes to tax codes file,
        preserving the original row order so that Git diffs show only the intended modifications.
        """
        tax_codes = self.TAX_CODES.sample(frac=1).reset_index(drop=True)
        for tax_code in tax_codes.to_dict('records'):
            engine.tax_codes.add([tax_code])
        assert_frame_equal(engine.tax_codes.list(), tax_codes, check_like=True)

        rows = [0, 3, len(tax_codes) - 1]
        for i in rows:
            tax_codes.loc[i, "description"] = f"New description {i + 1}"
            engine.tax_codes.modify([tax_codes.loc[i]])
            assert_frame_equal(engine.tax_codes.list(), tax_codes, check_like=True)

        engine.tax_codes.delete({"id": tax_codes['id'].iloc[rows]})
        expected = tax_codes.drop(rows).reset_index(drop=True)
        assert_frame_equal(engine.tax_codes.list(), expected, check_like=True)
