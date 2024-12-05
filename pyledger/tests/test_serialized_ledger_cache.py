"""Unit tests for the serialized ledger caching mechanism."""

import pytest
from consistent_df import assert_frame_equal
from .base_test import BaseTest
from pyledger import MemoryLedger


def test_serialized_ledger_cache():
    engine = MemoryLedger()
    engine.restore(
        settings=BaseTest.SETTINGS, tax_codes=BaseTest.TAX_CODES,
        accounts=BaseTest.ACCOUNTS, ledger=BaseTest.LEDGER_ENTRIES, price_history=BaseTest.PRICES
    )

    # Serialized ledger should change when removing tax codes used in ledger entries.
    serialized_ledger = engine.serialized_ledger()
    engine._tax_codes._df = BaseTest.TAX_CODES.head(2)
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)
    engine.serialized_ledger.cache_clear()
    with pytest.raises(AssertionError):
        assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)

    # Serialized ledger should change when removing accounts used in ledger entries.
    serialized_ledger = engine.serialized_ledger()
    engine.accounts._df = BaseTest.ACCOUNTS.head(5)
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)
    engine.serialized_ledger.cache_clear()
    with pytest.raises(AssertionError):
        assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)

    # Serialized ledger should change when modifying ledger entries.
    serialized_ledger = engine.serialized_ledger()
    engine.ledger._df = BaseTest.LEDGER_ENTRIES.query("id in ['1', '2', '3', '4', '5']")
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)
    engine.serialized_ledger.cache_clear()
    with pytest.raises(AssertionError):
        assert_frame_equal(engine.serialized_ledger(), serialized_ledger, ignore_row_order=True)
