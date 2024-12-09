"""Unit tests for the serialized ledger caching mechanism."""

import logging
import pandas as pd
from consistent_df import assert_frame_equal
import pytest
from .base_test import BaseTest
from pyledger import MemoryLedger

# Disable logger messages for the test suite to keep output clean.
logging.getLogger("ledger").setLevel(logging.CRITICAL)


@pytest.fixture
def engine():
    engine = MemoryLedger()
    engine.restore(
        settings=BaseTest.SETTINGS,
        tax_codes=BaseTest.TAX_CODES,
        accounts=BaseTest.ACCOUNTS,
        ledger=BaseTest.LEDGER_ENTRIES,
        price_history=BaseTest.PRICES,
    )
    return engine


def test_serialized_ledger_tax_codes_mutators_cache_invalidation(engine):
    serialized_ledger = engine.serialized_ledger()

    # Identify a tax code that is currently in use.
    used_tax_name = serialized_ledger['tax_code'].dropna().iloc[0]  # noqa: F841
    tax_code = BaseTest.TAX_CODES.query("id == @used_tax_name")

    # Direct manipulation of the private DataFrame should NOT affect the cached ledger.
    engine._tax_codes._df = pd.concat([
        tax_code, BaseTest.TAX_CODES.head(2)
    ]).drop_duplicates(["id"])
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger)

    # Manually clearing the cache should now reflect the changes.
    engine.serialized_ledger.cache_clear()
    assert not serialized_ledger.equals(engine.serialized_ledger())

    # Using mutator methods (delete, add, modify) should also invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.tax_codes.delete(tax_code)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.tax_codes.add(tax_code)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    tax_code.loc[:, "rate"] = 0.99
    engine.tax_codes.modify(tax_code)
    assert not serialized_ledger.equals(engine.serialized_ledger())


def test_serialized_ledger_accounts_mutators_cache_invalidation(engine):
    serialized_ledger = engine.serialized_ledger()

    # Identify an account that is currently in use.
    used_account = serialized_ledger['account'].dropna().iloc[0]  # noqa: F841
    account = BaseTest.ACCOUNTS.query("account == @used_account")

    # Direct manipulation of the private DataFrame should NOT affect the cached ledger.
    engine._accounts._df = pd.concat([
        account, BaseTest.ACCOUNTS.head(5)
    ]).drop_duplicates(["account"])
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger)

    # Manually clearing the cache should now reflect the changes.
    engine.serialized_ledger.cache_clear()
    assert not serialized_ledger.equals(engine.serialized_ledger())

    # Using account mutator methods (delete, add) should invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.accounts.delete(account)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.accounts.add(account)
    assert not serialized_ledger.equals(engine.serialized_ledger())


def test_serialized_ledger_ledger_mutators_cache_invalidation(engine):
    ledger = BaseTest.LEDGER_ENTRIES.query("id == '1'")
    serialized_ledger = engine.serialized_ledger()

    # Direct manipulation of the private DataFrame should NOT affect the cached ledger.
    engine._ledger._df = pd.concat([
        BaseTest.LEDGER_ENTRIES.query("id in ['2', '3', '4', '5']"), ledger
    ]).drop_duplicates(["id"])
    assert_frame_equal(engine.serialized_ledger(), serialized_ledger)

    # Manually clearing the cache should now reflect the changes.
    engine.serialized_ledger.cache_clear()
    assert not serialized_ledger.equals(engine.serialized_ledger())

    # Using ledger mutator methods (delete, add, modify) should invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.ledger.delete(ledger)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.ledger.add(ledger)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    ledger.loc[:, "description"] = "test description"
    engine.ledger.modify(ledger)
    assert not serialized_ledger.equals(engine.serialized_ledger())
