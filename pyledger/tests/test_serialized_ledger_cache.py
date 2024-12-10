"""Unit tests for the serialized ledger caching mechanism."""

import logging
import pytest
from .base_test import BaseTest
from pyledger import MemoryLedger


@pytest.fixture
def muted_logger():
    logger = logging.getLogger("ledger")
    original_level = logger.level
    logger.setLevel(logging.CRITICAL)
    yield logger
    logger.setLevel(original_level)


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


def test_serialized_ledger_cache(engine):
    serialized_ledger = engine.serialized_ledger()

    # Direct manipulation of the private DataFrame should NOT affect the cached data.
    engine._ledger._df = engine._ledger._df.query("id in ['2', '3', '4', '5']")
    assert serialized_ledger.equals(engine.serialized_ledger()), "serialized_ledger was not cached"

    # Manually clearing the cache should now reflect the changes.
    engine.serialized_ledger.cache_clear()
    assert not serialized_ledger.equals(engine.serialized_ledger()), "cached was not cleared"


def test_tax_code_mutators_invalidate_serialized_ledger(engine, muted_logger):
    # Identify a tax code that is in use.
    used_tax_code = engine.ledger.list()['tax_code'].dropna().iloc[0]  # noqa: F841
    tax_code = engine.tax_codes.list().query("id == @used_tax_code")

    # Using mutator methods (delete, add, modify) should invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.tax_codes.delete(tax_code)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.tax_codes.add(tax_code)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.tax_codes.modify(tax_code.assign(rate=lambda x: x["rate"] / 2))
    assert not serialized_ledger.equals(engine.serialized_ledger())


def test_account_mutators_invalidate_serialized_ledger(engine, muted_logger):
    # Identify an account that is in use.
    used_account = engine.ledger.list()['account'].dropna().iloc[0]  # noqa: F841
    account = engine.accounts.list().query("account == @used_account")

    # Using account mutator methods (delete, add) should invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.accounts.delete(account)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.accounts.add(account)
    assert not serialized_ledger.equals(engine.serialized_ledger())


def test_ledger_mutators_invalidate_serialized_ledger(engine):
    ledger = engine.ledger.list().query("id == '1'")

    # Using ledger mutator methods (delete, add, modify) should invalidate the cache.
    serialized_ledger = engine.serialized_ledger()
    engine.ledger.delete(ledger)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.ledger.add(ledger)
    assert not serialized_ledger.equals(engine.serialized_ledger())

    serialized_ledger = engine.serialized_ledger()
    engine.ledger.modify(ledger.assign(description="test description"))
    assert not serialized_ledger.equals(engine.serialized_ledger())
