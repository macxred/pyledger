"""Test suit for sanitize_xy methods."""

from io import StringIO
import logging
import pandas as pd
import pytest
from pyledger import MemoryLedger
from consistent_df import assert_frame_equal


@pytest.fixture
def capture_logs():
    """Fixture to capture logs during the test."""
    logger = logging.getLogger("ledger")
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger.addHandler(handler)
    original_level = logger.level
    logger.setLevel(logging.WARNING)
    yield log_stream
    logger.setLevel(original_level)
    logger.removeHandler(handler)
    log_stream.close()


@pytest.fixture
def engine():
    return MemoryLedger()


def test_sanitize_assets(engine, capture_logs):
    ASSETS_CSV = """
        ticker, increment,      date
        AAPL,       -0.01, 2024-01-01
        MSFT,        0.00, 2024-01-02
        GOOG,        0.01, 2024-01-04
        NFLX,        0.01,        NaT
        TSLA,       -1.00,        NaT
    """
    EXPECTED_CSV = """
        ticker, increment,      date
        GOOG,        0.01, 2024-01-04
        NFLX,        0.01,        NaT
    """

    assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
    expected_assets = pd.read_csv(StringIO(EXPECTED_CSV), skipinitialspace=True)
    sanitized_assets = engine.sanitize_assets(engine.assets.standardize(assets))

    assert_frame_equal(engine.assets.standardize(expected_assets), sanitized_assets)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0


def test_sanitize_prices(engine, capture_logs):
    PRICES_CSV = """
        ticker, currency,      date,      price
        AAPL,    USD,    2024-01-01,    150.0
        MSFT,    AAA,    2024-01-03,    250.0
        TSLA,    USD,    2024-01-03,    800.0
        GOOG,    EUR,    2024-01-04,    300.0
        NFLX,    USD,    2024-01-02,    120.0
    """
    ASSETS_CSV = """
        ticker, increment,      date
        AAPL,        0.01, 2024-01-01
        MSFT,        1.00, 2024-01-02
        GOOG,        0.01, 2024-01-04
        NFLX,        0.01,        NaT
    """
    EXPECTED_CSV = """
        ticker, currency,      date,      price
        AAPL,    USD,    2024-01-01,    150.0
        GOOG,    EUR,    2024-01-04,    300.0
        NFLX,    USD,    2024-01-02,    120.0
    """

    prices = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)
    assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
    expected_prices = pd.read_csv(StringIO(EXPECTED_CSV), skipinitialspace=True)
    engine.restore(assets=assets)

    sanitized_prices = engine.sanitize_prices(engine.price_history.standardize(prices))
    assert_frame_equal(engine.price_history.standardize(expected_prices), sanitized_prices)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0


def test_sanitize_revaluations(engine, capture_logs):
    ACCOUNT_CSV = """
        group,                       account, currency, tax_code, description
        Assets,                         1000,      USD,         , Cash in Bank USD
        Assets,                         1005,      USD,         , Cash in other Bank USD
        Assets,                         1010,      EUR,         , Cash in Bank EUR
        Assets,                         1015,      EUR,         , Cash in other Bank EUR
        Assets,                         1300,      USD,         , VAT Recoverable (Input VAT)
        Assets,                         1310,      AAA,         , VAT Recoverable (Input VAT)
        Expenses/Financial Expenses,    7050,      USD,         , Foreign Exchange Gain/Loss
        Revenue/Financial Gain,         8050,      USD,         , Foreign Exchange Gain
    """
    PRICES_CSV = """
        date,       ticker,  price, currency
        2023-12-28,    EUR, 1.1068, USD
    """
    REVALUATIONS_CSV = """
        date,         account, debit, credit, description
                  , 1000:1300,  7050,   8050, Invalid date
        2023-12-29, 1000:1300,      ,       , No credit nor debit specified
        2023-12-29, 1000:1300,  7777,       , Not valid debit
        2023-12-29, 1000:1300,      ,   8888, Not valid credit
        2023-12-29, 1000:1310,      ,   8050, Invalid currency
        2020-12-29, 1000:1300,      ,   8050, No price definition
        2023-12-29, 1000:1300,      ,   8050, Correct revaluation
        2023-12-29, 1000:1300,  7050,       , Correct revaluation
        2023-12-29, 1000:1300,  7050,   8050, Correct revaluation
    """
    EXPECTED_REVALUATIONS_CSV = """
        date,         account, debit, credit, description
        2023-12-29, 1000:1300,      ,   8050, Correct revaluation
        2023-12-29, 1000:1300,  7050,       , Correct revaluation
        2023-12-29, 1000:1300,  7050,   8050, Correct revaluation
    """

    prices = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)
    accounts = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    revaluations = pd.read_csv(StringIO(REVALUATIONS_CSV), skipinitialspace=True)
    expected_revaluations = pd.read_csv(StringIO(EXPECTED_REVALUATIONS_CSV), skipinitialspace=True)
    engine.restore(accounts=accounts, price_history=prices)

    sanitized_revaluations = engine.sanitize_revaluations(
        engine.revaluations.standardize(revaluations)
    )
    assert_frame_equal(
        engine.revaluations.standardize(expected_revaluations), sanitized_revaluations
    )
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0
