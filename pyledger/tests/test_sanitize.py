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


def test_sanitize_tax_codes(engine, capture_logs):
    ACCOUNT_CSV = """
        group,         account, currency, tax_code, description
        Assets,           1000,      USD,         , Cash in Bank USD
        Liabilities,      2000,      USD,         , Accounts Payable
    """
    TAX_CSV = """
        id,                   account,  rate,  is_inclusive,    description,             contra
        EXEMPT,                      ,  0.00,          True,    Exempt from VAT,
        INVALID_RATE,            1000,  1.50,         False,    Invalid rate tax code,   1000
        NON_EXISTENT_ACCOUNT,    9999,  0.10,         False,    Account does not exist,  2000
        NON_EXISTENT_CONTRA,     1000,  0.10,         False,    Contra does not exist,   9999
        NON_EXISTENT_ACCOUNTS,       ,  0.10,         False,    Accounts do not exist,
        MISSING_CONTRA,          1000,  0.10,         False,    Missing contra,
        MISSING_ACCOUNT,             ,  0.10,         False,    Missing account,         1000
        VALID,                   1000,  0.05,         False,    Valid tax code,          2000
    """
    EXPECTED_TAX_CSV = """
        id,          account,  rate,  is_inclusive,  description,      contra
        EXEMPT,             ,  0.00,          True,  Exempt from VAT,
        MISSING_CONTRA, 1000,  0.10,         False,  Missing contra,
        MISSING_ACCOUNT,    ,  0.10,         False,  Missing account,  1000
        VALID,          1000,  0.05,         False,  Valid tax code,   2000
    """
    accounts = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    tax_codes = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)
    expected_tax_codes = pd.read_csv(StringIO(EXPECTED_TAX_CSV), skipinitialspace=True)
    tax_codes = engine.tax_codes.standardize(tax_codes)

    # Test sanitize process with specified accounts DataFrame
    sanitized_tax_codes = engine.sanitize_tax_codes(tax_codes, accounts=accounts)
    assert_frame_equal(engine.tax_codes.standardize(expected_tax_codes), sanitized_tax_codes)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0

    # Test sanitize process with populated accounts
    engine.restore(accounts=accounts)
    sanitized_tax_codes = engine.sanitize_tax_codes(tax_codes)
    assert_frame_equal(engine.tax_codes.standardize(expected_tax_codes), sanitized_tax_codes)


def test_sanitize_accounts(engine, capture_logs):
    TAX_CSV = """
        id,      account, rate,  is_inclusive,           description
        EXEMPT,         , 0.00,          True, Exempt from VAT
        OUT_STD,    2200, 0.20,          True, Output VAT at Standard Rate 20%
    """
    ACCOUNT_CSV = """
        group,         account, currency, tax_code, description
        Assets,           1000,      USD,   EXEMPT, VALID_CURR
        Assets,           2001,      XXX,   EXEMPT, INVALID_CURR
        Liabilities,      2002,      USD,         , NO_TAX_CODE
        Revenue,          3000,      USD,  MISSING, INVALID_TAX_CODE
    """
    EXPECTED_ACCOUNT_CSV = """
        group,         account, currency, tax_code, description
        Assets,           1000,      USD,   EXEMPT, VALID_CURR
        Liabilities,      2002,      USD,         , NO_TAX_CODE
        Revenue,          3000,      USD,         , INVALID_TAX_CODE
    """

    tax_codes = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)
    accounts = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    expected = pd.read_csv(StringIO(EXPECTED_ACCOUNT_CSV), skipinitialspace=True)
    standardized_accounts = engine.accounts.standardize(accounts)

    # Test sanitize process with specified tax codes DataFrame
    sanitized = engine.sanitize_accounts(standardized_accounts, tax_codes=tax_codes)
    assert_frame_equal(engine.accounts.standardize(expected), sanitized)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0

    # Test sanitize process with populated system tax codes
    engine.restore(tax_codes=tax_codes)
    sanitized = engine.sanitize_accounts(standardized_accounts)
    assert_frame_equal(engine.accounts.standardize(expected), sanitized)


def test_sanitized_accounts_tax_codes(engine, capture_logs):
    TAX_CSV = """
        id,             account, rate,  is_inclusive,    description,            contra
        EXEMPT,                , 0.00,          True,    Exempt from VAT,
        INVALID_RATE,      1000, 1.50,        False,    Invalid rate tax code,  1000
        MISSING_ACCOUNT,   9999, 0.10,        False,    Account does not exist, 2002
        VALID,             1000, 0.05,        False,    Valid tax code,         2002
    """
    ACCOUNT_CSV = """
        group,          account, currency,         tax_code, description
        Assets,            1000,      USD,           VALID, VALID_CURR
        Assets,            2001,      XXX,          EXEMPT, INVALID_CURR
        Liabilities,       2002,      USD,                , NO_TAX_CODE
        Revenue,           3000,      USD, MISSING_ACCOUNT, INVALID_TAX_CODE
    """
    EXPECTED_ACCOUNT_CSV = """
        group,          account, currency, tax_code,         description
        Assets,            1000,      USD,      VALID, VALID_CURR
        Liabilities,       2002,      USD,           , NO_TAX_CODE
        Revenue,           3000,      USD,           , INVALID_TAX_CODE
    """
    EXPECTED_TAX_CSV = """
        id,            account, rate,  is_inclusive,    description,      contra
        EXEMPT,               , 0.00,          True,    Exempt from VAT,
        VALID,            1000, 0.05,         False,    Valid tax code,   2002
    """

    tax_df = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)
    accounts_df = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    expected_accounts_df = pd.read_csv(StringIO(EXPECTED_ACCOUNT_CSV), skipinitialspace=True)
    expected_accounts_df = engine.accounts.standardize(expected_accounts_df)
    expected_tax_df = pd.read_csv(StringIO(EXPECTED_TAX_CSV), skipinitialspace=True)
    expected_tax_df = engine.tax_codes.standardize(expected_tax_df)
    engine.restore(accounts=accounts_df, tax_codes=tax_df)

    final_accounts_df, final_tax_codes_df = engine.sanitized_accounts_tax_codes()
    assert_frame_equal(expected_accounts_df, final_accounts_df, check_like=True)
    assert_frame_equal(expected_tax_df, final_tax_codes_df)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) > 0


def test_sanitize_journal(engine, capture_logs):
    ACCOUNT_CSV = """
        group,          account, currency,   tax_code,   description
        Assets,            1000,      USD,           , VALID_CURR
        Assets,            1100,      JPY,           , VALID_CURR
        Assets,            1200,      CHF,           , VALID_CURR
        Assets,            1300,      CHF,           , VALID_CURR
        Assets,            1400,      CHF,           , VALID_CURR
        Liabilities,       2000,      USD,           , NO_TAX_CODE
        Revenue,           3000,      USD,           , INVALID_TAX_CODE
    """
    TAX_CSV = """
        id,            account, rate,  is_inclusive,    description,      contra
        EXEMPT,               , 0.00,          True,    Exempt from VAT,
        VALID,            1000, 0.05,          True,    Valid tax code,
    """
    ASSETS_CSV = """
        ticker, increment,      date
        EUR,        0.01, 2022-01-01
        JPY,        1.00, 2022-01-02
        USD,        0.01, 2022-01-01
        CHF,        0.01, 2022-01-01
    """
    PRICES_CSV = """
        date,       ticker,  price, currency
        2023-12-28,    EUR, 1.1068, USD
        2023-12-29,    JPY, 0.0071, USD
        2023-12-29,    CHF, 1.1353, USD
    """
    PROFIT_CENTERS_CSV = """
        profit_center,
                Shop,
    """
    # flake8: noqa: E501
    JOURNAL_CSV = """
        id,        date, account, contra, currency,      amount, report_amount, profit_center,  tax_code, description, document
         1,  2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Invalid date,
         1,  2024-01-02,    1000,       ,      USD,   800000.00,              ,              ,          , Invalid date,
         2,  2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Valid,
         2,  2024-01-01,        ,   1000,      USD,   800000.00,              ,              ,          , Valid,
         3,  2024-01-01,    1000,   2000,      USD,   800000.00,              ,              ,   Invalid, Invalid tax code set to NA,
         4,  2024-01-01,    1000,   2000,      USD,   800000.00,              ,              ,     VALID, Valid tax code,
         5,  2024-01-01,        ,       ,      USD,   800000.00,              ,              ,          , No account or contra,
         6,  2024-01-01,    1111,       ,      USD,   800000.00,              ,              ,          , Invalid account reference,
         7,  2024-01-01,    2222,       ,      USD,   800000.00,              ,              ,          , Invalid contra reference,
         8,  2024-01-01,    1000,       ,      AAA,   800000.00,              ,              ,          , Invalid currency,
         9,  2024-01-01,    1000,       ,      CHF,           0,              ,              ,          , Currencies mismatch valid with amount 0,
         10, 2024-01-01,        ,   1000,      CHF,           0,              ,              ,          , Currencies mismatch valid with amount 0,
         11, 2024-01-01,    1200,       ,      JPY,           1,              ,              ,          , Currencies mismatch on account,
         11, 2024-01-01,        ,   1000,      USD,           1,              ,              ,          , Currencies mismatch on account,
         12, 2024-01-01,    1000,       ,      USD,           1,              ,              ,          , Currencies mismatch on account,
         12, 2024-01-01,        ,   1200,      JPY,           1,              ,              ,          , Currencies mismatch on account,
         13, 2024-01-01,    1000,   2000,      CHF,           1,              ,              ,          , Currencies mismatch on account in reporting currency,
         14, 2021-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , No price reference,
         15, 2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Not balanced amount,
         16, 2024-01-01,        ,   1000,      USD,   800000.00,              ,              ,          , Not balanced amount,
         17, 2024-01-01,        ,   1000,      USD,   800000.00,              ,              ,          , Not balanced amount,
         17, 2024-01-01,        ,   1000,      USD,   800000.00,              ,              ,          , Not balanced amount,
         18, 2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Not balanced amount,
         18, 2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Not balanced amount,
         19, 2024-01-01,    1000,       ,      USD,   800000.00,              ,              ,          , Not balanced amount,
         19, 2024-01-01,        ,   1000,      USD,   100000.00,              ,              ,          , Not balanced amount,
         20, 2024-01-01,        ,   1000,      USD,   800000.00,              ,              ,          , Not balanced amount,
         20, 2024-01-01,    1200,       ,      CHF,   100000.00,              ,              ,          , Not balanced amount,
         21, 2024-01-01,    1000,       ,      USD,     -999.99,              ,              ,          , Balanced amount,
         21, 2024-01-01,        ,   2000,      USD,     -555.55,              ,              ,          , Balanced amount,
         21, 2024-01-01,        ,   2000,      USD,     -444.44,              ,              ,          , Balanced amount,
         22, 2024-01-01,        ,   1000,      USD,   400000.00,              ,              ,          , Not balanced amount,
         22, 2024-01-01,    1000,       ,      USD,   200000.00,              ,              ,          , Not balanced amount,
         22, 2024-01-01,    1000,       ,      USD,   100000.00,              ,              ,          , Not balanced amount,
         23, 2024-01-01,        ,   1100,      JPY,   400000.00,              ,              ,          , Balanced amount,
         23, 2024-01-01,    1200,       ,      CHF,   200000.00,              ,              ,          , Balanced amount,
         23, 2024-01-01,    1000,       ,      USD,   217660.00,              ,              ,          , Balanced amount,
         23, 2024-01-01,        ,   1000,      USD,   441880.00,              ,              ,          , Balanced amount,
         24, 2024-01-01,        ,   1100,      JPY,   400000.00,              ,              ,          , Not balanced amount,
         24, 2024-01-01,    1200,       ,      CHF,   200000.00,              ,              ,          , Not balanced amount,
         24, 2024-01-01,    1000,       ,      USD,     1000.00,              ,              ,          , Not balanced amount,
         24, 2024-01-01,        ,   1000,      USD,     8560.00,              ,              ,          , Not balanced amount,
         25, 2024-01-01,    1000,   2000,      USD,   800000.00,              ,          Shop,          , Valid profit center,
         26, 2024-01-01,    1000,   2000,      USD,   800000.00,              ,       INVALID,          , Invalid profit center,
         # Transactions with a single non-reporting currency that are balanced in their own currency
         # but imbalanced in the reporting currency due to rounding errors should pass the sanitization process.
         27, 2024-12-31,        ,   1200,      CHF,     1999.99,              ,              ,          , Balanced amount,
         27, 2024-12-31,    1300,       ,      CHF,     1000.99,              ,              ,          , Balanced amount,
         27, 2024-12-31,    1400,       ,      CHF,      999.00,              ,              ,          , Balanced amount,
         28, 2024-12-31,    1200,       ,      CHF,     1999.99,              ,              ,          , Balanced amount,
         28, 2024-12-31,        ,   1300,      CHF,     1000.99,              ,              ,          , Balanced amount,
         28, 2024-12-31,        ,   1400,      CHF,      999.00,              ,              ,          , Balanced amount,
    """
    EXPECTED_JOURNAL_CSV = """
        id,        date, account, contra, currency,      amount, report_amount, profit_center, tax_code, description, document
         2,  2024-01-01,    1000,       ,      USD,   800000.00,     800000.00,              ,         , Valid,
         2,  2024-01-01,        ,   1000,      USD,   800000.00,     800000.00,              ,         , Valid,
         3,  2024-01-01,    1000,   2000,      USD,   800000.00,     800000.00,              ,         , Invalid tax code set to NA,
         4,  2024-01-01,    1000,   2000,      USD,   800000.00,     800000.00,              ,    VALID, Valid tax code,
         9,  2024-01-01,    1000,       ,      CHF,           0,             0,              ,         , Currencies mismatch valid with amount 0,
         10, 2024-01-01,        ,   1000,      CHF,           0,             0,              ,         , Currencies mismatch valid with amount 0,
         13, 2024-01-01,    1000,   2000,      CHF,           1,          1.14,              ,         , Currencies mismatch on account in reporting currency,
         21, 2024-01-01,    1000,       ,      USD,     -999.99,       -999.99,              ,         , Balanced amount,
         21, 2024-01-01,        ,   2000,      USD,     -555.55,       -555.55,              ,         , Balanced amount,
         21, 2024-01-01,        ,   2000,      USD,     -444.44,       -444.44,              ,         , Balanced amount,
         23, 2024-01-01,        ,   1100,      JPY,   400000.00,       2840.00,              ,         , Balanced amount,
         23, 2024-01-01,    1200,       ,      CHF,   200000.00,     227060.00,              ,         , Balanced amount,
         23, 2024-01-01,    1000,       ,      USD,   217660.00,     217660.00,              ,         , Balanced amount,
         23, 2024-01-01,        ,   1000,      USD,   441880.00,     441880.00,              ,         , Balanced amount,
         27, 2024-12-31,        ,   1200,      CHF,     1999.99,       2270.59,              ,         , Balanced amount,
         27, 2024-12-31,    1300,       ,      CHF,     1000.99,       1136.42,              ,         , Balanced amount,
         27, 2024-12-31,    1400,       ,      CHF,      999.00,       1134.17,              ,         , Balanced amount,
         28, 2024-12-31,    1200,       ,      CHF,     1999.99,       2270.59,              ,         , Balanced amount,
         28, 2024-12-31,        ,   1300,      CHF,     1000.99,       1136.42,              ,         , Balanced amount,
         28, 2024-12-31,        ,   1400,      CHF,      999.00,       1134.17,              ,         , Balanced amount,
    """
    EXPECTED_JOURNAL_WITH_PROFIT_CENTERS_CSV = """
        id,        date, account, contra, currency,      amount, report_amount, profit_center, tax_code, description, document
         25, 2024-01-01,    1000,   2000,      USD,   800000.00,     800000.00,          Shop,         , Valid profit center,
    """
    prices = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)
    assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
    tax_df = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)
    accounts_df = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    expected_journal = pd.read_csv(StringIO(EXPECTED_JOURNAL_CSV), skipinitialspace=True)
    expected_journal_with_profit_centers = pd.read_csv(
        StringIO(EXPECTED_JOURNAL_WITH_PROFIT_CENTERS_CSV), skipinitialspace=True
    )
    # Read the CSV while ignoring commented lines
    journal_lines = JOURNAL_CSV.strip().split("\n")
    journal_lines = [line for line in journal_lines if not line.strip().startswith("#")]
    JOURNAL_CSV = "\n".join(journal_lines)
    journal = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)
    engine.restore(accounts=accounts_df, tax_codes=tax_df, price_history=prices, assets=assets)

    # Sanitize journal entries without profit centers
    expected_journal_df = engine.journal.standardize(expected_journal)
    sanitized = engine.sanitize_journal(engine.journal.standardize(journal))
    assert_frame_equal(expected_journal_df, sanitized)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) == 8, "Expected strict number of captured logs"

    # Clear captured logs
    capture_logs.seek(0)
    capture_logs.truncate(0)

    # Sanitize journal entries with profit centers
    PROFIT_CENTERS = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)
    engine.restore(profit_centers=PROFIT_CENTERS)
    expected_journal_df = engine.journal.standardize(expected_journal_with_profit_centers)
    sanitized = engine.sanitize_journal(engine.journal.standardize(journal))
    assert_frame_equal(expected_journal_df, sanitized)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) == 7, "Expected strict number of captured logs"


def test_sanitize_reconciliation(engine, capture_logs):
    ACCOUNT_CSV = """
        group,                       account, currency, tax_code, description
        Assets,                         1000,      USD,         , Cash in Bank USD
        Assets,                         1005,      CHF,         , Cash in Bank CHF
        Assets,                         1010,      EUR,         , Cash in Bank EUR
    """
    ASSETS_CSV = """
        ticker, increment,      date
        EUR,        0.001, 2022-01-01
        USD,        0.01, 2022-01-01
        CHF,        0.0001, 2022-01-01
    """
    PROFIT_CENTERS_CSV = """
        profit_center,
                Shop,
    """
    RECONCILIATION_CSV = """
        period,         account, currency,          profit_center,       balance,  report_balance, tolerance, document
        2023-12-3441,      1000,      CHF,                       ,          0.00,            0.00,          , Invalid date
        2023-kkk,          1000,      CHF,                       ,          0.00,            0.00,          , Invalid date
                ,          1000,      CHF,                       ,          0.00,            0.00,          , Missing date
        2024-01-23,        1000,      EUR,                       ,          0.00,            0.00,      0.01, Valid row
        2024-01-23,   1000:2999,      EUR,                       ,          0.00,            0.00,      0.01, Valid account period
        2024-01-23,    1000-hhh,      EUR,                       ,          0.00,            0.00,      0.01, Invalid account period
        2024-01-23,   4000:9999,      EUR,                       ,          0.00,            0.00,      0.01, Invalid account reference
        2024-01-23,        1000,         ,                       ,          0.00,            0.00,      0.01, Missing currency reference
        2024-01-23,        1000,      AAA,                       ,          0.00,                ,      0.01, Invalid currency and missing balance
        2024-01-23,        1000,      AAA,                       ,          3.44,            0.00,      0.01, Set currency balance to NA
        2024-01-23,        1000,      EUR,                       ,              ,                ,      0.01, Both missing balances
        2024-08,      1000:2999,      CHF,              "General",          0.00,            0.00,      0.01, Invalid profit center reference
        2024,              1000,      CHF,                 "Shop",          0.00,            0.00,      0.01, Valid profit center reference
        2024-01-23,        1000,      EUR,                       ,          1.00,            2.00,          , Set lowest tolerance
        2024-01-23,        1000,      EUR,                       ,              ,            2.00,          , Set lowest tolerance
        2024-01-23,        1000,      EUR,                       ,          1.00,                ,          , Set lowest tolerance
    """
    EXPECTED_RECONCILIATION_CSV = """
        period,         account, currency,          profit_center,       balance,  report_balance, tolerance, document
        2024-01-23,        1000,      EUR,                       ,          0.00,            0.00,      0.01, Valid row
        2024-01-23,   1000:2999,      EUR,                       ,          0.00,            0.00,      0.01, Valid account period
        2024-01-23,        1000,         ,                       ,          0.00,            0.00,      0.01, Missing currency reference
        2024-01-23,        1000,         ,                       ,              ,            0.00,      0.01, Set currency balance to NA
        2024,              1000,      CHF,                 "Shop",          0.00,            0.00,      0.01, Valid profit center reference
        2024-01-23,        1000,      EUR,                       ,          1.00,            2.00,      0.00, Set lowest tolerance
        2024-01-23,        1000,      EUR,                       ,              ,            2.00,      0.00, Set lowest tolerance
        2024-01-23,        1000,      EUR,                       ,          1.00,                ,      0.00, Set lowest tolerance
    """

    assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
    accounts = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    reconciliation = pd.read_csv(StringIO(RECONCILIATION_CSV), skipinitialspace=True)
    profit_centers = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)
    expected_reconciliation = pd.read_csv(StringIO(EXPECTED_RECONCILIATION_CSV), skipinitialspace=True)
    engine.restore(
        accounts=accounts, assets=assets, profit_centers=profit_centers,
        configuration={"reporting_currency": "CHF"}
    )

    sanitized_reconciliation = engine.sanitize_reconciliation(
        engine.reconciliation.standardize(reconciliation)
    )
    assert_frame_equal(
        engine.reconciliation.standardize(expected_reconciliation), sanitized_reconciliation
    )
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) == 6


def test_sanitize_target_balance(engine, capture_logs):
    ACCOUNT_CSV = """
        group,          account, currency,   tax_code,   description
        Assets,            1000,      USD,           , VALID_CURR
        Assets,            1100,      JPY,           , VALID_CURR
        Assets,            1200,      CHF,           , VALID_CURR
        Assets,            2000,      USD,           , VALID_CURR
    """
    TAX_CSV = """
        id,            account, rate,  is_inclusive,    description,      contra
        EXEMPT,               , 0.00,          True,    Exempt from VAT,
        VALID,            1000, 0.05,          True,    Valid tax code,
    """
    ASSETS_CSV = """
        ticker, increment,      date
        EUR,        0.01, 2022-01-01
        JPY,        1.00, 2022-01-02
        USD,        0.01, 2022-01-01
        CHF,        0.01, 2022-01-01
    """
    PRICES_CSV = """
        date,       ticker,  price, currency
        2023-12-28,    EUR, 1.1068, USD
        2023-12-29,    JPY, 0.0071, USD
        2023-12-29,    CHF, 1.1353, USD
    """
    PROFIT_CENTERS_CSV = """
        profit_center,
                Shop,
    """
    # flake8: noqa: E501
    TARGET_BALANCE_CSV = """
        id,        date, account, contra, currency, profit_center,         description, document, balance, lookup_period, lookup_accounts, lookup_profit_centers
         1,  2024-01-01,    1000,   2000,      USD,              ,     invalid balance,         ,        ,          2024,            1000,
         2,  2024-01-01,    1000,   2000,      USD,              ,       valid balance,         ,       1,          2024,            1000,
         3,  2024-01-01,    1000,   2000,      USD,              ,      invalid period,         ,       1,        period,            1000,
         4,  2024-01-01,    1000,   2000,      USD,              ,      invalid period,         ,       1,       33434:4,            1000,
         5,  2024-01-01,    1000,   2000,      USD,              ,        valid period,         ,       1,       2024-Q4,            1000,
         6,  2024-01-01,    1000,   2000,      USD,              ,    invalid accounts,         ,       1,          2024,            cccc,
         7,  2024-01-01,    1000,   2000,      USD,              ,    invalid accounts,         ,       1,          2024,         1000:df,
         8,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,       1000:9999,
         9,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,  1000:9999-1200,
        10,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,  3000:9999+1200,
        11,  2024-01-01,    1000,   2000,      USD,          Shop, invalid profit cntr,         ,       1,       2024-Q4,            1000, invalid_pc
        12,  2024-01-01,    1000,   2000,      USD,          Shop,   valid profit cntr,         ,       1,       2024-Q4,            1000,
        13,  2024-01-01,    1000,   2000,      USD,          Shop,   valid profit cntr,         ,       1,       2024-Q4,            1000, Shop
    """
    EXPECTED_TARGET_BALANCE_CSV = """
        id,        date, account, contra, currency, profit_center,         description, document, balance, lookup_period, lookup_accounts, lookup_profit_centers
         2,  2024-01-01,    1000,   2000,      USD,              ,       valid balance,         ,       1,          2024,            1000,
         5,  2024-01-01,    1000,   2000,      USD,              ,        valid period,         ,       1,       2024-Q4,            1000,
         8,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,       1000:9999,
         9,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,  1000:9999-1200,
        10,  2024-01-01,    1000,   2000,      USD,              ,      valid accounts,         ,       1,          2024,  3000:9999+1200,
    """
    EXPECTED_TARGET_BALANCE_WITH_PROFIT_CENTERS_CSV = """
        id,        date, account, contra, currency, profit_center,         description, document, balance, lookup_period, lookup_accounts, lookup_profit_centers
        12,  2024-01-01,    1000,   2000,      USD,          Shop,   valid profit cntr,         ,       1,       2024-Q4,            1000,
        13,  2024-01-01,    1000,   2000,      USD,          Shop,   valid profit cntr,         ,       1,       2024-Q4,            1000, Shop
    """
    tax_df = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)
    prices = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)
    assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
    accounts_df = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    profit_centers = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)
    expected_target_balance = pd.read_csv(StringIO(EXPECTED_TARGET_BALANCE_CSV), skipinitialspace=True)
    target_balance = pd.read_csv(StringIO(TARGET_BALANCE_CSV), skipinitialspace=True)
    engine.restore(
        accounts=accounts_df, tax_codes=tax_df, price_history=prices, assets=assets
    )

    # Sanitize target balance without profit centers
    expected_target_balance = engine.target_balance.standardize(expected_target_balance)
    sanitized = engine.sanitize_target_balance(engine.target_balance.standardize(target_balance))
    assert_frame_equal(expected_target_balance, sanitized)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) == 5, "Expected strict number of captured logs"

    # Clear captured logs
    capture_logs.seek(0)
    capture_logs.truncate(0)

    # Sanitize target balance with profit centers
    engine.restore(profit_centers=profit_centers)
    expected_target_balance = engine.target_balance.standardize(
        pd.read_csv(StringIO(EXPECTED_TARGET_BALANCE_WITH_PROFIT_CENTERS_CSV), skipinitialspace=True)
    )
    sanitized = engine.sanitize_target_balance(engine.target_balance.standardize(target_balance))
    assert_frame_equal(expected_target_balance, sanitized)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert len(log_messages) == 5, "Expected strict number of captured logs"

def test_sanitize_profit_center(engine, capture_logs):
    PROFIT_CENTER_CSV = """
        profit_center
        SALES
        +MARKETING
        FINANCE
        DEV+OPS
        HR
    """
    EXPECTED_CSV = """
        profit_center
        SALES
        FINANCE
        HR
    """

    profit_centers = pd.read_csv(StringIO(PROFIT_CENTER_CSV), skipinitialspace=True)
    expected = pd.read_csv(StringIO(EXPECTED_CSV), skipinitialspace=True)
    sanitized = engine.sanitize_profit_center(profit_centers)
    assert_frame_equal(expected.reset_index(drop=True), sanitized, check_dtype=False)
    log_messages = capture_logs.getvalue().strip().split("\n")
    assert any("Discarding" in msg and "+" in msg for msg in log_messages)
