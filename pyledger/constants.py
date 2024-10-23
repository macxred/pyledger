"""Constants used throughout the application."""

import pandas as pd
from io import StringIO

REVALUATION_SCHEMA_CSV = """
    column,             dtype,                mandatory
    date,               datetime64[ns],       True
    account,            string,               True
    credit,             int64,                True
    debit,              Int64,                False
    description,        string,               True
    price,              Float64,              False
"""
REVALUATION_SCHEMA = pd.read_csv(StringIO(REVALUATION_SCHEMA_CSV), skipinitialspace=True)

TAX_CODE_SCHEMA_CSV = """
    column,             dtype,                mandatory
    id,                 string[python],       True
    account,            Int64,                True
    rate,               Float64,              True
    is_inclusive,       bool,                 True
    description,        string[python],       True
    contra,             Int64,                False
"""
TAX_CODE_SCHEMA = pd.read_csv(StringIO(TAX_CODE_SCHEMA_CSV), skipinitialspace=True)

ACCOUNT_SCHEMA_CSV = """
    column,             dtype,                mandatory
    account,            int,                  True
    currency,           string[python],       True
    description,        string[python],       True
    tax_code,           string[python],       False
    group,              string[python],       False
"""
ACCOUNT_SCHEMA = pd.read_csv(StringIO(ACCOUNT_SCHEMA_CSV), skipinitialspace=True)

PRICE_SCHEMA_CSV = """
    column,             dtype,                mandatory
    ticker,             string,               True
    date,               datetime64[ns],       True
    currency,           string,               True
    price,              float64,              True
"""
PRICE_SCHEMA = pd.read_csv(StringIO(PRICE_SCHEMA_CSV), skipinitialspace=True)

LEDGER_SCHEMA_CSV = """
    column,              dtype,                mandatory
    id,                  string[python],       False
    date,                datetime64[ns],       True
    account,             Int64,                True
    contra,              Int64,                False
    currency,            string[python],       True
    amount,              Float64,              True
    report_amount,       Float64,              False
    tax_code,            string[python],       False
    description,         string[python],       True
    document,            string[python],       False
"""
LEDGER_SCHEMA = pd.read_csv(StringIO(LEDGER_SCHEMA_CSV), skipinitialspace=True)

ASSETS_SCHEMA_CSV = """
    column,             dtype,                mandatory
    ticker,             string,               True
    increment,          float64,              True
    date,               datetime64[ns],       False
"""
ASSETS_SCHEMA = pd.read_csv(StringIO(ASSETS_SCHEMA_CSV), skipinitialspace=True)


CURRENCY_PRECISION = {
    "AUD": 0.01,
    "CAD": 0.01,
    "CHF": 0.01,
    "EUR": 0.01,
    "GBP": 0.01,
    "JPY": 1.00,
    "NZD": 0.01,
    "NOK": 0.01,
    "SEK": 0.01,
    "USD": 0.01,
}

DEFAULT_SETTINGS = {
    "reporting_currency": "USD",
    "precision": CURRENCY_PRECISION
}
