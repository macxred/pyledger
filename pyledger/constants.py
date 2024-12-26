"""Constants used throughout the application."""

import pandas as pd
from io import StringIO
from consistent_df import enforce_schema

REVALUATION_SCHEMA_CSV = """
    column,             dtype,                mandatory,       id
    date,               datetime64[ns],       True,          True
    account,            string[python],       True,          True
    credit,             Int64,                True,         False
    debit,              Int64,                True,         False
    description,        string[python],       True,         False
"""
REVALUATION_SCHEMA = pd.read_csv(StringIO(REVALUATION_SCHEMA_CSV), skipinitialspace=True)

TAX_CODE_SCHEMA_CSV = """
    column,             dtype,                mandatory,       id
    id,                 string[python],       True,          True
    account,            Int64,                True,         False
    rate,               Float64,              True,         False
    is_inclusive,       bool,                 True,         False
    description,        string[python],       True,         False
    contra,             Int64,                False,        False
"""
TAX_CODE_SCHEMA = pd.read_csv(StringIO(TAX_CODE_SCHEMA_CSV), skipinitialspace=True)

ACCOUNT_SCHEMA_CSV = """
    column,             dtype,                mandatory,       id
    account,            int,                  True,          True
    currency,           string[python],       True,         False
    description,        string[python],       True,         False
    tax_code,           string[python],       False,        False
    group,              string[python],       False,        False
"""
ACCOUNT_SCHEMA = pd.read_csv(StringIO(ACCOUNT_SCHEMA_CSV), skipinitialspace=True)

PRICE_SCHEMA_CSV = """
    column,             dtype,                mandatory,       id
    ticker,             string,               True,          True
    date,               datetime64[ns],       True,          True
    currency,           string,               True,          True
    price,              float,                True,         False
"""
PRICE_SCHEMA = pd.read_csv(StringIO(PRICE_SCHEMA_CSV), skipinitialspace=True)

LEDGER_SCHEMA_CSV = """
    column,              dtype,                mandatory,       id
    id,                  string[python],       False,         True
    date,                datetime64[ns],       True,         False
    account,             Int64,                True,         False
    contra,              Int64,                False,        False
    currency,            string[python],       True,         False
    amount,              Float64,              True,         False
    report_amount,       Float64,              False,        False
    tax_code,            string[python],       False,        False
    cost_center,         string[python],       False,        False
    description,         string[python],       True,         False
    document,            string[python],       False,        False
"""
LEDGER_SCHEMA = pd.read_csv(StringIO(LEDGER_SCHEMA_CSV), skipinitialspace=True)

ASSETS_SCHEMA_CSV = """
    column,              dtype,                mandatory,       id
    ticker,              string,               True,          True
    increment,           float,                True,         False
    date,                datetime64[ns],       False,         True
"""
ASSETS_SCHEMA = pd.read_csv(StringIO(ASSETS_SCHEMA_CSV), skipinitialspace=True)

ASSETS_CSV = """
    ticker, increment,
       AUD,      0.01,
       CAD,      0.01,
       CHF,      0.01,
       EUR,      0.01,
       GBP,      0.01,
       JPY,      1.00,
       NZD,      0.01,
       NOK,      0.01,
       SEK,      0.01,
       USD,      0.01,
"""

COST_CENTER_SCHEMA_CSV = """
    column,        dtype,   mandatory,   id
    cost_center,  string,        True,   True
"""
COST_CENTER_SCHEMA = pd.read_csv(StringIO(COST_CENTER_SCHEMA_CSV), skipinitialspace=True)

DEFAULT_PRECISION = 0.01

DEFAULT_ASSETS = enforce_schema(
    pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True), ASSETS_SCHEMA
)

DEFAULT_SETTINGS = {"reporting_currency": "USD"}
