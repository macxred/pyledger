"""Constants used throughout the application."""

import pandas as pd
from io import StringIO

FX_ADJUSTMENT_SCHEMA_CSV = """
    column_name,        dtype,                mandatory
    date,               datetime64[ns],       True
    adjust,             string[python],       True
    credit,             Int64,                True
    debit,              Int64,                True
    text,               string[python],       True
"""
FX_ADJUSTMENT_SCHEMA = pd.read_csv(StringIO(FX_ADJUSTMENT_SCHEMA_CSV), skipinitialspace=True)

VAT_CODE_SCHEMA_CSV = """
    column_name,        dtype,                mandatory
    id,                 string[python],       True
    text,               string[python],       True
    inclusive,          bool,                 True
    account,            Int64,                True
    inverse_account,    Int64,                False
    rate,               Float64,              True
    date,               datetime64[ns],       False
"""
VAT_CODE_SCHEMA = pd.read_csv(StringIO(VAT_CODE_SCHEMA_CSV), skipinitialspace=True)

ACCOUNT_SCHEMA_CSV = """
    column_name,        dtype,                mandatory
    group,              string[python],       False
    account,            Int64,                True
    currency,           string[python],       True
    vat_code,           string[python],       False
    text,               string[python],       True
"""
ACCOUNT_SCHEMA = pd.read_csv(StringIO(ACCOUNT_SCHEMA_CSV), skipinitialspace=True)

PRICE_SCHEMA_CSV = """
    column_name,        dtype,                mandatory
    ticker,             string[python],       True
    date,               datetime64[ns],       True
    currency,           string[python],       True
    price,              Float64,              True
"""
PRICE_SCHEMA = pd.read_csv(StringIO(PRICE_SCHEMA_CSV), skipinitialspace=True)

LEDGER_SCHEMA_CSV = """
    column_name,         dtype,                mandatory
    id,                  string[python],       False
    date,                datetime64[ns],       True
    account,             Int64,                True
    counter_account,     Int64,                False
    currency,            string[python],       True
    amount,              Float64,              True
    base_currency_amount,Float64,              False
    vat_code,            string[python],       False
    text,                string[python],       True
    document,            string[python],       False
"""
LEDGER_SCHEMA = pd.read_csv(StringIO(LEDGER_SCHEMA_CSV), skipinitialspace=True)

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
