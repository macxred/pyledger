"""Definition of abstract base class for testing."""

import json
import pandas as pd
from abc import ABC
from io import StringIO
from pyledger import MemoryLedger


TAX_CSV = """
    id,      account, rate,  is_inclusive, description
    EXEMPT,         , 0.00,          True, Exempt from VAT
    OUT_STD,    2200, 0.20,          True, Output VAT at Standard Rate 20%
    OUT_RED,    2200, 0.05,          True, Output VAT at Reduced Rate 5%
    IN_STD,     1300, 0.20,          True, Input VAT at Standard Rate 20%
    IN_RED,     1300, 0.05,          True, Input VAT at Reduced Rate 5%
"""
TAX_CODES = pd.read_csv(StringIO(TAX_CSV), skipinitialspace=True)

ACCOUNT_CSV = """
    group,                       account, currency, tax_code, description
    Assets,                         1000,      USD,         , Cash in Bank USD
    Assets,                         1005,      USD,         , Cash in other Bank USD
    Assets,                         1010,      EUR,         , Cash in Bank EUR
    Assets,                         1015,      EUR,         , Cash in other Bank EUR
    Assets,                         1020,      JPY,         , Cash in Bank JPY
    Assets,                         1025,      CHF,         , Cash in Bank CHF
    Assets,                         1300,      USD,         , VAT Recoverable (Input VAT)
    Liabilities,                    2000,      USD,         , Accounts Payable USD
    Liabilities,                    2010,      USD,         , Accounts Payable EUR
    Liabilities,                    2200,      USD,         , VAT Payable (Output VAT)
    Equity,                         3000,      USD,         , Owner's Equity
    Revenue,                        4000,      USD,  OUT_STD, Sales Revenue - USD
    Revenue,                        4001,      EUR,  OUT_STD, Sales Revenue - EUR
    Expenses,                       5000,      USD,   IN_STD, Purchases
    Expenses/Financial Expenses,    7050,      USD,         , Foreign Exchange Gain/Loss
    Revenue/Financial Gain,         8050,      USD,         , Foreign Exchange Gain
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

# flake8: noqa: E501
LEDGER_CSV = """
    id,       date, account, contra, currency,      amount, report_amount, tax_code, profit_center, description, document
     1, 2024-01-01,    1000,       ,      USD,   800000.00,              ,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
     1,           ,    1010,       ,      EUR,      120.00,        132.82,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
     1,           ,    1020,       ,      JPY, 42000000.00,     298200.00,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
     1,           ,        ,   3000,      USD,  1098332.82,              ,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
     2, 2024-01-24,    1000,   4000,      USD,     1200.00,              ,  OUT_STD,        Bakery, Sell cakes, 2024/receivables/2024-01-24.pdf
     3, 2024-04-12,        ,   1000,      USD,    21288.24,              ,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
     3,           ,    1010,       ,      EUR,    20000.00,      21288.24,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
     4, 2024-05-25,    1010,   5000,      EUR,     -800.00,              ,   IN_STD,          Cafe, Purchase goods, 2024/payables/2024-05-25.pdf
     5, 2024-05-05,    1000,   5000,      USD,     -555.55,              ,   IN_STD,          Cafe, Purchase with tax, 2024/payables/2024-05-05.pdf
     6, 2024-05-06,    1000,   5000,      USD,     -666.66,              ,   IN_RED,          Cafe, Purchase at reduced tax, 2024/payables/2024-05-06.pdf
     7, 2024-05-07,    1000,   5000,      USD,     -777.77,              ,   EXEMPT,        Bakery, Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
     8, 2024-05-08,    1000,       ,      USD,     -999.99,              ,         ,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
     8,           ,        ,   5000,      USD,     -555.55,              ,   IN_STD,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
     8,           ,        ,   5000,      USD,     -444.44,              ,   EXEMPT,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
     9, 2024-07-01,    1010,       ,      EUR,     1500.00,              ,         ,        Bakery, Sale at mixed VAT rate, /invoices/invoice_002.pdf
     9,           ,    4001,       ,      EUR,    -1000.00,              ,  OUT_STD,        Bakery, Sale at mixed VAT rate, /invoices/invoice_002.pdf
     9,           ,        ,   4001,      EUR,      500.00,              ,  OUT_RED,        Bakery, Sale at mixed VAT rate, /invoices/invoice_002.pdf
    10, 2024-07-04,    1020,       ,      JPY, 12345678.00,      76386.36,         ,       General, Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
    10,           ,    1010,       ,      EUR,   -70791.78,     -76386.36,         ,       General, Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
    11, 2024-08-06,    1000,   2000,      USD,      500.00,              ,         ,       General, Payment from customer, 2024/banking/USD_2024-Q2.pdf
    12, 2024-08-07,    1000,   2000,      USD,     -200.00,              ,         ,       General, Payment to supplier,
    13, 2024-08-08,    2000,   1000,      USD,     1000.00,              ,         ,       General, Correction of previous entry,
    14, 2024-08-08,    1010,   2010,      EUR,        0.00,              ,         ,    Restaurant, Zero amount transaction,
    15, 2024-05-24,    1000,       ,      USD,      100.00,              ,         ,    Restaurant, Collective transaction with zero amount,
    15,           ,    1000,       ,      USD,     -100.00,              ,         ,    Restaurant, Collective transaction with zero amount,
    15,           ,    1000,       ,      USD,        0.00,              ,         ,    Restaurant, Collective transaction with zero amount,
    16, 2024-05-24,    1000,   1005,      USD,      100.00,              ,         ,    Restaurant, Collective transaction - leg with debit and credit account,
    16,           ,    1010,       ,      EUR,       20.00,         20.50,         ,    Restaurant, Collective transaction - leg with credit account,
    16,           ,        ,   1015,      EUR,       20.00,         20.50,         ,    Restaurant, Collective transaction - leg with debit account,
    17, 2024-09-09,    1010,   7050,      EUR,        0.00,         -5.55,         ,    Restaurant, Manual Foreign currency adjustment
    18, 2024-09-10,    1020,       ,      JPY,        0.00,          5.55,         ,    Restaurant, Manual Foreign currency adjustment
    18,           ,        ,   8050,      USD,        5.55,              ,         ,    Restaurant, Manual Foreign currency adjustment
    19, 2024-12-01,    1000,   3000,      EUR, 10000000.00,              ,         ,       General, Capital Increase,
    20, 2024-12-02,    1010,   2010,      EUR, 90000000.00,   91111111.10,         ,          Shop, Value 90 Mio USD @1.0123456789 (10 decimal places),
    21, 2024-12-03,    2010,   1010,      EUR, 90000000.00,   91111111.10,         ,          Shop, Revert previous entry,
    22, 2024-12-04,        ,   1000,      USD,  9500000.00,              ,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
    22, 2024-12-04,    1010,       ,      EUR,  9978888.88,    9500000.00,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
    23, 2024-12-05,        ,   1000,      USD,   200000.00,              ,         ,       General, Convert USD to EUR and CHF,
    23,           ,    1010,       ,      EUR,    97750.00,     100000.00,         ,       General, Convert USD to EUR and CHF,
    23,           ,    1025,       ,      CHF, 14285714.29,     100000.00,         ,       General, Convert USD to EUR and CHF,
"""
LEDGER = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)
# flake8: enable

ASSETS_CSV = """
    ticker,  increment, date
       AUD,      0.001, 2023-01-01
       AUD,       0.01, 2024-01-02
       CAD,        0.1,
       CAD,       0.01, 2023-06-02
       TRY,          1, 2022-01-03
       CHF,      0.001,
       CHF,        0.1, 2024-05-04
       EUR,       0.01,
       GBP,       0.01, 2023-07-05
       JPY,          1,
       NZD,      0.001, 2023-03-06
       NOK,        0.1,
       SEK,       0.01, 2023-08-07
       USD,       0.01,
"""
ASSETS = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)

PRICES_CSV = """
          date, ticker,  price, currency
    2023-12-29,    EUR, 1.1068, USD
    2024-03-29,    EUR, 1.0794, USD
    2024-06-28,    EUR, 1.0708, USD
    2024-09-30,    EUR, 1.1170, USD
    2023-12-29,    JPY, 0.0071, USD
    2024-03-29,    JPY, 0.0066, USD
    2024-06-28,    JPY, 0.0062, USD
    2024-09-30,    JPY, 0.0070, USD
    2023-12-29,    CHF,  0.007, USD
"""
PRICES = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)

REVALUATIONS_CSV = """
    date,         account, debit, credit, description
    2024-03-31, 1000:2999,  7050,   8050, FX revaluations
    2024-06-30, 1000:2999,  7050,       , FX revaluations
    2024-09-30, 1000:2999,  7050,       , FX revaluations
    2024-12-31, 1000:2999,  7050,       , FX revaluations
"""
REVALUATIONS = pd.read_csv(StringIO(REVALUATIONS_CSV), skipinitialspace=True)

PROFIT_CENTERS_CSV = """
    profit_center,
             Shop,
          General,
           Bakery,
       Restaurant,
             Cafe,
"""
PROFIT_CENTERS = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)

# flake8: noqa: E501
EXPECTED_BALANCE_CSV = """
    date,         account,            profit_center, balance
    2023-12-31, 1000:9999,                         , "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                         , "{reporting_currency: 0.0, USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:1999,                         , "{reporting_currency: 1098332.82, USD:   800000.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01,      1000,                         , "{reporting_currency:  800000.00, USD:   800000.00}"
    2024-01-01,      1010,                         , "{reporting_currency:     132.82, EUR:      120.00}"
    2024-01-01,      1020,                         , "{reporting_currency:  298200.00, JPY: 42000000.00}"
    2024-01-23,      1000,                         , "{reporting_currency:  800000.00, USD:   800000.00}"
    2024-01-23,      2200,                         , "{reporting_currency:       0.00, USD:        0.00}"
    2024-01-24,      1000,                         , "{reporting_currency:  801200.00, USD:   801200.00}"
    2024-01-24,      2200,                         , "{reporting_currency:    -200.00, USD:     -200.00}"
    2024-03-30, 1000:1999,                         , "{reporting_currency: 1099532.82, USD:   801200.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31, 1000:1999,                         , "{reporting_currency: 1078529.53, USD:   801200.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31,      7050,                         , "{reporting_currency:   21003.29, USD:    21003.29}"
    2024-03-31,      8050,                         , "{reporting_currency:       0.00, USD:        0.00}"
    2023-12-31, 1000:9999,                "General", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2023-12-31, 1000:9999,    "General, Shop, Cafe", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                "General", "{reporting_currency: 0.0, USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,    "General, Shop, Cafe", "{reporting_currency: 0.0, USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                   "Cafe", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-23,      1000,                "General", "{reporting_currency: 800000.0, USD: 800000.0}"
    2024-01-23,      1000,  "General, Shop, Bakery", "{reporting_currency: 800000.0, USD: 800000.0}"
    2024-01-24,      1000,  "General, Shop, Bakery", "{reporting_currency: 801200.0, USD: 801200.0}"
    2024-01-24,      2200,  "General, Shop, Bakery", "{reporting_currency: 0.0, USD: 0.0}"
    2024-03-31, 1000:1999,  "General, Shop, Bakery", "{reporting_currency: 1099532.82, USD: 801200.0, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31, 1000:1999,             "Restaurant", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 1000:9999,             "Restaurant", "{reporting_currency: 0.0, USD: -5.55, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 1000:9999,    "General, Restaurant", "{reporting_currency: 0.0, USD: -498338.37, EUR: 27078.22, JPY: 54345678.0, CHF: 14285714.3}"
"""
EXPECTED_BALANCE = pd.read_csv(StringIO(EXPECTED_BALANCE_CSV), skipinitialspace=True)
EXPECTED_BALANCE["balance"] = (EXPECTED_BALANCE["balance"]
                               .str.replace(r'(\w+):', r'"\1":', regex=True)
                               .apply(json.loads))

def parse_profit_center(value):
    """Function to split values by commas and convert to list"""
    if pd.isna(value) or value.strip() == "":
        return None
    return [item.strip() for item in value.split(",")]
EXPECTED_BALANCE["profit_center"] = EXPECTED_BALANCE["profit_center"].apply(parse_profit_center)
# flake8: enable

class BaseTest(ABC):
    engine = MemoryLedger()
    SETTINGS = {"REPORTING_CURRENCY": "USD"}
    ASSETS = engine.assets.standardize(ASSETS)
    ACCOUNTS = engine.accounts.standardize(ACCOUNTS)
    PRICES = engine.price_history.standardize(PRICES)
    LEDGER_ENTRIES = engine.ledger.standardize(LEDGER)
    TAX_CODES = engine.tax_codes.standardize(TAX_CODES)
    REVALUATIONS = engine.revaluations.standardize(REVALUATIONS)
    PROFIT_CENTERS = engine.profit_centers.standardize(PROFIT_CENTERS)
    EXPECTED_BALANCE = EXPECTED_BALANCE
