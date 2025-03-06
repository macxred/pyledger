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
    group,                                 account, currency, tax_code, description
    Assets/Cash/Bank of America,              1000,      USD,         , Cash in Bank USD
    Assets/Cash/Other Bank,                   1005,      USD,         , Cash in other Bank USD
    Assets/Cash/Deutsche Bank,                1010,      EUR,         , Cash in Bank EUR
    Assets/Cash/Other Bank,                   1015,      EUR,         , Cash in other Bank EUR
    Assets/Cash/Mitsubishi UFJ,               1020,      JPY,         , Cash in Bank JPY
    Assets/Cash/UBS,                          1025,      CHF,         , Cash in Bank CHF
    Assets/Tax Recoverable,                   1300,      USD,         , VAT Recoverable (Input VAT)
    Liabilities/Payables,                     2000,      USD,         , Accounts Payable USD
    Liabilities/Payables,                     2010,      EUR,         , Accounts Payable EUR
    Liabilities/Tax Payable,                  2200,      USD,         , VAT Payable (Output VAT)
    Equity,                                   3000,      USD,         , Owner's Equity
    Revenue/Sales,                            4000,      USD,  OUT_STD, Sales Revenue - USD
    Revenue/Sales,                            4001,      EUR,  OUT_STD, Sales Revenue - EUR
    Expenses/Cost of Goods Sold,              5000,      USD,   IN_STD, Purchases
    Expenses/Other/Financial,                 7050,      USD,         , Foreign Exchange Gain/Loss
    Revenue/Other/Financial,                  8050,      USD,         , Foreign Exchange Gain

"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

# flake8: noqa: E501
JOURNAL_CSV = """
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
    19, 2024-12-01,    1000,   3000,      USD, 10000000.00,              ,         ,       General, Capital Increase,
    20, 2024-12-02,    1010,   2010,      EUR, 90000000.00,   91111111.10,         ,          Shop, Value 90 Mio USD @1.0123456789 (10 decimal places),
    21, 2024-12-03,    2010,   1010,      EUR, 90000000.00,   91111111.10,         ,          Shop, Revert previous entry,
    22, 2024-12-04,        ,   1000,      USD,  9500000.00,              ,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
    22, 2024-12-04,    1010,       ,      EUR,  9978888.88,    9500000.00,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
    23, 2024-12-05,        ,   1000,      USD,   200000.00,              ,         ,       General, Convert USD to EUR and CHF,
    23,           ,    1010,       ,      EUR,    97750.00,     100000.00,         ,       General, Convert USD to EUR and CHF,
    23,           ,    1025,       ,      CHF, 14285714.29,     100000.00,         ,       General, Convert USD to EUR and CHF,
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)
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
    2024-12-31, 1000:2999,      ,   7050, FX revaluations
"""
REVALUATIONS = pd.read_csv(StringIO(REVALUATIONS_CSV), skipinitialspace=True)

PROFIT_CENTERS_CSV = """
    profit_center
             Shop
          General
           Bakery
       Restaurant
             Cafe
"""
PROFIT_CENTERS = pd.read_csv(StringIO(PROFIT_CENTERS_CSV), skipinitialspace=True)

# flake8: noqa: E501
EXPECTED_BALANCE_CSV = """
    period,       account,            profit_center, balance
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
    2024-12-31,      4001,                         , "{reporting_currency:   -1402.23, EUR:    -1309.52}"
    2024-Q4,    1000:1999,                         , "{reporting_currency: 11655605.63,USD: 300000.0, EUR: 10076638.88, JPY: 0.0, CHF: 14285714.3}"
    2024,       1000:1999,                         , "{reporting_currency: 12756779.54,USD: 1076572.64, EUR: 10026667.1, JPY: 54345678.0, CHF: 14285714.3}"
    2024-08,    1000:1999,                         , "{reporting_currency: -700.0, USD: -700.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2023-12-31, 1000:9999,                "General", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2023-12-31, 1000:9999,    "General, Shop, Cafe", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                "General", "{reporting_currency: 0.0, USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,    "General, Shop, Cafe", "{reporting_currency: 0.0, USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                   "Cafe", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-23,      1000,                "General", "{reporting_currency: 800000.0, USD: 800000.0}"
    2024-01-23,      1000,  "General, Shop, Bakery", "{reporting_currency: 800000.0, USD: 800000.0}"
    2024-01-24,      1000,  "General, Shop, Bakery", "{reporting_currency: 801200.0, USD: 801200.0}"
    2024-01-24,      2200,  "General, Shop, Bakery", "{reporting_currency:  -200.00, USD: -200.00}"
    2024-03-31, 1000:1999,  "General, Shop, Bakery", "{reporting_currency: 1099532.82, USD: 801200.0, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31, 1000:1999,             "Restaurant", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 1000:9999,             "Restaurant", "{reporting_currency: 0.0, USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 1000:9999,    "General, Restaurant", "{reporting_currency: 0.0, USD: -498332.82, EUR: 27078.22, JPY: 54345678.0, CHF: 14285714.3}"
"""
EXPECTED_BALANCE = pd.read_csv(StringIO(EXPECTED_BALANCE_CSV), skipinitialspace=True)
EXPECTED_BALANCE["balance"] = (EXPECTED_BALANCE["balance"]
                               .str.replace(r'(\w+):', r'"\1":', regex=True)
                               .apply(json.loads))

EXPECTED_BALANCES_CSV = """
        period,  accounts,            profit_center,  group,                          description,                 account, currency,     balance, report_balance
    2023-12-31, 1000:1015,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
    2024-01-01, 1000:1999,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
              ,          ,                         , Assets/Tax Recoverable,          VAT Recoverable (Input VAT),    1300,      USD,        0.00, 0.00
    2024-01-01,      1000,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
       2024-Q4, 1000:1999,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   300000.00, 300000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR, 10076638.88, 11255605.63
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY,        0.00, 0.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF, 14285714.29, 100000.00
              ,          ,                         , Assets/Tax Recoverable,          VAT Recoverable (Input VAT),    1300,      USD,        0.00, 0.00
          2024, 1000:1999,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,  1076311.79, 1076311.79
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,     -100.00, -100.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR, 10026687.10, 11199809.49
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,     -20.00, -22.34
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 54345678.00, 380419.75
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF, 14285714.29, 100000.00
              ,          ,                         , Assets/Tax Recoverable,          VAT Recoverable (Input VAT),    1300,      USD,     360.85, 360.85
       2024-12, 1000:1025,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   300000.00, 300000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR, 10076638.88, 11255605.63
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY,        0.00, 0.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF, 14285714.29, 100000.00
    2023-12-31, 1000:1015,                "General", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
    2024-01-01, 1000:1999,                "General", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
              ,          ,                         , Assets/Tax Recoverable,          VAT Recoverable (Input VAT),    1300,      USD,        0.00, 0.00
    2024-01-24,      1000,  "General, Shop, Bakery", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   801200.00, 801200.00
    2024-03-31, 1000:1999,  "General, Shop, Bakery", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   801200.00, 801200.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
              ,          ,                         , Assets/Tax Recoverable,          VAT Recoverable (Input VAT),    1300,      USD,        0.00, 0.00
"""
EXPECTED_BALANCES = pd.read_csv(StringIO(EXPECTED_BALANCES_CSV), skipinitialspace=True)

EXPECTED_HISTORY = [{
        "period": "2024-12-31", "account": "1000", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,      amount,     balance, tax_code, description, document
            2024-01-01,       ,      USD,   800000.00,   800000.00,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,   4000,      USD,     1200.00,   801200.00,  OUT_STD, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-04-12,       ,      USD,   -21288.24,   779911.76,         , Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-05-05,   5000,      USD,     -555.55,   779356.21,   IN_STD, Purchase with tax, 2024/payables/2024-05-05.pdf
            2024-05-06,   5000,      USD,     -666.66,   778689.55,   IN_RED, Purchase at reduced tax, 2024/payables/2024-05-06.pdf
            2024-05-07,   5000,      USD,     -777.77,   777911.78,   EXEMPT, Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
            2024-05-08,       ,      USD,     -999.99,   776911.79,         , Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
            2024-05-24,   1005,      USD,      100.00,   777011.79,         , Collective transaction - leg with debit and credit account,
            2024-05-24,       ,      USD,        0.00,   777011.79,         , Collective transaction with zero amount,
            2024-05-24,       ,      USD,     -100.00,   776911.79,         , Collective transaction with zero amount,
            2024-05-24,       ,      USD,      100.00,   777011.79,         , Collective transaction with zero amount,
            2024-08-06,   2000,      USD,      500.00,   777511.79,         , Payment from customer, 2024/banking/USD_2024-Q2.pdf
            2024-08-07,   2000,      USD,     -200.00,   777311.79,         , Payment to supplier,
            2024-08-08,   2000,      USD,    -1000.00,   776311.79,         , Correction of previous entry,
            2024-12-01,   3000,      USD, 10000000.00, 10776311.79,         , Capital Increase,
            2024-12-04,       ,      USD, -9500000.00,  1276311.79,         , Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
            2024-12-05,       ,      USD,  -200000.00,  1076311.79,         , Convert USD to EUR and CHF,
            """
    }, {
        "period": "2024-12-31", "account": "2200", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,  amount, balance, tax_code, description, document
            2024-01-24,   4000,      USD, -200.00, -200.00,  OUT_STD, TAX: Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-07-01,   4001,      EUR, -166.67, -366.67,  OUT_STD, TAX: Sale at mixed VAT rate, /invoices/invoice_002.pdf
            2024-07-01,   4001,      EUR,  -23.81, -390.48,  OUT_RED, TAX: Sale at mixed VAT rate, /invoices/invoice_002.pdf
            """
    }, {
        "period": "2024-03-31", "account": "1000:1999", "profit_centers": None, "drop": True, "account_history":
            """
                  date, account, contra, currency,      amount,     balance, report_amount, report_balance, tax_code, description, document
            2024-01-01,    1000,       ,      USD,   800000.00,   800000.00,     800000.00,      800000.00,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1010,       ,      EUR,      120.00,   800120.00,        132.82,      800132.82,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1020,       ,      JPY, 42000000.00, 42800120.00,     298200.00,     1098332.82,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,    1000,   4000,      USD,     1200.00, 42801320.00,       1200.00,     1099532.82,  OUT_STD, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-03-31,    1010,       ,      EUR,        0.00, 42801320.00,         -3.29,     1099529.53,         , FX revaluations,
            2024-03-31,    1020,       ,      JPY,        0.00, 42801320.00,     -21000.00,     1078529.53,         , FX revaluations,
            """
    }, {
        "period": "2024", "account": "1010", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,       amount,     balance, report_amount, report_balance, tax_code, description, document
            2024-01-01,       ,      EUR,       120.00,      120.00,        132.82,         132.82,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-03-31,       ,      EUR,         0.00,      120.00,         -3.29,         129.53,         , FX revaluations,
            2024-04-12,       ,      EUR,     20000.00,    20120.00,      21288.24,       21417.77,         , Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-05-24,       ,      EUR,        20.00,    20140.00,         20.50,       21438.27,         , Collective transaction - leg with credit account,
            2024-05-25,   5000,      EUR,      -800.00,    19340.00,       -863.52,       20574.75,   IN_STD, Purchase goods, 2024/payables/2024-05-25.pdf
            2024-06-30,       ,      EUR,         0.00,    19340.00,        134.52,       20709.27,         , FX revaluations,
            2024-07-01,       ,      EUR,      1500.00,    20840.00,       1606.20,       22315.47,         , Sale at mixed VAT rate, /invoices/invoice_002.pdf
            2024-07-04,       ,      EUR,    -70791.78,   -49951.78,     -76386.36,      -54070.89,         , Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
            2024-08-08,   2010,      EUR,         0.00,   -49951.78,          0.00,      -54070.89,         , Zero amount transaction,
            2024-09-09,   7050,      EUR,         0.00,   -49951.78,         -5.55,      -54076.44,         , Manual Foreign currency adjustment,
            2024-09-30,       ,      EUR,         0.00,   -49951.78,      -1719.70,      -55796.14,         , FX revaluations,
            2024-12-02,   2010,      EUR,  90000000.00, 89950048.22,   91111111.10,    91055314.96,         , Value 90 Mio USD @1.0123456789 (10 decimal places),
            2024-12-03,   2010,      EUR, -90000000.00,   -49951.78,  -91111111.10,      -55796.14,         , Revert previous entry,
            2024-12-04,       ,      EUR,   9978888.88,  9928937.10,    9500000.00,     9444203.86,         , Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
            2024-12-05,       ,      EUR,     97750.00, 10026687.10,     100000.00,     9544203.86,         , Convert USD to EUR and CHF,
            2024-12-31,       ,      EUR,         0.00, 10026687.10,    1655605.63,    11199809.49,         , FX revaluations,
            """
    }, {
        "period": "2024-Q4", "account": "1000", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,      amount, balance, description
            2024-12-01,   3000,      USD, 10000000.00, 10776311.79, Capital Increase
            2024-12-04,       ,      USD, -9500000.00, 1276311.79, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places)
            2024-12-05,       ,      USD,  -200000.00, 1076311.79, Convert USD to EUR and CHF
            """
    }, {
        "period": "2024-08", "account": "1000", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,   amount,   balance, description, document
            2024-08-06,   2000,      USD,   500.00, 777511.79, Payment from customer, 2024/banking/USD_2024-Q2.pdf
            2024-08-07,   2000,      USD,  -200.00, 777311.79, Payment to supplier,
            2024-08-08,   2000,      USD, -1000.00, 776311.79, Correction of previous entry,
            """
    }, {
        "period": "2024-12-31", "account": "1000", "profit_centers": "General", "drop": True, "account_history":
            """
                  date, contra, currency,      amount,     balance, profit_center, description, document
            2024-01-01,       ,      USD,   800000.00,   800000.00,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-08-06,   2000,      USD,      500.00,   800500.00,       General, Payment from customer, 2024/banking/USD_2024-Q2.pdf
            2024-08-07,   2000,      USD,     -200.00,   800300.00,       General, Payment to supplier,
            2024-08-08,   2000,      USD,    -1000.00,   799300.00,       General, Correction of previous entry,
            2024-12-01,   3000,      USD, 10000000.00, 10799300.00,       General, Capital Increase,
            2024-12-05,       ,      USD,  -200000.00, 10599300.00,       General, Convert USD to EUR and CHF,
            """
    }, {
        "period": "2024-12-31", "account": "1000:1020", "profit_centers": "General, Shop, Bakery", "drop": True, "account_history":
            """
                  date, account, contra, currency,       amount,      balance, report_amount, report_balance, tax_code, profit_center, description, document
            2024-01-01,    1000,       ,      USD,    800000.00,    800000.00,     800000.00,      800000.00,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1010,       ,      EUR,       120.00,    800120.00,        132.82,      800132.82,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1020,       ,      JPY,  42000000.00,  42800120.00,     298200.00,     1098332.82,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,    1000,   4000,      USD,      1200.00,  42801320.00,       1200.00,     1099532.82,  OUT_STD,        Bakery, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-04-12,    1010,       ,      EUR,     20000.00,  42821320.00,      21288.24,     1120821.06,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-04-12,    1000,       ,      USD,    -21288.24,  42800031.76,     -21288.24,     1099532.82,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-05-07,    1000,   5000,      USD,      -777.77,  42799253.99,       -777.77,     1098755.05,   EXEMPT,        Bakery, Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
            2024-05-08,    1000,       ,      USD,      -999.99,  42798254.00,       -999.99,     1097755.06,         ,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
            2024-07-01,    1010,       ,      EUR,      1500.00,  42799754.00,       1606.20,     1099361.26,         ,        Bakery, Sale at mixed VAT rate, /invoices/invoice_002.pdf
            2024-07-04,    1020,       ,      JPY,  12345678.00,  55145432.00,      76386.36,     1175747.62,         ,       General, Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
            2024-07-04,    1010,       ,      EUR,    -70791.78,  55074640.22,     -76386.36,     1099361.26,         ,       General, Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
            2024-08-06,    1000,   2000,      USD,       500.00,  55075140.22,        500.00,     1099861.26,         ,       General, Payment from customer, 2024/banking/USD_2024-Q2.pdf
            2024-08-07,    1000,   2000,      USD,      -200.00,  55074940.22,       -200.00,     1099661.26,         ,       General, Payment to supplier,
            2024-08-08,    1000,   2000,      USD,     -1000.00,  55073940.22,      -1000.00,     1098661.26,         ,       General, Correction of previous entry,
            2024-12-01,    1000,   3000,      USD,  10000000.00,  65073940.22,   10000000.00,    11098661.26,         ,       General, Capital Increase,
            2024-12-02,    1010,   2010,      EUR,  90000000.00, 155073940.22,   91111111.10,   102209772.36,         ,          Shop, Value 90 Mio USD @1.0123456789 (10 decimal places),
            2024-12-03,    1010,   2010,      EUR, -90000000.00,  65073940.22,  -91111111.10,    11098661.26,         ,          Shop, Revert previous entry,
            2024-12-04,    1010,       ,      EUR,   9978888.88,  75052829.10,    9500000.00,    20598661.26,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
            2024-12-04,    1000,       ,      USD,  -9500000.00,  65552829.10,   -9500000.00,    11098661.26,         ,          Shop, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places),
            2024-12-05,    1010,       ,      EUR,     97750.00,  65650579.10,     100000.00,    11198661.26,         ,       General, Convert USD to EUR and CHF,
            2024-12-05,    1000,       ,      USD,   -200000.00,  65450579.10,    -200000.00,    10998661.26,         ,       General, Convert USD to EUR and CHF,
            """
    }
]

EXPECTED_AGGREGATED_BALANCES_CSV = """
    group,                          description,                   report_balance
    /Assets/Cash,                   Bank of America,               1076311.79
    /Assets/Cash,                   Other Bank,                    -122.34
    /Assets/Cash,                   Deutsche Bank,                 11199809.49
    /Assets/Cash,                   Mitsubishi UFJ,                380419.75
    /Assets/Cash,                   UBS,                           100000.00
    /Assets/Tax Recoverable,        VAT Recoverable (Input VAT),   360.85
    /Liabilities/Payables,          Accounts Payable USD,          700.00
    /Liabilities/Payables,          Accounts Payable EUR,          0.00
    /Liabilities/Tax Payable,       VAT Payable (Output VAT),      -403.97
    /Equity,                        Owner's Equity,                -11098332.82
    /Revenue/Sales,                 Sales Revenue - USD,           -1000.00
    /Revenue/Sales,                 Sales Revenue - EUR,           -1402.23
    /Expenses/Cost of Goods Sold,   Purchases,                     3502.64
    /Expenses/Other,                Financial,                     -1659837.61
    /Revenue/Other,                 Financial,                     -5.55
"""
EXPECTED_AGGREGATED_BALANCES = pd.read_csv(StringIO(EXPECTED_AGGREGATED_BALANCES_CSV), skipinitialspace=True)

def parse_profit_center(value):
    """Function to split values by commas and convert to list"""
    if pd.isna(value) or value.strip() == "":
        return None
    return [item.strip() for item in value.split(",")]
EXPECTED_BALANCE["profit_center"] = EXPECTED_BALANCE["profit_center"].apply(parse_profit_center)
# flake8: enable

class BaseTest(ABC):
    engine = MemoryLedger()
    CONFIGURATION = {"REPORTING_CURRENCY": "USD"}
    ASSETS = engine.assets.standardize(ASSETS)
    ACCOUNTS = engine.accounts.standardize(ACCOUNTS)
    PRICES = engine.price_history.standardize(PRICES)
    JOURNAL = engine.journal.standardize(JOURNAL)
    TAX_CODES = engine.tax_codes.standardize(TAX_CODES)
    REVALUATIONS = engine.revaluations.standardize(REVALUATIONS)
    PROFIT_CENTERS = engine.profit_centers.standardize(PROFIT_CENTERS)
    EXPECTED_BALANCE = EXPECTED_BALANCE
    EXPECTED_BALANCES = EXPECTED_BALANCES
    EXPECTED_AGGREGATED_BALANCES = EXPECTED_AGGREGATED_BALANCES
    EXPECTED_HISTORY = EXPECTED_HISTORY
