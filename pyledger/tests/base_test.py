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
    group,                                                           account, currency, tax_code, description
    Assets/Cash/Bank of America,                                        1000,      USD,         , Cash in Bank USD
    Assets/Cash/Other Bank,                                             1005,      USD,         , Cash in other Bank USD
    Assets/Cash/Deutsche Bank,                                          1010,      EUR,         , Cash in Bank EUR
    Assets/Cash/Other Bank,                                             1015,      EUR,         , Cash in other Bank EUR
    Assets/Cash/Mitsubishi UFJ,                                         1020,      JPY,         , Cash in Bank JPY
    Assets/Cash/UBS,                                                    1025,      CHF,         , Cash in Bank CHF
    Assets/Current Assets/Current receivables,                          1170,      USD,         , Input VAT on materials goods services energy
    Assets/Current Assets/Current receivables,                          1171,      USD,         , Input VAT on investments other operating expenses
    Assets/Current Assets/Current receivables,                          1175,      USD,         , Accounts Receivable VAT Cleared
    Assets/Tax Recoverable,                                             1300,      USD,         , VAT Recoverable (Input VAT)
    Liabilities/Payables,                                               2000,      USD,         , Accounts Payable USD
    Liabilities & Equity/Current Liabilities/Accrued Liabilities,       2201,      USD,         , Accounts Payable VAT Cleared
    Liabilities/Payables,                                               2010,      EUR,         , Accounts Payable EUR
    Liabilities & Equity/Current Liabilities/Accrued Liabilities,       2200,      USD,         , VAT payable (output tax)
    Liabilities & Equity/Shareholder's Equity/Loss brought forward,     2970,      USD,         , Profit/Loss Carried Forward
    Liabilities & Equity/Shareholder's Equity/Profit for the year,      2979,      USD,         , Profit/Loss for the Year
    Equity,                                                             3000,      USD,         , Owner's Equity
    Revenue/Sales,                                                      4000,      USD,  OUT_STD, Sales Revenue - USD
    Revenue/Sales,                                                      4001,      EUR,  OUT_STD, Sales Revenue - EUR
    Expenses/Cost of Goods Sold,                                        5000,      USD,   IN_STD, Purchases
    Expenses/Other/Financial,                                           7050,      USD,         , Foreign Exchange Gain/Loss
    Revenue/Other/Financial,                                            8050,      USD,         , Foreign Exchange Gain
    Revenue/Balance,                                                    9200,      USD,         , Net Profit/Loss for the Year

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
    24, 2024-07-01,    4001,       ,      EUR,      166.67,        178.47,         ,          Shop, Shop sale,
    24,           ,    4001,       ,      EUR,       23.81,         25.50,         ,          Shop, Shop sale,
    24,           ,    2200,       ,      EUR,     -166.67,       -178.47,         ,          Shop, Shop sale,
    24,           ,    2200,       ,      EUR,      -23.81,        -25.50,         ,          Shop, Shop sale,
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
    date,         account, debit, credit, description,     split_per_profit_center
    2024-03-31, 1000:2999,  7050,   8050, FX revaluations, False
    2024-06-30, 1000:2999,  7050,       , FX revaluations, False
    2024-09-30, 1000:2999,  7050,       , FX revaluations, True
    2024-12-31, 1000:2999,      ,   7050, FX revaluations, True
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

RECONCILIATION_CSV = """
    period,         account, currency,          profit_center,       balance,  report_balance, tolerance, document,                           source
    2023-12-31,   1000:2999,      CHF,                       ,          0.00,            0.00,          , 2023/reconciliation/2023-12-31.pdf, 2023/financial/all.pdf
    2024-01-23,   1000:2999,      EUR,                       ,        120.00,      1098332.82,      0.01, 2024/reconciliation/2024-01-23.pdf, 2024/start/all.pdf
    2024-09-25,        1000,      USD,                       ,     776311.79,       776311.79,         1,                                   , 2024/financial/data.pdf
    2024-Q4,      1000:2999,      EUR,                       ,   10076638.88,     11655605.63,      0.01,    2024/reconciliation/2024-Q4.pdf, 2024/financial/data.pdf
    2024-08,      1000:2999,      CHF,               "Bakery",          0.00,            0.00,      0.01,                                   , 2024/financial/custom/data.pdf
    2024,         1000:9999,      EUR,              "General",      27078.22,             0.0,      0.01,       2024/reconciliation/2024.pdf, 2024/financial/all.pdf
    2024,         1000:9999,      USD,              "General",    -498332.82,             0.0,      0.01,       2024/reconciliation/2024.pdf, 2024/financial/all.pdf
"""
RECONCILIATION = pd.read_csv(StringIO(RECONCILIATION_CSV), skipinitialspace=True)

TARGET_BALANCE_CSV = """
    id, date,       account, contra,            currency, profit_center,                                  description, lookup_period, lookup_accounts,     balance
    1,  2024-12-31,    2979,   9200,  reporting_currency,       General,                        P&L for the year 2024,          2024,       3000:9999,           0
    2,  2025-01-02,    2970,   2979,  reporting_currency,       General, Move P&L for the year to P&L Carried Forward,          2024,            2979,           0
    3,  2024-12-31,    1170,   1175,                 USD,       General,                    VAT return 2024 input tax,          2024,            1170,           0
    4,  2024-12-31,    1171,   1175,                 USD,       General,                    VAT return 2024 input tax,          2024,            1171,           0
    5,  2024-12-31,    2200,   1175,                 USD,       General,                    VAT return 2024 sales tax,          2024,            2200,           0
"""
TARGET_BALANCE = pd.read_csv(StringIO(TARGET_BALANCE_CSV), skipinitialspace=True)

# flake8: noqa: E501
EXPECTED_BALANCES_CSV = """
    period,       account,            profit_center, report_balance,   balance
    2023-12-31, 1000:9999,                         ,            0.0,   "{USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                         ,            0.0,   "{USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:1999,                         ,     1098332.82,   "{USD:   800000.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01,      1000,                         ,      800000.00,   "{USD:   800000.00}"
    2024-01-01,      1010,                         ,         132.82,   "{EUR:      120.00}"
    2024-01-01,      1020,                         ,      298200.00,   "{JPY: 42000000.00}"
    2024-01-23,      1000,                         ,      800000.00,   "{USD:   800000.00}"
    2024-01-23,      2200,                         ,           0.00,   "{USD:        0.00}"
    2024-01-24,      1000,                         ,      801200.00,   "{USD:   801200.00}"
    2024-01-24,      2200,                         ,        -200.00,   "{USD:     -200.00}"
    2024-03-30, 1000:1999,                         ,     1099532.82,   "{USD:   801200.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31, 1000:1999,                         ,     1078529.53,   "{USD:   801200.00, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31,      7050,                         ,       21003.29,   "{USD:    21003.29}"
    2024-03-31,      8050,                         ,           0.00,   "{USD:        0.00}"
    2024-12-31,      4001,                         ,       -1198.26,   "{EUR:    -1119.04}"
    2024-Q4,    1000:1999,                         ,    11655805.63,   "{USD: 300200.00, EUR: 10076638.88, JPY: 0.0, CHF: 14285714.3}"
    2024,       1000:1999,                         ,     12719310.1,   "{USD: 1076628.72, EUR: 10026800.43, JPY: 54345678.0, CHF: 14285714.3}"
    2024-08,    1000:1999,                         ,         -700.0,   "{USD: -700.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 3000:9999,                         ,           0.00,   "{USD: 473.11, EUR: -452.37}"
    2024-12-31,      2970,                         ,           0.00,   "{USD:    0.00}"
    2024-12-31,      9200,                         ,    12719202.16,   "{USD: 12719202.16}"
    2024-12-31,      2979,                         ,   -12719202.16,   "{USD: -12719202.16}"
    2025-01-02,      2970,                         ,   -12719202.16,   "{USD: -12719202.16}"
    2025-01-02,      2979,                         ,           0.00,   "{USD: 0.00}"
    2025-01-02,      9200,                         ,    12719202.16,   "{USD: 12719202.16}"
    2024-12-31,      1170,                         ,           0.00,   "{USD: 0.00}"
    2024-12-31,      1171,                         ,           0.00,   "{USD: 0.00}"
    2024-12-31,      1175,                         ,         200.00,   "{USD: 200.00}"
    2024-12-31,      2200,                         ,        -807.94,   "{USD: -400.0, EUR: -380.96}"
    2023-12-31, 1000:9999,                "General",            0.0,   "{USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2023-12-31, 1000:9999,    "General, Shop, Cafe",            0.0,   "{USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                "General",            0.0,   "{USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,    "General, Shop, Cafe",            0.0,   "{USD: -298332.82, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-01-01, 1000:9999,                   "Cafe",            0.0,   "{USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-01-23,      1000,                "General",       800000.0,   "{USD: 800000.0}"
    2024-01-23,      1000,  "General, Shop, Bakery",       800000.0,   "{USD: 800000.0}"
    2024-01-24,      1000,  "General, Shop, Bakery",       801200.0,   "{USD: 801200.0}"
    2024-01-24,      2200,  "General, Shop, Bakery",        -200.00,   "{USD: -200.00}"
    2024-03-31, 1000:1999,  "General, Shop, Bakery",     1099532.82,   "{USD: 801200.0, EUR: 120.0, JPY: 42000000.0, CHF: 0.0}"
    2024-03-31, 1000:1999,             "Restaurant",            0.0,   "{USD: 0.0, EUR: 0.0, JPY: 0.0, CHF: 0.0}"
    2024-12-31, 1000:9999,             "Restaurant",            0.0,   "{USD: -5.55}"
    2024-12-31, 1000:2999,                "General",    11110666.37,   "{USD: 10600000.0, EUR: 27078.22, JPY: 54345678.0, CHF: 14285714.3}"
    2024-12-31, 1000:2999,                   "Shop",     1647266.67,   "{USD: -9521288.24, EUR: 9998698.4}"
    2024-12-31, 1000:2999,                 "Bakery",         786.36,   "{USD: -685.17, EUR: 1309.52}"
    2024-12-31, 1000:9999,    "General, Restaurant",            0.0,   "{USD: -510671.92, EUR: 27078.22, JPY: 54345678.0, CHF: 14285714.3}"
"""
EXPECTED_BALANCES = pd.read_csv(StringIO(EXPECTED_BALANCES_CSV), skipinitialspace=True)

EXPECTED_INDIVIDUAL_BALANCES_CSV = """
        period,  accounts,            profit_center,  group,                          description,                 account, currency,     balance, report_balance
    2023-12-31, 1000:1015,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
    2024-01-01, 1000:1050,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
    2024-01-01,      1000,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
       2024-Q4, 1000:1050,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   300000.00, 300000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR, 10076638.88, 11255605.63
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY,        0.00, 0.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF, 14285714.29, 100000.00
          2024, 1000:1050,                         , Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,  1076311.79, 1076311.79
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,     -100.00, -100.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR, 10026687.10, 11199940.72
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,     -20.00, -23.26
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 54345678.00, 342620.0
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF, 14285714.29, 100000.00
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
    2024-01-01, 1000:1050,                "General", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   800000.00, 800000.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
    2024-01-24,      1000,  "General, Shop, Bakery", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   801200.00, 801200.00
    2024-03-31, 1000:1050,  "General, Shop, Bakery", Assets/Cash/Bank of America,     Cash in Bank USD,               1000,      USD,   801200.00, 801200.00
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank USD,         1005,      USD,        0.00, 0.00
              ,          ,                         , Assets/Cash/Deutsche Bank,       Cash in Bank EUR,               1010,      EUR,     120.00, 132.82
              ,          ,                         , Assets/Cash/Other Bank,          Cash in other Bank EUR,         1015,      EUR,        0.00, 0.00
              ,          ,                         , Assets/Cash/Mitsubishi UFJ,      Cash in Bank JPY,               1020,      JPY, 42000000.00, 298200.00
              ,          ,                         , Assets/Cash/UBS,                 Cash in Bank CHF,               1025,      CHF,        0.00, 0.00
"""
EXPECTED_INDIVIDUAL_BALANCES = pd.read_csv(StringIO(EXPECTED_INDIVIDUAL_BALANCES_CSV), skipinitialspace=True)

EXPECTED_HISTORY = [{
        "period": "2024-04-13", "account": "1000", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,      amount,     balance, tax_code, description, document
            2024-01-01,       ,      USD,   800000.00,   800000.00,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,   4000,      USD,     1200.00,   801200.00,  OUT_STD, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-04-12,       ,      USD,   -21288.24,   779911.76,         , Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf"""
    }, {
        "period": "2024-12-31", "account": "2200", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,  amount, balance, tax_code, description, document
            2024-01-24,   4000,      USD, -200.00, -200.00,  OUT_STD, TAX: Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-07-01,       ,      EUR, -166.67, -366.67,         , Shop sale,
            2024-07-01,       ,      EUR,  -23.81, -390.48,         , Shop sale,
            2024-07-01,   4001,      EUR,  -23.81, -414.29,  OUT_RED, TAX: Sale at mixed VAT rate, /invoices/invoice_002.pdf
            2024-07-01,   4001,      EUR, -166.67, -580.96,  OUT_STD, TAX: Sale at mixed VAT rate, /invoices/invoice_002.pdf"""
    }, {
        "period": "2024-03-31", "account": "1000:1999", "profit_centers": None, "drop": True, "account_history":
            """
                  date, account, contra, currency,      amount,     balance, report_amount, report_balance, tax_code, description, document
            2024-01-01,    1000,       ,      USD,   800000.00,   800000.00,     800000.00,      800000.00,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1010,       ,      EUR,      120.00,   800120.00,        132.82,      800132.82,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1020,       ,      JPY, 42000000.00, 42800120.00,     298200.00,     1098332.82,         , Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,    1000,   4000,      USD,     1200.00, 42801320.00,       1200.00,     1099532.82,  OUT_STD, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-03-31,    1020,       ,      JPY,        0.00, 42801320.00,     -21000.00,     1078532.82,         , FX revaluations,
            2024-03-31,    1010,       ,      EUR,        0.00, 42801320.00,         -3.29,     1078529.53,         , FX revaluations,
            """
    }, {
        "period": "2024", "account": "1020", "profit_centers": None, "drop": True, "account_history":
            """
                  date, currency,       amount,     balance, report_amount, report_balance, description, document
            2024-01-01,      JPY,  42000000.00, 42000000.00,     298200.00,      298200.00, Opening balance, 2023/financials/balance_sheet.pdf
            2024-03-31,      JPY,         0.00, 42000000.00,     -21000.00,      277200.00, FX revaluations,
            2024-06-30,      JPY,         0.00, 42000000.00,     -16800.00,      260400.00, FX revaluations,
            2024-07-04,      JPY,  12345678.00, 54345678.00,      76386.36,      336786.36, Convert JPY to EUR, 2024/transfers/2024-07-05_JPY-EUR.pdf
            2024-09-10,      JPY,         0.00, 54345678.00,          5.55,      336791.91, Manual Foreign currency adjustment,"""
    }, {
        "period": "2024-Q4", "account": "1000", "profit_centers": None, "drop": True, "account_history":
            """
                  date, contra, currency,      amount, balance, description
            2024-12-01,   3000,      USD, 10000000.00, 10776311.79, Capital Increase
            2024-12-04,       ,      USD, -9500000.00, 1276311.79, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places)
            2024-12-05,       ,      USD,  -200000.00, 1076311.79, Convert USD to EUR and CHF"""
    }, {
        "period": "2024-05", "account": "1000:1020", "profit_centers": None, "drop": False, "account_history":
            """
                  date, account, contra, currency,  amount,     balance, report_amount, report_balance, tax_code, profit_center, description, document
            2024-05-05,    1000,   5000,      USD, -555.55, 42799476.21,       -555.55,     1077973.98,   IN_STD,              , Purchase with tax, 2024/payables/2024-05-05.pdf
            2024-05-06,    1000,   5000,      USD, -666.66, 42798809.55,       -666.66,     1077307.32,   IN_RED,              , Purchase at reduced tax, 2024/payables/2024-05-06.pdf
            2024-05-07,    1000,   5000,      USD, -777.77, 42798031.78,       -777.77,     1076529.55,   EXEMPT,              , Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
            2024-05-08,    1000,       ,      USD, -999.99, 42797031.79,       -999.99,     1075529.56,         ,              , Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
            2024-05-24,    1015,       ,      EUR,   -20.0, 42797131.79,         -20.5,     1075509.06,         ,              , Collective transaction - leg with debit account,
            2024-05-24,    1005,   1000,      USD,  -100.0, 42797031.79,        -100.0,     1075409.06,         ,              , Collective transaction - leg with debit and credit account,
            2024-05-24,    1010,       ,      EUR,    20.0, 42797051.79,          20.5,     1075429.56,         ,              , Collective transaction - leg with credit account,
            2024-05-24,    1000,   1005,      USD,   100.0, 42797151.79,         100.0,     1075529.56,         ,              , Collective transaction - leg with debit and credit account,
            2024-05-24,    1000,       ,      USD,     0.0, 42797151.79,           0.0,     1075529.56,         ,              , Collective transaction with zero amount,
            2024-05-24,    1000,       ,      USD,  -100.0, 42797051.79,        -100.0,     1075429.56,         ,              , Collective transaction with zero amount,
            2024-05-24,    1000,       ,      USD,   100.0, 42797151.79,         100.0,     1075529.56,         ,              , Collective transaction with zero amount,
            2024-05-25,    1010,   5000,      EUR,  -800.0, 42796231.79,       -863.52,     1074666.04,   IN_STD,              , Purchase goods, 2024/payables/2024-05-25.pdf
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
            2024-12-05,       ,      USD,  -200000.00, 10599300.00,       General, Convert USD to EUR and CHF,"""
    }, {
        "period": "2024-05-30", "account": "1000:1020", "profit_centers": "General, Shop, Bakery", "drop": True, "account_history":
            """
                  date, account, contra, currency,       amount,      balance, report_amount, report_balance, tax_code, profit_center, description, document
            2024-01-01,    1000,       ,      USD,    800000.00,    800000.00,     800000.00,      800000.00,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1010,       ,      EUR,       120.00,    800120.00,        132.82,      800132.82,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-01,    1020,       ,      JPY,  42000000.00,  42800120.00,     298200.00,     1098332.82,         ,       General, Opening balance, 2023/financials/balance_sheet.pdf
            2024-01-24,    1000,   4000,      USD,      1200.00,  42801320.00,       1200.00,     1099532.82,  OUT_STD,        Bakery, Sell cakes, 2024/receivables/2024-01-24.pdf
            2024-04-12,    1010,       ,      EUR,     20000.00,  42821320.00,      21288.24,     1120821.06,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-04-12,    1000,       ,      USD,    -21288.24,  42800031.76,     -21288.24,     1099532.82,         ,          Shop, Convert USD to EUR, 2024/transfers/2024-04-12_USD-EUR.pdf
            2024-05-07,    1000,   5000,      USD,      -777.77,  42799253.99,       -777.77,     1098755.05,   EXEMPT,        Bakery, Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
            2024-05-08,    1000,       ,      USD,      -999.99,  42798254.00,       -999.99,     1097755.06,         ,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf"""
    }, {
        "period": "2024-05", "account": "1000:1020", "profit_centers": "Cafe, Bakery", "drop": False, "account_history":
            """
                date, account, contra, currency,  amount,     balance, report_amount, report_balance, tax_code, profit_center, description, document
            2024-05-05,    1000,   5000,      USD, -555.55,    644.45,       -555.55,         644.45,   IN_STD,          Cafe, Purchase with tax, 2024/payables/2024-05-05.pdf
            2024-05-06,    1000,   5000,      USD, -666.66,    -22.21,       -666.66,         -22.21,   IN_RED,          Cafe, Purchase at reduced tax, 2024/payables/2024-05-06.pdf
            2024-05-07,    1000,   5000,      USD, -777.77,   -799.98,       -777.77,        -799.98,   EXEMPT,        Bakery, Tax-Exempt purchase, 2024/payables/2024-05-07.pdf
            2024-05-08,    1000,       ,      USD, -999.99,  -1799.97,       -999.99,       -1799.97,         ,        Bakery, Purchase with mixed tax rates, 2024/payables/2024-05-08.pdf
            2024-05-25,    1010,   5000,      EUR, -800.00,  -2599.97,       -863.52,       -2663.49,   IN_STD,          Cafe, Purchase goods, 2024/payables/2024-05-25.pdf"""
    }
]

EXPECTED_RECONCILIATION = [{
        "period": "2023-12-31", "source_pattern": None, "reconciliation":
            """
            period,    account, currency, profit_center,  balance,  report_balance,  tolerance,                            document, actual_balance,  actual_report_balance
        2023-12-31,  1000:2999,      CHF,              ,      0.0,             0.0,     0.0005,  2023/reconciliation/2023-12-31.pdf,            0.0,                    0.0"""
    }, {
        "period": "2024-12-31", "source_pattern": None, "reconciliation":
        """
            period,    account, currency, profit_center,     balance,  report_balance,  tolerance,                            document, actual_balance, actual_report_balance
        2023-12-31,  1000:2999,      CHF,              ,         0.0,             0.0,     0.0005,  2023/reconciliation/2023-12-31.pdf,           0.00,                  0.00
              2024,  1000:9999,      EUR,       General,    27078.22,             0.0,       0.01,        2024/reconciliation/2024.pdf,       27078.22,                  0.00
              2024,  1000:9999,      USD,       General,  -498332.82,             0.0,       0.01,        2024/reconciliation/2024.pdf,     -510666.37,                  0.00
        2024-01-23,  1000:2999,      EUR,              ,      120.00,      1098332.82,       0.01,  2024/reconciliation/2024-01-23.pdf,         120.00,            1098332.82
           2024-08,  1000:2999,      CHF,        Bakery,         0.0,             0.0,       0.01,                                    ,           0.00,                  0.00
        2024-09-25,       1000,      USD,              ,   776311.79,       776311.79,        1.0,                                    ,      776311.79,             776311.79
           2024-Q4,  1000:2999,      EUR,              , 10076638.88,     11655605.63,       0.01,     2024/reconciliation/2024-Q4.pdf,    10076638.88,           11655605.63"""
    }, {
        "period": "2024", "source_pattern": None, "reconciliation":
        """
            period,    account, currency, profit_center,       balance,  report_balance,  tolerance,                            document, actual_balance,  actual_report_balance
              2024,  1000:9999,      EUR,       General,      27078.22,             0.0,       0.01,        2024/reconciliation/2024.pdf,       27078.22,                   0.00
              2024,  1000:9999,      USD,       General,    -498332.82,             0.0,       0.01,        2024/reconciliation/2024.pdf,     -510666.37,                   0.00
        2024-01-23,  1000:2999,      EUR,              ,        120.00,      1098332.82,       0.01,  2024/reconciliation/2024-01-23.pdf,         120.00,             1098332.82
           2024-08,  1000:2999,      CHF,        Bakery,           0.0,             0.0,       0.01,                                    ,           0.00,                   0.00
        2024-09-25,       1000,      USD,              ,     776311.79,       776311.79,        1.0,                                    ,      776311.79,              776311.79
           2024-Q4,  1000:2999,      EUR,              ,   10076638.88,     11655605.63,       0.01,     2024/reconciliation/2024-Q4.pdf,    10076638.88,            11655605.63"""
    }, {
        "period": "2024-Q3", "source_pattern": None, "reconciliation":
        """
            period,    account, currency, profit_center,   balance,  report_balance,  tolerance, document, actual_balance,  actual_report_balance
           2024-08,  1000:2999,      CHF,        Bakery,       0.0,             0.0,       0.01,         ,           0.00,                   0.00
        2024-09-25,       1000,      USD,              , 776311.79,       776311.79,        1.0,         ,      776311.79,              776311.79"""
    }, {
        "period": "2024-09", "source_pattern": None, "reconciliation":
        """
            period, account, currency, profit_center,   balance,  report_balance,  tolerance, document, actual_balance,  actual_report_balance
        2024-09-25,    1000,      USD,              , 776311.79,       776311.79,        1.0,         ,      776311.79,              776311.79"""
    }, {
        "period": "2023", "source_pattern": r"^2023/.*\.pdf$", "reconciliation":
        """
            period,    account, currency, profit_center,  balance,  report_balance,  tolerance,                            document,                 source, actual_balance,  actual_report_balance
        2023-12-31,  1000:2999,      CHF,              ,      0.0,             0.0,     0.0005,  2023/reconciliation/2023-12-31.pdf, 2023/financial/all.pdf,            0.0,                    0.0"""
    }, {
        "period": "2024", "source_pattern": r"^2024/financial/.*\.pdf$", "reconciliation":
        """
            period,    account, currency, profit_center,     balance,  report_balance,  tolerance,                            document,                          source, actual_balance,  actual_report_balance
              2024,  1000:9999,      EUR,       General,    27078.22,             0.0,       0.01,        2024/reconciliation/2024.pdf,          2024/financial/all.pdf,       27078.22,                   0.00
              2024,  1000:9999,      USD,       General,  -498332.82,             0.0,       0.01,        2024/reconciliation/2024.pdf,          2024/financial/all.pdf,     -510666.37,                   0.00
           2024-08,  1000:2999,      CHF,        Bakery,         0.0,             0.0,       0.01,                                    ,  2024/financial/custom/data.pdf,           0.00,                   0.00
        2024-09-25,       1000,      USD,              ,   776311.79,       776311.79,        1.0,                                    ,         2024/financial/data.pdf,      776311.79,              776311.79
           2024-Q4,  1000:2999,      EUR,              , 10076638.88,     11655605.63,       0.01,     2024/reconciliation/2024-Q4.pdf,         2024/financial/data.pdf,    10076638.88,            11655605.63"""
    }, {
        "period": "2024", "source_pattern": r"/custom/.*\.pdf$", "reconciliation":
        """
         period,    account, currency, profit_center,  balance,  report_balance,  tolerance, document,                          source, actual_balance,  actual_report_balance
        2024-08,  1000:2999,      CHF,        Bakery,      0.0,             0.0,       0.01,         ,  2024/financial/custom/data.pdf,            0.0,                    0.0"""
    }, {
        "period": "2024", "source_pattern": r".*/all\.pdf$", "reconciliation":
        """
            period,    account, currency, profit_center,    balance,  report_balance,  tolerance,                            document,                  source, actual_balance,  actual_report_balance
              2024,  1000:9999,      EUR,       General,   27078.22,             0.0,       0.01,        2024/reconciliation/2024.pdf,  2024/financial/all.pdf,       27078.22,                   0.00
              2024,  1000:9999,      USD,       General, -498332.82,             0.0,       0.01,        2024/reconciliation/2024.pdf,  2024/financial/all.pdf,     -510666.37,                   0.00
        2024-01-23,  1000:2999,      EUR,              ,     120.00,      1098332.82,       0.01,  2024/reconciliation/2024-01-23.pdf,      2024/start/all.pdf,         120.00,             1098332.82"""
    }
]

EXPECTED_AGGREGATED_BALANCES_CSV = """
    group,                                       description,                   report_balance
    /Assets/Cash,                                Bank of America,               1076311.79
    /Assets/Cash,                                Other Bank,                    -123.26
    /Assets/Cash,                                Deutsche Bank,                 11199940.72
    /Assets/Cash,                                Mitsubishi UFJ,                342620.0
    /Assets/Cash,                                UBS,                           100000.0
    /Assets/Current Assets,                      Current receivables,           0.0
    /Assets/Tax Recoverable,                     VAT Recoverable (Input VAT),   360.85
    /Liabilities/Payables,                       Accounts Payable USD,          700.0
    /Liabilities/Payables,                       Accounts Payable EUR,          0.0
    /Liabilities & Equity/Current Liabilities,   Accrued Liabilities,           -607.94
    /Liabilities & Equity/Shareholder's Equity,  Loss brought forward,          0.0
    /Liabilities & Equity/Shareholder's Equity,  Profit for the year,           0.0
    /Equity,                                     Owner's Equity,                -11098332.82
    /Revenue/Sales,                              Sales Revenue - USD,           -1000.0
    /Revenue/Sales,                              Sales Revenue - EUR,           -1198.26
    /Expenses/Cost of Goods Sold,                Purchases,                     3502.64
    /Expenses/Other,                             Financial,                     -1622168.17
    /Revenue/Other,                              Financial,                     -5.55
    /Revenue/Balance,                            Net Profit/Loss for the Year,  0.0
"""
EXPECTED_AGGREGATED_BALANCES = pd.read_csv(StringIO(EXPECTED_AGGREGATED_BALANCES_CSV), skipinitialspace=True)
# flake8: enable

class BaseTest(ABC):
    engine = MemoryLedger()

    @staticmethod
    def parse_profit_center(value):
        """Function to split values by commas and convert to list"""
        if pd.isna(value) or value.strip() == "":
            return None
        return [item.strip() for item in value.split(",")]

    @staticmethod
    def parse_balance_series(balance):
        """Convert a Series of strings like {USD: 100} into actual Python dictionaries."""
        return balance.replace(r'(\w+):', r'"\1":', regex=True).apply(json.loads)

    CONFIGURATION = {"REPORTING_CURRENCY": "USD"}
    ASSETS = engine.assets.standardize(ASSETS)
    ACCOUNTS = engine.accounts.standardize(ACCOUNTS)
    PRICES = engine.price_history.standardize(PRICES)
    JOURNAL = engine.journal.standardize(JOURNAL)
    TAX_CODES = engine.tax_codes.standardize(TAX_CODES)
    REVALUATIONS = engine.revaluations.standardize(REVALUATIONS)
    RECONCILIATION = engine.reconciliation.standardize(RECONCILIATION)
    PROFIT_CENTERS = engine.profit_centers.standardize(PROFIT_CENTERS)
    TARGET_BALANCE = engine.target_balance.standardize(TARGET_BALANCE)
    EXPECTED_BALANCES = EXPECTED_BALANCES
    EXPECTED_BALANCES["profit_center"] = EXPECTED_BALANCES["profit_center"].apply(parse_profit_center)
    EXPECTED_BALANCES["balance"] = parse_balance_series(EXPECTED_BALANCES["balance"])
    EXPECTED_INDIVIDUAL_BALANCES = EXPECTED_INDIVIDUAL_BALANCES
    EXPECTED_AGGREGATED_BALANCES = EXPECTED_AGGREGATED_BALANCES
    EXPECTED_HISTORY = EXPECTED_HISTORY
    EXPECTED_RECONCILIATION = EXPECTED_RECONCILIATION
