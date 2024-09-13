"""Constants used throughout the tests."""
import pandas as pd
from io import StringIO


VAT_CSV = """
    id,       account, rate,  inclusive, text
    OutStd,   2200,    0.038, True,      VAT at the regular 7.7% rate on goods or services
    OutRed,   2200,    0.025, True,      VAT at the reduced 2.5% rate on goods or services
    OutAcc,   2200,    0.038, True,      XXXXX
    OutStdEx, 2200,    0.077, False,     VAT at the regular 7.7% rate on goods or services
    InStd,    1170,    0.077, True,      Input Tax (Vorsteuer) at the regular 7.7% rate on
    InRed,    1170,    0.025, True,      Input Tax (Vorsteuer) at the reduced 2.5% rate on
    InAcc,    1170,    0.038, True,      YYYYY
    Test ,    9999,    0.038, True,      AAAAA
"""
VAT_CODES = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)

ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 9990,      EUR,         , Test EUR Bank Account
    /Assets, 9991,      USD,         , Test USD Bank Account
    /Assets, 9992,      CHF,         , Test CHF Bank Account
    /Assets, 9993,      EUR,         , Transitory Account EUR
    /Assets, 9994,      USD,         , Transitory Account USD
    /Assets, 9995,      CHF,         , Transitory Account CHF
    /Assets, 9999,      CHF,   Test  , Test Account with VAT
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount, vat_code, text,                             document
    1,  2024-05-24, 9992,            9995,      CHF,     100.00,                     ,  OutRed,   pytest single transaction 1,      /file1.txt
    2,  2024-05-24, 9991,                ,      USD,    -100.00,               -88.88,  OutRed,   pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 9991,                ,      USD,       1.00,                 0.89,  OutRed,   pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 9991,                ,      USD,      99.00,                87.99,  OutRed,   pytest collective txn 1 - line 1,
    3,  2024-04-24,     ,            9990,      EUR,     200.00,               175.55,  OutRed,   pytest collective txn 2 - line 1, /document-col-alt.pdf
    3,  2024-04-24, 9990,                ,      EUR,     200.00,               175.55,  OutRed,   pytest collective txn 2 - line 2, /document-col-alt.pdf
    4,  2024-05-24, 9991,            9994,      USD,     300.00,               450.45,  OutRed,   pytest single transaction 2,      /document-alt.pdf
    5,  2024-04-04, 9995,                ,      CHF, -125000.00,           -125000.00,        ,   Convert -125'000 CHF to USD @ 1.10511,
    5,  2024-04-04, 9994,                ,      USD,  138138.75,            125000.00,        ,   Convert -125'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 9995,                ,      CHF, -250000.00,                     ,        ,   Convert -250'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 9994,                ,      USD,  276277.50,            250000.00,        ,   Convert -250'000 CHF to USD @ 1.10511,
        # Transaction raised RequestException: API call failed. Total debit (125,000.00) and total credit (125,000.00) must be equal.
    7,  2024-01-16,     ,            9993,      EUR,  125000.00,            125362.50,        ,   Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
    7,  2024-01-16, 9995,                ,      CHF,  125362.50,            125362.50,        ,   Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
        # Transactions with negative amount
    8,  2024-05-24, 9990,            9993,      EUR,     -10.00,                -9.00,        ,   Individual transaction with negative amount,
        # Collective transaction with credit and debit account in single line item
    9,  2024-05-24, 9992,            9995,      CHF,     100.00,                     ,        ,   Collective transaction - leg with debit and credit account,
    9,  2024-05-24, 9990,                ,      EUR,      20.00,                19.00,        ,   Collective transaction - leg with credit account,
    9,  2024-05-24,     ,            9993,      EUR,      20.00,                19.00,        ,   Collective transaction - leg with debit account,
        # Transactions with zero base currency
    10, 2024-05-24, 9992,            9995,      CHF,       0.00,                     ,        ,   Individual transaction with zero amount,
    11, 2024-05-24, 9992,                ,      CHF,     100.00,                     ,  OutRed,   Collective transaction with zero amount,
    11, 2024-05-24, 9995,                ,      CHF,    -100.00,                     ,        ,   Collective transaction with zero amount,
    11, 2024-05-24, 9995,                ,      CHF,       0.00,                     ,        ,   Collective transaction with zero amount,
    12, 2024-03-02,     ,            9993,      EUR,  600000.00,            599580.00,        ,   Convert 600k EUR to CHF @ 0.9993,
    12, 2024-03-02, 9995,                ,      CHF,  599580.00,            599580.00,        ,   Convert 600k EUR to CHF @ 0.9993,
        # FX gain/loss: transactions in base currency with zero foreign currency amount
    13, 2024-06-26, 9991,            9995,      CHF,     999.00,                     ,        ,   Foreign currency adjustment
    14, 2024-06-26, 9990,                ,      EUR,       0.00,                 5.55,        ,   Foreign currency adjustment
    14, 2024-06-26,     ,            9995,      CHF,       5.55,                     ,        ,   Foreign currency adjustment
        # Transactions with two non-base currencies
    15, 2024-06-26,     ,            9991,      USD,  100000.00,             90000.00,        ,   Convert 100k USD to EUR @ 0.9375,
    15, 2024-06-26, 9990,                ,      EUR,   93750.00,             90000.00,        ,   Convert 100k USD to EUR @ 0.9375,
    16, 2024-06-26,     ,            9991,      USD,  200000.00,            180000.00,        ,   Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 9990,                ,      EUR,   93750.00,             90000.00,        ,   Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 9992,                ,      CHF,   90000.00,             90000.00,        ,   Convert 200k USD to EUR and CHF,
        # Foreign currency transaction exceeding precision for exchange rates in CashCtrl
    17, 2024-06-26, 9991,            9994,      USD,90000000.00,          81111111.11,        ,   Value 90 Mio USD @ 0.9012345679 with 10 digits precision,
    18, 2024-06-26,     ,            9994,      USD, 9500000.00,           7888888.88,        ,   Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
    18, 2024-06-26, 9992,                ,      CHF, 7888888.88,                     ,        ,   Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
"""

# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)