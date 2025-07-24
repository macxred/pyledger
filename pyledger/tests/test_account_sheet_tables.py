"""Test suite for testing account_sheet_tables() method."""

import pytest
import pandas as pd
from pyledger.memory_ledger import MemoryLedger
from .base_test import BaseTest
from io import StringIO
from consistent_df import assert_frame_equal


CONFIG_CSV = """
    column,          label,            width,   align
    date,            Date,             auto,    left
    contra,          Contra,           auto,    right
    currency,        Currency,         auto,    right
    amount,          Amount,           1.2fr,   right
    report_amount,   Report Amount,    1.2fr,   right
    balance,         Balance,          1.2fr,   right
    report_balance,  Report Balance,   1.2fr,   right
    tax_code,        Tax Code,         auto,    left
    description,     Description,      2fr,     left
    document,        Document,         2fr,     left
"""
CONFIG = pd.read_csv(StringIO(CONFIG_CSV), skipinitialspace=True)

# flake8: noqa: E501
EXPECTED_TYPST = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (auto, auto, 1.2fr, 1.2fr, 1.2fr, 1.2fr, 2fr, 2fr),\n"
    "  align: (left, right, right, right, right, right, left, left),\n"
    "  [*DATE*], [*CURRENCY*], [*AMOUNT*], [*REPORT\\-AMOUNT*], [*BALANCE*], [*REPORT\\-BALANCE*], [*DESCRIPTION*], [*DOCUMENT*],\n"
    "  [2024-01-01], [JPY], [42'000'000.00], [298'200.00], [42'000'000.00], [298'200.00], [Opening balance], [link(\"test/test/2023/financials/balance_sheet.pdf\", \"2023/financials/balance_sheet.pdf\")],\n"
    "  table.hline(),\n"
    "  [2024-03-31], [JPY], [], [-21'000.00], [42'000'000.00], [277'200.00], [FX revaluations], [],\n"
    "  [2024-06-30], [JPY], [], [-16'800.00], [42'000'000.00], [260'400.00], [FX revaluations], [],\n"
    "  [2024-07-04], [JPY], [12'345'678.00], [76'386.36], [54'345'678.00], [336'786.36], [Convert JPY to EUR], [link(\"test/test/2024/transfers/2024-07-05_JPY-EUR.pdf\", \"2024/transfers/2024-07-05_JPY-EUR.pdf\")],\n"
    "  [2024-09-10], [JPY], [], [5.55], [54'345'678.00], [336'791.91], [Manual Foreign currency adjustment], [],\n"
    "  [2024-09-30], [JPY], [], [43'627.84], [54'345'678.00], [380'419.75], [FX revaluations], [],\n"
    ")"
)
EXPECTED_DATAFRAME_CSV = """
date,         currency,   amount,          report_amount,   balance,         report_balance,   description,                          document
2024-01-01,   JPY,        42'000'000.00,   298'200.00,      42'000'000.00,   298'200.00,       Opening balance,                      "link(""test/test/2023/financials/balance_sheet.pdf"", ""2023/financials/balance_sheet.pdf"")"
2024-03-31,   JPY,        ,                -21'000.00,      42'000'000.00,   277'200.00,       FX revaluations,
2024-06-30,   JPY,        ,                -16'800.00,      42'000'000.00,   260'400.00,       FX revaluations,
2024-07-04,   JPY,        12'345'678.00,   76'386.36,       54'345'678.00,   336'786.36,       Convert JPY to EUR,                   "link(""test/test/2024/transfers/2024-07-05_JPY-EUR.pdf"", ""2024/transfers/2024-07-05_JPY-EUR.pdf"")"
2024-09-10,   JPY,        ,                5.55,            54'345'678.00,   336'791.91,       Manual Foreign currency adjustment,
2024-09-30,   JPY,        ,                43'627.84,       54'345'678.00,   380'419.75,       FX revaluations,
"""
EXPECTED_DATAFRAME = pd.read_csv(StringIO(EXPECTED_DATAFRAME_CSV), index_col=False, skipinitialspace=True)
# flake8: enable


@pytest.fixture()
def restored_engine():
    engine = MemoryLedger()
    engine.restore(
        accounts=BaseTest.ACCOUNTS, configuration=BaseTest.CONFIGURATION,
        journal=BaseTest.JOURNAL, assets=BaseTest.ASSETS, price_history=BaseTest.PRICES,
        revaluations=BaseTest.REVALUATIONS, profit_centers=BaseTest.PROFIT_CENTERS,
        tax_codes=BaseTest.TAX_CODES,
    )
    return engine


def test_account_balance_typst_format(restored_engine):
    result = restored_engine.account_sheet_tables(
        period="2025-12-31",
        config=CONFIG,
        root_folder="test/test/",
    )
    assert result[1020].strip() == EXPECTED_TYPST.strip()


def test_account_balance_dataframe_format(restored_engine):
    result = restored_engine.account_sheet_tables(
        period="2025-12-31",
        config=CONFIG,
        root_folder="test/test/",
        output="dataframe"
    )
    assert_frame_equal(result[1020], EXPECTED_DATAFRAME.fillna(""), check_dtype=False)
