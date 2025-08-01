"""Test suite for testing account_sheet_tables() method."""

import math
import pytest
import pandas as pd
from pyledger.memory_ledger import MemoryLedger
from .base_test import BaseTest
from io import StringIO
from consistent_df import assert_frame_equal


COLUMNS_CSV = """
    column,          label,            width,   align
    date,            Date,             1.2fr,    left
    contra,          Contra,           1.2fr,    right
    currency,        Currency,         auto,    right
    amount,          Amount,           1.2fr,   right
    report_amount,   Report Amount,    1.2fr,   right
    balance,         Balance,          1.2fr,   right
    report_balance,  Report Balance,   1.2fr,   right
    tax_code,        Tax Code,         auto,    left
    description,     Description,      2fr,     left
    document,        Document,         2fr,     left
"""
COLUMNS = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True)

# flake8: noqa: E501
EXPECTED_TYPST = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1.2fr, auto, 1.2fr, 1.2fr, 1.2fr, 1.2fr, 2fr, 2fr),\n"
    "  align: (left, right, right, right, right, right, left, left),\n"
    "  [Date], [Currency], [Amount], [Report Amount], [Balance], [Report Balance], [Description], [Document],\n"
    "  table.hline(),\n"
    "  [2024-01-01], [JPY], [42,000,000], [298,200.00], [42,000,000], [298,200.00], [Opening balance], [#link(\"test/test/2023/financials/balance_sheet.pdf\")[2023/financials/balance_sheet.pdf]],\n"
    "  [2024-03-31], [JPY], [], [-21,000.00], [42,000,000], [277,200.00], [FX revaluations], [],\n"
    "  [2024-06-30], [JPY], [], [-16,800.00], [42,000,000], [260,400.00], [FX revaluations], [],\n"
    "  [2024-07-04], [JPY], [12,345,678], [76,386.36], [54,345,678], [336,786.36], [Convert JPY to EUR], [#link(\"test/test/2024/transfers/2024-07-05_JPY-EUR.pdf\")[2024/transfers/2024-07-05_JPY-EUR.pdf]],\n"
    "  [2024-09-10], [JPY], [], [5.55], [54,345,678], [336,791.91], [Manual Foreign currency adjustment], [],\n"
    "  [2024-09-30], [JPY], [], [43,627.84], [54,345,678], [380,419.75], [FX revaluations], [],\n"
    ")"
)
EXPECTED_DATAFRAME_CSV = """
date,         currency,   amount,          report_amount,   balance,      report_balance,   description,                          document
2024-01-01,   JPY,        42'000'000,      298'200.00,      42'000'000,   298'200.00,       Opening balance,                      2023/financials/balance_sheet.pdf
2024-03-31,   JPY,        ,                -21'000.00,      42'000'000,   277'200.00,       FX revaluations,
2024-06-30,   JPY,        ,                -16'800.00,      42'000'000,   260'400.00,       FX revaluations,
2024-07-04,   JPY,        12'345'678,      76'386.36,       54'345'678,   336'786.36,       Convert JPY to EUR,                   2024/transfers/2024-07-05_JPY-EUR.pdf
2024-09-10,   JPY,        ,                5.55,            54'345'678,   336'791.91,       Manual Foreign currency adjustment,
2024-09-30,   JPY,        ,                43'627.84,       54'345'678,   380'419.75,       FX revaluations,
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
        columns=COLUMNS,
        root_url="test/test/",
    )
    assert result[1020].strip() == EXPECTED_TYPST.strip()


def test_account_balance_dataframe_format(restored_engine):
    def format_number(x: float, decimal_places: float) -> str:
        if pd.isna(x) or pd.isna(decimal_places):
            return ""
        return f"{x:,.{decimal_places}f}".replace(",", "'")

    result = restored_engine.account_sheet_tables(
        period="2025-12-31",
        root_url="test/test/",
        output="dataframe",
        format_number=format_number
    )
    EXPECTED = EXPECTED_DATAFRAME.fillna("")
    EXPECTED["document"] = EXPECTED["document"].replace("", pd.NA).fillna(pd.NA)
    assert_frame_equal(result[1020], EXPECTED, check_dtype=False)

def test_invalid_columns_raises_value_error(restored_engine):
    bad_cols = COLUMNS.copy()
    bad_cols.loc[len(bad_cols)] = ["nonexistent_col", "Foo", "auto", "left"]
    with pytest.raises(ValueError, match="not valid account history fields"):
        restored_engine.account_sheet_tables(
            period="2025-12-31",
            columns=bad_cols,
            root_url="test/test/",
        )


def test_accounts_filter_single_account(restored_engine):
    result = restored_engine.account_sheet_tables(
        period="2025-12-31",
        columns=COLUMNS,
        root_url="test/test/",
        output="dataframe",
        accounts=1020,
    )
    assert set(result.keys()) == {1020}
