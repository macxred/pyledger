"""Test suite for reporting helpers."""

import pandas as pd
from io import StringIO
import pytest
from consistent_df import assert_frame_equal
from pyledger.reporting import summarize_groups


BALANCE_CSV = """
group,                                               description,                report_balance
Assets/Other,                                        Cash in other Bank EUR,     -22.34
Assets/Cash/UBS,                                     Cash in Bank CHF,           100000.0
Assets/Cash/Bank of America,                         Cash in Bank USD,           1076311.79
Liabilities/Payables,                                Accounts Payable USD,       700.0
Liabilities/Current Liabilities/Accrued,             VAT payable (output tax),   -807.94
"""
BALANCE = pd.read_csv(StringIO(BALANCE_CSV), skipinitialspace=True)


def test_summarize_groups():
    EXPECTED_OUTPUT_CSV = """
    level,group,                                     description,                    report_balance
    H1,   Assets,                                    Assets,
    gap,  Assets/Other,                              ,
    H2,   Assets/Other,                              Other,
    body, Assets/Other,                              Cash in other Bank EUR,         -22.34
    S2,   Assets/Other,                              Total Other,                    -22.34
    gap,  Assets/Other,                              ,
    H2,   Assets/Cash,                               Cash,
    gap,  Assets/Cash/UBS,                           ,
    H3,   Assets/Cash/UBS,                           UBS,
    body, Assets/Cash/UBS,                           Cash in Bank CHF,               100000.0
    S3,   Assets/Cash/UBS,                           Total UBS,                      100000.0
    gap,  Assets/Cash/UBS,                           ,
    H3,   Assets/Cash/Bank of America,               Bank of America,
    body, Assets/Cash/Bank of America,               Cash in Bank USD,               1076311.79
    S3,   Assets/Cash/Bank of America,               Total Bank of America,          1076311.79
    gap,  Assets/Cash/Bank of America,               ,
    S2,   Assets/Cash,                               Total Cash,                     1176311.79
    gap,  Assets/Cash,                               ,
    S1,   Assets,                                    Total Assets,                   1176289.45
    gap,  Assets,                                    ,
    H1,   Liabilities,                               Liabilities,
    gap,  Liabilities/Payables,                      ,
    H2,   Liabilities/Payables,                      Payables,
    body, Liabilities/Payables,                      Accounts Payable USD,           700.0
    S2,   Liabilities/Payables,                      Total Payables,                 700.0
    gap,  Liabilities/Payables,                      ,
    H2,   Liabilities/Current Liabilities,           Current Liabilities,
    gap,  Liabilities/Current Liabilities/Accrued,   ,
    H3,   Liabilities/Current Liabilities/Accrued,   Accrued,
    body, Liabilities/Current Liabilities/Accrued,   VAT payable (output tax),       -807.94
    S3,   Liabilities/Current Liabilities/Accrued,   Total Accrued,                  -807.94
    gap,  Liabilities/Current Liabilities/Accrued,   ,
    S2,   Liabilities/Current Liabilities,           Total Current Liabilities,      -807.94
    gap,  Liabilities/Current Liabilities,           ,
    S1,   Liabilities,                               Total Liabilities,              -107.94
    """
    EXPECTED = pd.read_csv(StringIO(EXPECTED_OUTPUT_CSV), skipinitialspace=True)
    result = summarize_groups(BALANCE)
    EXPECTED["description"] = EXPECTED["description"].fillna("")
    EXPECTED["report_balance"] = EXPECTED["report_balance"].astype("Float64")
    assert_frame_equal(result, EXPECTED, check_dtype=False)


def test_summarize_groups_staggered():
    EXPECTED_OUTPUT_CSV = """
    level,group,                                     description,                    report_balance
    H2,   Assets/Other,                              Other,
    body, Assets/Other,                              Cash in other Bank EUR,         -22.34
    S2,   Assets/Other,                              Total Other,                    -22.34
    gap,  Assets/Other,                              ,
    H2,   Assets/Cash,                               Cash,
    gap,  Assets/Cash/UBS,                           ,
    H3,   Assets/Cash/UBS,                           UBS,
    body, Assets/Cash/UBS,                           Cash in Bank CHF,               100000.0
    S3,   Assets/Cash/UBS,                           Total UBS,                      100000.0
    gap,  Assets/Cash/UBS,                           ,
    H3,   Assets/Cash/Bank of America,               Bank of America,
    body, Assets/Cash/Bank of America,               Cash in Bank USD,               1076311.79
    S3,   Assets/Cash/Bank of America,               Total Bank of America,          1076311.79
    gap,  Assets/Cash/Bank of America,               ,
    S2,   Assets/Cash,                               Total Cash,                     1176311.79
    gap,  Assets/Cash,                               ,
    H1,   Assets,                                    Assets,                         1176289.45
    gap,  Assets,                                    ,
    H2,   Liabilities/Payables,                      Payables,
    body, Liabilities/Payables,                      Accounts Payable USD,           700.0
    S2,   Liabilities/Payables,                      Total Payables,                 700.0
    gap,  Liabilities/Payables,                      ,
    H2,   Liabilities/Current Liabilities,           Current Liabilities,
    gap,  Liabilities/Current Liabilities/Accrued,   ,
    H3,   Liabilities/Current Liabilities/Accrued,   Accrued,
    body, Liabilities/Current Liabilities/Accrued,   VAT payable (output tax),       -807.94
    S3,   Liabilities/Current Liabilities/Accrued,   Total Accrued,                  -807.94
    gap,  Liabilities/Current Liabilities/Accrued,   ,
    S2,   Liabilities/Current Liabilities,           Total Current Liabilities,      -807.94
    gap,  Liabilities/Current Liabilities,           ,
    H1,   Liabilities,                               Liabilities,                    1176181.51
    """
    EXPECTED = pd.read_csv(StringIO(EXPECTED_OUTPUT_CSV), skipinitialspace=True)
    result = summarize_groups(BALANCE, staggered=True)
    EXPECTED["description"] = EXPECTED["description"].fillna("")
    EXPECTED["report_balance"] = EXPECTED["report_balance"].astype("Float64")
    assert_frame_equal(result, EXPECTED, check_dtype=False)


def test_missing_required_columns():
    df = pd.DataFrame({
        "wrong_group": ["A/B"], "description": ["X"], "value": [1.0]
    })
    with pytest.raises(ValueError, match="DataFrame missing required columns"):
        summarize_groups(df, group="group", summarize={"value": "sum"})

    df = pd.DataFrame({
        "group": ["A/B"], "description": ["X"], "value_test": [1.0]
    })
    with pytest.raises(ValueError, match="DataFrame missing required columns"):
        summarize_groups(df, group="group", summarize={"value": "sum"})


def test_empty_dataframe():
    df = pd.DataFrame(columns=["group", "description", "amount"])
    result = summarize_groups(df, summarize={"amount": "sum"})
    assert result.empty
    assert set(result.columns) == {"level", "group", "description", "amount"}


def test_leading_space_inserts_blank_rows():
    df = pd.DataFrame({
        "group": ["A/B/C"], "description": ["desc"], "amount": [1.0]
    })
    result = summarize_groups(df, summarize={"amount": "sum"}, leading_space=2)

    gap_rows = result[result["level"] == "gap"]
    header_rows = result[result["level"].str.match(r"H\d")]
    assert len(gap_rows) >= 2
    assert any(header_rows["level"] == "H2")
