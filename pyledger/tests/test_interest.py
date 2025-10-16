import datetime
import pytest
import pandas as pd
from io import StringIO
from pyledger import MemoryLedger
from pyledger.ledger_engine import LedgerEngine
from consistent_df import assert_frame_equal, enforce_schema
from pyledger.constants import INTEREST_CALCULATION_SCHEMA, PRINCIPAL_HISTORY_SCHEMA


@pytest.mark.parametrize(
    "start_date, end_date, convention, expected",
    [
        # ACT/365
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 31), "ACT/365", 30 / 365),
        (datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), "ACT/365", 365 / 365),
        (datetime.date(2024, 1, 1), datetime.date(2025, 1, 1), "ACT/365", 366 / 365),  # Leap year
        # ACT/360
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 31), "ACT/360", 30 / 360),
        (datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), "ACT/360", 365 / 360),
        # 30/360
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 31), "30/360", 30 / 360),
        (datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), "30/360", 360 / 360),
        (datetime.date(2024, 1, 31), datetime.date(2024, 2, 29), "30/360", 29 / 360),  # Month-end
        (datetime.date(2024, 1, 30), datetime.date(2024, 2, 29), "30/360", 29 / 360),  # Month-end
        # ACT/ACT
        (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31), "ACT/ACT", 364 / 365),  # Non-leap
        (datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), "ACT/ACT", 365 / 366),  # Leap year
        # Common periods
        (datetime.date(2024, 1, 1), datetime.date(2024, 4, 1), "ACT/365", 91 / 365),  # Quarter
        (datetime.date(2024, 1, 1), datetime.date(2024, 7, 1), "ACT/360", 182 / 360),  # Half year
        (datetime.date(2024, 1, 1), datetime.date(2025, 1, 1), "30/360", 1.0),  # Full year
        (datetime.date(2024, 12, 1), datetime.date(2025, 1, 31), "ACT/ACT", 61 / 366),  # Cross year
        # Edge case: same day
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), "ACT/365", 0.0),
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), "ACT/360", 0.0),
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), "30/360", 0.0),
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 1), "ACT/ACT", 0.0),
    ]
)
def test_day_count_factor(start_date, end_date, convention, expected):
    result = LedgerEngine._calculate_day_count_factor(start_date, end_date, convention)
    TOLERANCE = 1e-9
    assert abs(result - expected) < TOLERANCE, (
        f"Expected {expected}, got {result} for {convention} "
        f"from {start_date} to {end_date}"
    )


def test_day_count_factor_invalid_convention():
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 12, 31)
    with pytest.raises(ValueError, match="Unsupported day count convention"):
        LedgerEngine._calculate_day_count_factor(start, end, "INVALID")


@pytest.fixture
def engine():
    return MemoryLedger()


def test_calculate_interest(engine):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,        balance, description
        2024-01-15,  10000.00, Initial loan disbursement
        2024-02-01,  10000.00, Month end
        2024-03-15,  12500.00, Additional drawdown
        2024-04-01,  12500.00, Month end
        2024-05-20,   8000.00, Partial repayment
        2024-06-30,   8000.00, Quarter end
        2024-07-01,      0.00, Full repayment
        2024-08-01,   5000.00, New loan
        2024-09-30,   5000.00, End
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    expected = enforce_schema(pd.read_csv(StringIO("""
        date,       amount,                days, description
        2024-02-01,  27.945205479452056,   17,  Interest from 2024-01-15 to 2024-02-01
        2024-03-15,  70.68493150684931,    43,  Interest from 2024-02-01 to 2024-03-15
        2024-04-01,  34.93150684931507,    17,  Interest from 2024-03-15 to 2024-04-01
        2024-05-20,  100.68493150684932,   49,  Interest from 2024-04-01 to 2024-05-20
        2024-06-30,  53.91780821917808,    41,  Interest from 2024-05-20 to 2024-06-30
        2024-07-01,  1.3150684931506849,   1,   Interest from 2024-06-30 to 2024-07-01
        2024-09-30,  49.31506849315068,    60,  Interest from 2024-08-01 to 2024-09-30
    """), skipinitialspace=True), INTEREST_CALCULATION_SCHEMA)

    result = engine.calculate_interest(history, 0.06, "ACT/365")
    assert_frame_equal(result, expected, ignore_index=True, check_like=True)


def test_calculate_interest_zero_balance(engine):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,       balance, description
        2024-01-01, 1000.00, Active
        2024-02-01,    0.00, Zero
        2024-03-01, 1000.00, Active again
        2024-04-01, 1000.00, End
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    expected = enforce_schema(pd.read_csv(StringIO("""
        date,       amount,                days, description
        2024-02-01, 10.191780821917808,   31,   Interest from 2024-01-01 to 2024-02-01
        2024-04-01, 10.191780821917808,   31,   Interest from 2024-03-01 to 2024-04-01
    """), skipinitialspace=True), INTEREST_CALCULATION_SCHEMA)

    result = engine.calculate_interest(history, 0.12, "ACT/365")
    assert_frame_equal(result, expected, ignore_index=True, check_like=True)


def test_calculate_interest_negative_balance(engine):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,        balance, description
        2024-01-01, -5000.00, Loan receivable
        2024-07-01, -5000.00, Mid-year
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)
    expected = enforce_schema(pd.read_csv(StringIO("""
        date,       amount,                  days, description
        2024-07-01, -199.452054794520564,     182,  Interest from 2024-01-01 to 2024-07-01
    """), skipinitialspace=True), INTEREST_CALCULATION_SCHEMA)

    result = engine.calculate_interest(history, 0.08, "ACT/365")
    assert_frame_equal(result, expected, ignore_index=True, check_like=True)


def test_calculate_interest_missing_columns_raises_error(engine):
    invalid_history = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
        "balance": [1000.00, 1100.00]
    })
    with pytest.raises(ValueError, match="missing required columns"):
        engine.calculate_interest(invalid_history, 0.05)


def test_calculate_interest_invalid_convention_raises_error(engine):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,       balance, description
        2024-01-01, 1000.00, Initial
        2024-02-01, 1000.00, Month 1
        2024-03-01, 1000.00, Month 2
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    with pytest.raises(ValueError, match="Unsupported day count convention"):
        engine.calculate_interest(history, 0.05, "INVALID")


def test_calculate_interest_single_entry(engine):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,       balance, description
        2024-01-01, 1000.00, Only entry
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    result = engine.calculate_interest(history, 0.05)

    assert result.empty
    assert list(result.columns) == ["date", "amount", "days", "description"]


def test_calculate_interest_unsorted_data_warns(engine, caplog):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,       balance, description
        2024-03-01, 1000.00, Last
        2024-01-01, 1000.00, First
        2024-02-01, 1000.00, Middle
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    result = engine.calculate_interest(history, 0.12, "ACT/365")

    assert "not sorted by date" in caplog.text
    assert len(result) == 2


def test_calculate_interest_duplicate_dates_warns(engine, caplog):
    history = enforce_schema(pd.read_csv(StringIO("""
        date,       balance, description
        2024-01-01, 1000.00, First
        2024-02-01, 1500.00, Duplicate 1
        2024-02-01,  800.00, Duplicate 2
        2024-03-01, 1000.00, Last
    """), skipinitialspace=True), PRINCIPAL_HISTORY_SCHEMA)

    result = engine.calculate_interest(history, 0.10, "ACT/365")

    assert "duplicate dates" in caplog.text
    assert len(result) == 2
