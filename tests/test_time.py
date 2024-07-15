import datetime
import pytest
from pyledger import parse_date_span, last_day_of_month

@pytest.mark.parametrize(
    "input_date, expected_date",
    [
        (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31)),
        (datetime.date(2023, 2, 15), datetime.date(2023, 2, 28)),  # Non-leap year
        (datetime.date(2024, 2, 15), datetime.date(2024, 2, 29)),  # Leap year
        (datetime.date(2023, 12, 31), datetime.date(2023, 12, 31)),
    ],
)
def test_last_day_of_month(input_date, expected_date):
    assert last_day_of_month(input_date) == expected_date

def test_parse_date_span_none():
    assert parse_date_span(None) == (None, None)

@pytest.mark.parametrize(
    "input_date, expected_span",
    [
        (datetime.date(2023, 1, 1), (None, datetime.date(2023, 1, 1))),
        (datetime.datetime(2023, 1, 1), (None, datetime.date(2023, 1, 1))),
        ("2023-01-01", (None, datetime.date(2023, 1, 1))),
        ("2023-01", (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31))),
        ("2023-Q1", (datetime.date(2023, 1, 1), datetime.date(2023, 3, 31))),
        ("2023", (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31))),
        (2023, (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31))),
    ],
)
def test_parse_date_span_valid(input_date, expected_span):
    assert parse_date_span(input_date) == expected_span

@pytest.mark.parametrize(
    "invalid_input",
    ["invalid", 123.456],
)
def test_parse_date_span_invalid(invalid_input):
    with pytest.raises(ValueError):
        parse_date_span(invalid_input)
