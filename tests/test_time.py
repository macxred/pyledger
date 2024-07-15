"""Tests for date processing helper functions in pyledger."""

import datetime
from pyledger import last_day_of_month, parse_date_span
import pytest


@pytest.mark.parametrize(
    "input_date, expected_date",
    [
        (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31)),
        (datetime.date(2023, 2, 15), datetime.date(2023, 2, 28)),  # Non-leap year
        (datetime.date(2024, 2, 15), datetime.date(2024, 2, 29)),  # Leap year
        (datetime.date(2023, 12, 31), datetime.date(2023, 12, 31)),
    ],
)
def test_last_day_of_month(input_date: datetime.date, expected_date: datetime.date):
    """Test the last_day_of_month function."""
    assert last_day_of_month(input_date) == expected_date


def test_parse_date_span_none():
    """Test the parse_date_span function with None input."""
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
def test_parse_date_span_valid(
    input_date: datetime.date | datetime.datetime | str | int,
    expected_span: tuple[datetime.date | None, datetime.date | None]
):
    """Test the parse_date_span function with valid inputs."""
    assert parse_date_span(input_date) == expected_span


@pytest.mark.parametrize(
    "invalid_input",
    ["invalid", 123.456],
)
def test_parse_date_span_invalid(invalid_input: str | float):
    """Test the parse_date_span function with invalid inputs."""
    with pytest.raises(ValueError):
        parse_date_span(invalid_input)
