"""Helper functions for date processing in pyledger."""

import datetime
import re


def last_day_of_month(date: datetime.date) -> datetime.date:
    """Returns the last day of the month for a given date.

    Args:
        date (datetime.date): The date for which to find the last day of its month.

    Returns:
        datetime.date: The last day of the month for the given date.
    """
    next_month = date.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def parse_date_span(
    x: datetime.date | datetime.datetime | str | int | None
) -> tuple[datetime.date | None, datetime.date | None]:
    """Converts a given period, expressed as a date, datetime, string, integer, or None,
    into a tuple of start and end dates. The function handles single dates (returning
    (None, <date>)) to signify the period from inception to that date, and 'None' to
    represent an indefinite period (all available data).

    Args:
        x (datetime.date | datetime.datetime | str | int | None): The period to be interpreted.

    Returns:
        tuple: A tuple of two datetime.date objects or None, representing the start and
        end dates of the period.

    Examples:
        parse_date_span(None) -> (None, None)
        parse_date_span(datetime.date(2023, 1, 1)) -> (None, datetime.date(2023, 1, 1))
        parse_date_span("2023-01-01") -> (None, datetime.date(2023, 1, 1))
        parse_date_span("2023-01") -> (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31))
        parse_date_span("2023-Q1") -> (datetime.date(2023, 1, 1), datetime.date(2023, 3, 31))
        parse_date_span("2023") -> (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31))
    """
    if x is None:
        return (None, None)
    elif isinstance(x, datetime.datetime):
        return (None, x.date())
    elif isinstance(x, datetime.date):
        return (None, x)
    elif isinstance(x, (str, int)):
        x_str = str(x).strip()
        if re.fullmatch(r"[0-9]{4}", x_str):
            year = int(x_str)
            return (datetime.date(year, 1, 1), datetime.date(year, 12, 31))
        elif re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", x_str):
            return (None, datetime.date.fromisoformat(x_str))
        elif re.fullmatch(r"[0-9]{4}-[0-9]{2}", x_str):
            year = int(x_str[:4])
            month = int(x_str[5:7])
            start = datetime.date(year, month, 1)
            end = last_day_of_month(start)
            return (start, end)
        elif re.fullmatch(r"[0-9]{4}-Q[1-4]", x_str):
            year = int(x_str[:4])
            quarter = int(x_str[6])
            start = datetime.date(year, (quarter - 1) * 3 + 1, 1)
            end = last_day_of_month(datetime.date(year, quarter * 3, 1))
            return (start, end)
        else:
            raise ValueError(f"Cannot interpret '{x_str}' as an interval.")
    else:
        raise ValueError(f"Cannot interpret '{x}' of type {type(x).__name__} as an interval.")
