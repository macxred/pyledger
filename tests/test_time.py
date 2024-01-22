import datetime
import unittest
from pyledger import parse_date_span, last_day_of_month

class TestTimeModule(unittest.TestCase):

    def test_last_day_of_month(self):
        self.assertEqual(last_day_of_month(datetime.date(2023, 1, 1)), datetime.date(2023, 1, 31))
        self.assertEqual(last_day_of_month(datetime.date(2023, 2, 15)), datetime.date(2023, 2, 28))  # Non-leap year
        self.assertEqual(last_day_of_month(datetime.date(2024, 2, 15)), datetime.date(2024, 2, 29))  # Leap year
        self.assertEqual(last_day_of_month(datetime.date(2023, 12, 31)), datetime.date(2023, 12, 31))

    def test_parse_date_span_none(self):
        self.assertEqual(parse_date_span(None), (None, None))

    def test_parse_date_span_date(self):
        self.assertEqual(parse_date_span(datetime.date(2023, 1, 1)), (None, datetime.date(2023, 1, 1)))

    def test_parse_date_span_datetime(self):
        self.assertEqual(parse_date_span(datetime.datetime(2023, 1, 1)), (None, datetime.date(2023, 1, 1)))

    def test_parse_date_span_str_isoformat(self):
        self.assertEqual(parse_date_span("2023-01-01"), (None, datetime.date(2023, 1, 1)))

    def test_parse_date_span_str_month(self):
        self.assertEqual(parse_date_span("2023-01"), (datetime.date(2023, 1, 1), datetime.date(2023, 1, 31)))

    def test_parse_date_span_str_quarter(self):
        self.assertEqual(parse_date_span("2023-Q1"), (datetime.date(2023, 1, 1), datetime.date(2023, 3, 31)))

    def test_parse_date_span_str_year(self):
        self.assertEqual(parse_date_span("2023"), (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31)))

    def test_parse_date_span_int_year(self):
        self.assertEqual(parse_date_span(2023), (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31)))

    def test_parse_date_span_invalid_str(self):
        with self.assertRaises(ValueError):
            parse_date_span("invalid")

    def test_parse_date_span_invalid_type(self):
        with self.assertRaises(ValueError):
            parse_date_span(123.456)

if __name__ == '__main__':
    unittest.main()
