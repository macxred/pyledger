import unittest
import datetime
import pandas as pd
from pyledger import TestLedger

class TestStandaloneLedger(unittest.TestCase):
    def test_standardize_ledger_columns(self):
        ledger = TestLedger()
        postings = pd.DataFrame({
            'date': [datetime.date.today(), pd.NA, pd.NA, "2024-01-01"],
            'account': [1020, pd.NA, pd.NA, 1020],
            'counter_account': [pd.NA, 6000, 6100, 3000],
            'currency': 'CHF',
            'text': ["Collective entry", pd.NA, pd.NA, "Simple entry"],
            'amount': [1000, 800, 200, 10]})
        ledger.standardize_ledger_columns(postings)

        with self.assertRaises(ValueError) as context:
            # Attempt to standardize an entry without required 'account' column
            posting = pd.DataFrame({
                'date': [datetime.date.today()],
                'currency': 'CHF',
                'text': ["Entry without account column"],
                'amount': [1000]})
            ledger.standardize_ledger_columns(posting)

    def test_add_ledger_entry(self):
        ledger = TestLedger()

        # Add ledger entries
        for i in range(5):
            ledger.add_ledger_entry({
                'date': datetime.date.today(),
                'account': 1020,
                'counter_account': 6000,
                'currency': 'CHF',
                'text': f"Entry {i+1}",
                'amount': 100 * (i + 1)})

        with self.assertRaises(ValueError) as context:
            # Attempt to add an entry with a duplicate 'id'
            ledger.add_ledger_entry({
                'id': 1,  # Duplicate id
                'date': datetime.date.today(),
                'account': 1020,
                'counter_account': 3000,
                'currency': 'CHF',
                'text': "Duplicate Entry",
                'amount': 100})

        # Retrieve the original ledger
        original_ledger = ledger.ledger()
        self.assertIsInstance(original_ledger, pd.DataFrame)
        self.assertEqual(len(original_ledger), 5)
        self.assertFalse(original_ledger['id'].duplicated().any())

        # Retrieve serialized ledger
        serialized_ledger = ledger.serialized_ledger()
        self.assertIsInstance(serialized_ledger, pd.DataFrame)
        self.assertEqual(len(serialized_ledger), 10)

    def test_vat_journal_entries(self):
        ledger = TestLedger()

        postings = pd.DataFrame({
            'date': datetime.date.today(),
            'account': [1020, 1020, 1020, 6300, 1030, 6500],
            'counter_account': [6000, 6100, 6200, 1020, 6400, 1045],
            'currency': 'CHF',
            'text': ["Entry without VAT code",
                     "Amount exempt from VAT",
                     "Amount Including Input Tax",
                     "Amount Including Input Tax (accounts inverted)",
                     "Amount Excluding Output Tax",
                     "Amount Excluding Output Tax (accounts inverted)",],
            'vat_code': [None, "Exempt", "InStd", "InStd", "OutStdEx", "OutStdEx"],
            'amount': [-1000, -1000, -1000, 1000, -1000, 1000]})
        postings = ledger.standardize_ledger(postings)
        postings = ledger.sanitize_ledger(postings)

        # Posting without VAT code does not generate any VAT journal entries
        df = ledger.vat_journal_entries(postings.iloc[0:1, :])
        self.assertEqual(len(df), 0)

        # Posting exempt from VAT does not generate any VAT journal entries
        df = ledger.vat_journal_entries(postings.iloc[1:2, :])
        self.assertEqual(len(df), 0)

        # Journal entry for posting including VAT
        df = ledger.vat_journal_entries(postings.iloc[2:3, :])
        expected_vat = round(-1000 * 0.077 / (1 + 0.077), 2)
        self.assertEqual(len(df), 1)
        self.assertEqual(df['account'].item(), 6200)
        self.assertEqual(df['counter_account'].item(), 1170)
        self.assertEqual(df['amount'].item(), expected_vat)

        # Similar posting: inverted accounts and inverse amount
        df = ledger.vat_journal_entries(postings.iloc[3:4, :])
        self.assertEqual(len(df), 1)
        self.assertEqual(df['account'].item(), 6300)
        self.assertEqual(df['counter_account'].item(), 1170)
        self.assertEqual(df['amount'].item(), expected_vat)

        # Journal entry for posting excluding VAT
        df = ledger.vat_journal_entries(postings.iloc[4:5, :])
        expected_vat = round(-1000 * 0.077, 2)
        self.assertEqual(len(df), 1)
        self.assertEqual(df['account'].item(), 1030)
        self.assertEqual(df['counter_account'].item(), 2200)
        self.assertEqual(df['amount'].item(), expected_vat)

        # Similar posting: inverted accounts and inverse amount
        df = ledger.vat_journal_entries(postings.iloc[5:6, :])
        self.assertEqual(len(df), 1)
        self.assertEqual(df['account'].item(), 1045)
        self.assertEqual(df['counter_account'].item(), 2200)
        self.assertEqual(df['amount'].item(), expected_vat)

        # Validate account balance
        ledger.add_ledger_entry(postings)
        self.assertEqual(len(ledger.account_history(6000)), 1)
        self.assertEqual(len(ledger.account_history(6100)), 1)
        self.assertEqual(len(ledger.account_history(6200)), 2)
        self.assertEqual(len(ledger.account_history(6300)), 2)
        self.assertEqual(len(ledger.account_history(6400)), 1)
        self.assertEqual(len(ledger.account_history(6500)), 1)
        self.assertEqual(len(ledger.account_history(1020)), 4)
        self.assertEqual(len(ledger.account_history(1030)), 2)
        self.assertEqual(len(ledger.account_history(1045)), 2)
        self.assertEqual(ledger.account_balance(6000)['CHF'], 1000.0)
        self.assertEqual(ledger.account_balance(6100)['CHF'], 1000.0)
        expected_balance = round(1000 / (1 + 0.077), 2)
        self.assertEqual(ledger.account_balance(6200)['CHF'], expected_balance)
        self.assertEqual(ledger.account_balance(6300)['CHF'], expected_balance)
        self.assertEqual(ledger.account_balance(6400)['CHF'], 1000.0)
        self.assertEqual(ledger.account_balance(6500)['CHF'], 1000.0)
        self.assertEqual(ledger.account_balance(1020)['CHF'], -4000.0)
        expected_balance = -1 * round(1000 * (1 + 0.077), 2)
        self.assertEqual(ledger.account_balance(1030)['CHF'], expected_balance)
        self.assertEqual(ledger.account_balance(1045)['CHF'], expected_balance)

if __name__ == '__main__':
    unittest.main()
