import pandas as pd
import re
from io import StringIO
from .standalone_ledger import StandaloneLedger

class TestLedger(StandaloneLedger):
    """
    Implementation of the StandaloneLedger class that is initiated with
    hard-coded settings, account chart and VAT codes. Its purpose is to
    facilitate demos, experimentation and testing.
    """
    SETTINGS = {
        'base_currency': 'CHF',
        'precision': {'CAD': 0.01, 'CHF': 0.01, 'EUR': 0.01,
                      'GBP': 0.01, 'HKD': 0.01, 'USD': 0.01}}

    SWISS_SME_ACCOUNT_CHART = """
        account, currency, vat_code, text
        1000,         CHF,         , Cash on hand
        1020,         CHF,         , Cash at Bank A
        1030,         CHF,         , Cash at Bank B
        1045,         CHF,         , Credit cards / debit cards
        1100,         CHF,         , Trade receivables
        1170,         CHF,         , VAT Input Tax
        1300,         CHF,         , Accrued income and prepaid expenses
        # 2 Liabilities
        2000,         CHF,         , Trade creditors / Accounts payable
        2030,         CHF,         , Prepayments received
        # 210 Current interest-bearing liabilities
        2100,         CHF,         , Bank borrowings and overdrafts
        2120,         CHF,         , Liabilities from finance leasing transactions
        2140,         CHF,         , Other current interest-bearing liabilities
        # 220 Other current liabilities
        2200,         CHF,         , VAT owed
        2201,         CHF,         , VAT according to VAT report
        2206,         CHF,         , Withholding tax owed
        2208,         CHF,         , Direct taxes
        2209,         CHF,         , Tax at source on wages and salaries owed / Withholding tax on
        2210,         CHF,         , Other current liabilities (non-interest-bearing)
        2269,         CHF,         , Dividends payable
        2270,         CHF,         , Social security and pension funds owed
        # 230 Deferred income and accrued expenses
        2300,         CHF,         , Deferred income and accrued expenses
        2330,         CHF,         , Short-term provisions
        4000,         CHF,         , Sales Revenue
        # 5 STAFF COSTS
        5000,         CHF,         , Purchases
        5000,         CHF,         , Wages and salaries
        5700,         CHF,         , Social securities and pension funds expenses
        5800,         CHF,         , Other staff expense
        5900,         CHF,         , Purchased services expense
        # 6 OTHER OPERATING EXPENSES, DEPRECIATIONS AND VALUE ADJUSTMENTS, FINANCIAL RESULT
        6000,         CHF,    InStd, Rent and associated expenses
        6100,         CHF,    InStd, Maintenance and repair
        6105,         CHF,    InStd, Leasing of movable tangible assets
        6200,         CHF,    InStd, Vehicle and transportation expenses
        6260,         CHF,    InStd, Leasing expense and rental of vehicles
        6300,         CHF,    InStd, Insurance expense
        6400,         CHF,    InStd, Energy and disposal expenses
        6500,         CHF,    InStd, Administrative expense
        6570,         CHF,    InStd, IT and computing costs
        6600,         CHF,    InStd, Advertising expense
        6700,         CHF,    InStd, Other operational expense
        6800,         CHF,    InStd, Depreciation and value adjustments on capital assets items
        6900,         CHF,    InStd, Financial expense
        6950,         CHF,    InStd, Financial income
        """

    SWISS_VAT = """
        id, date, account, inverse_account, rate, inclusive, text
        Exempt,   2001-01-01,     ,     , 0.0,    True, Exempt from VAT
        OutStd,   2001-01-01, 2200,     , 0.077,  True, VAT at the regular 7.7% rate on goods or services sold
        OutRed,   2001-01-01, 2200,     , 0.025,  True, VAT at the reduced 2.5% rate on goods or services sold
        OutAcc,   2001-01-01, 2200,     , 0.037,  True, VAT at the 2.5% accommodation rate on goods or services sold
        OutStdEx, 2001-01-01, 2200,     , 0.077, False, VAT at the regular 7.7% rate on goods or services sold
        InStd,    2001-01-01, 1170,     , 0.077,  True, Input Tax (Vorsteuer) at the regular 7.7% rate on purchased goods or services
        InRed,    2001-01-01, 1170,     , 0.025,  True, Input Tax (Vorsteuer) at the reduced 2.5% rate on purchased goods or services
        InAcc,    2001-01-01, 1170,     , 0.037,  True, Input Tax (Vorsteuer) at the 3.7% accommodation rate on purchased goods or services
        AcqStd,   2001-01-01, 1170, 2200, 0.077, False, Acquisition tax (Bezugsteuer): VAT on services purchased abroad for which no VAT has been levied yet.
        """

    def __init__(self):
        """
        Initialize the TestLedger with hard coded settings, account chat and
        VAT codes.
        """
        super().__init__(
            settings=self.SETTINGS,
            accounts=pd.read_csv(
                StringIO(self.drop_commented_lines(self.SWISS_SME_ACCOUNT_CHART)),
                skipinitialspace=True),
            vat_codes=pd.read_csv(
                StringIO(self.drop_commented_lines(self.SWISS_VAT)),
                skipinitialspace=True)
        )

    @staticmethod
    def drop_commented_lines(text):
        """
        Drop commented lines from a multi-line string.
        Commented lines are identified by a leading pound sign '#' following
        optional leading white space.
        """
        lines = text.splitlines()
        lines = [line for line in lines if not re.match("[ ]*#", line)]
        return '\n'.join(lines)


# import pandas as pd
# from pyledger import TestLedger
# ledger = TestLedger()
# add_ledger_entry("")