import datetime, json, numpy as np, os, pandas as pd, warnings, pyledger
from accountbot import ExcelLedger
from pathlib import Path

engine = "Excel"

if engine == 'Text':
    accounts = pyledger.TextLedger(
        settings="~/macx/accounts/settings.json",
        accounts="~/macx/accounts/account_chart.csv",
        prices="~/macx/accounts/prices",
        ledger="~/macx/accounts/ledger",
        vat_codes="~/macx/accounts/vat_codes.csv",
        fx_adjustments="~/macx/accounts/fx_adjustments.csv",
    )
elif engine == 'Excel':
    accounts = ExcelLedger(
        settings="~/macx/accounts/settings.json",
        accounts="~/macx/accounts/account_chart.csv",
        prices="~/macx/accounts/prices",
        ledger="~/macx/accounting/2023/batch_postings",
        vat_codes="~/macx/accounts/vat_codes.csv",
        fx_adjustments="~/macx/accounts/fx_adjustments.csv",
    )
else:
    raise ValueError(f"Unknown engine: '{engine}'. "
                     f"Use one of 'accounts', 'Cachedaccounts', or 'Text'.")

accounts.read_ledger()
self = accounts
self.export_xlsx_account_sheets("bla.xlsx")