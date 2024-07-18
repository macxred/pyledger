"""Constants used throughout the application."""

FX_ADJUSTMENTS_COLUMNS = {
    "date": "datetime64[ns, Europe/Berlin]",
    "adjust": "string[python]",
    "credit": "Int64",
    "debit": "Int64",
    "text": "string[python]",
}

VAT_CODE_COLUMNS = {
    "id": "string[python]",
    "date": "datetime64[ns, Europe/Berlin]",
    "text": "string[python]",
    "inclusive": "bool",
    "account": "Int64",
    "inverse_account": "Int64",
    "rate": "Float64",
}

ACCOUNT_CHART_COLUMNS = {
    "account": "Int64",
    "currency": "string[python]",
    "text": "string[python]",
    "vat_code": "string[python]",
    "group": "string[python]",
}

LEDGER_COLUMNS = {
    "id": "string[python]",
    "date": "datetime64[ns, Europe/Berlin]",
    "account": "Int64",
    "counter_account": "Int64",
    "currency": "string[python]",
    "amount": "Float64",
    "base_currency_amount": "Float64",
    "vat_code": "string[python]",
    "text": "string[python]",
    "document": "string[python]",
}

PRICE_COLUMNS = {
    "ticker": "string[python]",
    "currency": "string[python]",
    "price": "Float64",
    "date": "datetime64[ns, Europe/Berlin]",
}
