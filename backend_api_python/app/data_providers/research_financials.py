"""Bounded, best-effort financial observations; never verified historical vintages."""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re

from app.services.fundamental_data import FUNDAMENTAL_FIELDS, _finite_or_none

CONCEPTS = {
    'TotalRevenue': 'revenue', 'NetIncome': 'net_income',
    'StockholdersEquity': 'shareholder_equity', 'TotalDebt': 'total_debt',
    'FreeCashFlow': 'free_cash_flow', 'OrdinarySharesNumber': 'shares_outstanding',
}
CORE_FIELDS = tuple(CONCEPTS.values())
CURRENCIES = frozenset('USD GBP EUR CAD AUD NZD CHF JPY CNY HKD SGD INR BRL ZAR SEK NOK DKK PLN MXN KRW TWD ILS TRY AED SAR IDR MYR THB PHP'.split())


class FinancialUnavailable(Exception):
    """No usable, validated statement data; not a retryable transport failure."""


class FinancialRetryable(Exception):
    """Provider transport failure; apply shared cooldown before retrying."""


def _request(symbol, frequency):
    from yfinance.data import YfData
    now = datetime.now(timezone.utc)
    return YfData().get_raw_json(
        'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/' + symbol,
        params={'symbol': symbol, 'type': ','.join(frequency + key for key in CONCEPTS),
                'period1': int((now - timedelta(days=6 * 366)).timestamp()),
                'period2': int((now + timedelta(days=1)).timestamp())}, timeout=15,
    )


def fetch_financials(*, market, symbol, exchange, request=None, today=None):
    if not isinstance(market, str) or market not in {'US', 'UK'} or not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,49}', symbol):
        raise FinancialUnavailable('unsupported_identity')
    frequency = 'quarterly' if market == 'US' else 'annual'
    try:
        data = (request or _request)(symbol, frequency)
    except Exception as exc:
        raise FinancialRetryable('provider_unavailable') from exc
    return normalize_financials(data, market=market, symbol=symbol, exchange=exchange, today=today)


def normalize_financials(data, *, market, symbol, exchange, today=None):
    today = today or date.today()
    frequency = 'quarterly' if market == 'US' else 'annual'
    expected_period = '3M' if market == 'US' else '12M'
    try:
        root = data['timeseries']
        series = root['result']
        if root.get('error') or not isinstance(series, list) or not 1 <= len(series) <= len(CONCEPTS):
            raise ValueError('invalid_series')
        points, currencies, seen = defaultdict(dict), defaultdict(set), set()
        for item in series:
            types = item['meta']['type']
            if len(types) != 1 or types[0] in seen or item['meta'].get('symbol') != [symbol]:
                raise ValueError('invalid_identity')
            key = types[0]
            seen.add(key)
            if not key.startswith(frequency) or key[len(frequency):] not in CONCEPTS:
                raise ValueError('unexpected_concept')
            field = CONCEPTS[key[len(frequency):]]
            values = item.get(key, [])
            if not isinstance(values, list) or len(values) > 12:
                raise ValueError('unbounded_periods')
            for point in values:
                when = date.fromisoformat(point['asOfDate'])
                if point.get('periodType') != expected_period or when > today:
                    continue
                if field in points[when]:
                    raise ValueError('duplicate_point')
                raw = point.get('reportedValue', {}).get('raw')
                value = None if isinstance(raw, bool) else _finite_or_none(raw)
                points[when][field] = value
                if field != 'shares_outstanding' and value is not None:
                    currency = point.get('currencyCode')
                    currencies[when].add(currency if isinstance(currency, str) and len(currency) <= 12 else 'unknown')
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise FinancialUnavailable('invalid_financial_response') from exc
    observations = []
    for when, values in sorted(points.items()):
        currency_set = currencies[when]
        currency = next(iter(currency_set)) if len(currency_set) == 1 and currency_set.issubset(CURRENCIES) else ''
        row = {field: values.get(field) for field in FUNDAMENTAL_FIELDS}
        if not currency:
            for field in CORE_FIELDS:
                if field != 'shares_outstanding':
                    row[field] = None
        shares, equity, debt = row['shares_outstanding'], row['shareholder_equity'], row['total_debt']
        if shares is not None and shares <= 0:
            row['shares_outstanding'] = shares = None
        if currency and equity is not None:
            row['book_value'] = _finite_or_none(equity / shares) if shares else None
            row['debt_to_equity'] = _finite_or_none(debt / equity) if debt is not None and equity > 0 else None
        if not any(row[field] is not None for field in CORE_FIELDS):
            continue
        fingerprint = hashlib.sha256(json.dumps([exchange, when.isoformat(), frequency, currency, row], sort_keys=True).encode()).hexdigest()
        observations.append({**row, 'market': 'USStock' if market == 'US' else 'UKStock', 'symbol': symbol,
            'period_end': when, 'available_at': today, 'frequency': frequency, 'currency': currency,
            'source': f'research_yahoo_{frequency}:{fingerprint[:16]}', 'source_version': fingerprint,
            'metadata': {'providerSymbol': symbol, 'exchange': exchange, 'financialCurrency': currency or None,
                         'reportedCurrencies': sorted(currency_set), 'quoteCurrency': None,
                         'pointInTime': False, 'availabilitySource': 'first_observed',
                         'historicalComparability': 'unverified', 'interimCoverage': 'unavailable' if market == 'UK' else 'not_applicable',
                         'derivations': {'book_value': 'equity / reported shares', 'debt_to_equity': 'debt / equity'},
                         'observedAt': datetime.now(timezone.utc).isoformat()}})
    if not observations:
        raise FinancialUnavailable('no_financial_data')
    return observations
