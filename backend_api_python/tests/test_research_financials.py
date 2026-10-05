from datetime import date

import pytest

from app.data_providers.research_financials import (
    FinancialUnavailable, FinancialRetryable, fetch_financials, normalize_financials,
)


def payload(frequency='annual', currency='GBP', period='12M', symbol='TEST.L'):
    concepts = {'TotalRevenue': 100, 'NetIncome': 10, 'StockholdersEquity': 50,
                'TotalDebt': 5, 'FreeCashFlow': 8, 'OrdinarySharesNumber': 20}
    return {'timeseries': {'error': None, 'result': [
        {'meta': {'type': [frequency + key], 'symbol': [symbol]},
         frequency + key: [{'asOfDate': '2026-03-31', 'periodType': period,
                           'currencyCode': currency, 'reportedValue': {'raw': value}}]}
        for key, value in concepts.items()]}}


def test_uk_annual_financial_currency_and_first_observed():
    rows = normalize_financials(payload(), market='UK', symbol='TEST.L', exchange='LSE', today=date(2026, 10, 5))
    row = rows[0]
    assert row['currency'] == 'GBP' and row['frequency'] == 'annual'
    assert row['available_at'] == date(2026, 10, 5)
    assert row['revenue'] == 100 and row['book_value'] == 2.5
    assert row['market_cap'] is None and row['pe_ratio'] is None
    assert row['metadata']['pointInTime'] is False
    assert row['metadata']['interimCoverage'] == 'unavailable'
    assert row['metadata']['quoteCurrency'] is None


def test_us_quarters_not_annualized_or_ttm_guessed():
    row = normalize_financials(payload('quarterly', 'USD', '3M', 'TEST'),
                              market='US', symbol='TEST', exchange='NASDAQ')[0]
    assert row['frequency'] == 'quarterly' and row['net_income'] == 10
    assert row['net_income_ttm'] is None and row['return_on_equity'] is None


@pytest.mark.parametrize('currency', ['', None, 'GBp', 'unknown'])
def test_unknown_or_quote_pence_currency_withholds_monetary_values(currency):
    row = normalize_financials(payload(currency=currency), market='UK', symbol='TEST.L', exchange='LSE')[0]
    assert row['revenue'] is None and row['book_value'] is None
    assert row['shares_outstanding'] == 20
    if currency == 'GBp':
        assert row['metadata']['reportedCurrencies'] == ['GBp']


def test_mixed_financial_units_fail_closed():
    data = payload()
    data['timeseries']['result'][1]['annualNetIncome'][0]['currencyCode'] = 'EUR'
    row = normalize_financials(data, market='UK', symbol='TEST.L', exchange='LSE')[0]
    assert row['revenue'] is None and row['net_income'] is None


def test_missing_and_boolean_values_remain_missing():
    data = payload()
    data['timeseries']['result'][0]['annualTotalRevenue'][0]['reportedValue']['raw'] = True
    data['timeseries']['result'].pop(1)
    row = normalize_financials(data, market='UK', symbol='TEST.L', exchange='LSE')[0]
    assert row['revenue'] is None and row['net_income'] is None


@pytest.mark.parametrize('change', ['period', 'symbol', 'empty', 'error', 'duplicate', 'future'])
def test_malformed_or_unavailable_contract(change):
    data = payload()
    series = data['timeseries']['result'][0]
    if change == 'period':
        for item in data['timeseries']['result']:
            item[item['meta']['type'][0]][0]['periodType'] = '3M'
    elif change == 'symbol':
        series['meta']['symbol'] = ['OTHER.L']
    elif change == 'empty':
        data['timeseries']['result'] = []
    elif change == 'error':
        data['timeseries']['error'] = {'description': 'unavailable'}
    elif change == 'duplicate':
        data['timeseries']['result'].append(series)
    elif change == 'future':
        for item in data['timeseries']['result']:
            item[item['meta']['type'][0]][0]['asOfDate'] = '2099-01-01'
    with pytest.raises(FinancialUnavailable):
        normalize_financials(data, market='UK', symbol='TEST.L', exchange='LSE')


def test_fetch_uses_one_injected_request_and_sanitizes_failure():
    calls = []
    def request(symbol, frequency):
        calls.append((symbol, frequency))
        return payload()
    assert fetch_financials(market='UK', symbol='TEST.L', exchange='LSE', request=request)
    assert calls == [('TEST.L', 'annual')]
    def fail(*args):
        raise TimeoutError('secret-bearing-provider-url')
    with pytest.raises(FinancialRetryable, match='provider_unavailable') as error:
        fetch_financials(market='UK', symbol='TEST.L', exchange='LSE', request=fail)
    assert 'secret' not in str(error.value)
