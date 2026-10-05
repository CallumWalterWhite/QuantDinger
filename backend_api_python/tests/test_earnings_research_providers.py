from datetime import datetime, timezone
import pytest
from app.data_providers.earnings_evidence import normalize_prices, normalize_news


def metadata():
    return {'symbol': 'TEST.L', 'exchangeName': 'LSE', 'exchangeTimezoneName': 'Europe/London', 'currency': 'GBp'}


def test_london_forming_session_and_currency():
    bars = [{'session_date': '2026-10-05', 'close': 100, 'open': 99, 'high': 102, 'low': 98, 'volume': 10}]
    now = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
    result = normalize_prices(bars, metadata(), market='UK', symbol='TEST.L', exchange='LSE', now=now)
    assert result['data']['bars'] == []
    result = normalize_prices(bars, metadata(), market='UK', symbol='TEST.L', exchange='LSE', now=now.replace(hour=17))
    assert result['data']['currency'] == 'GBp'
    assert result['data']['bars'][0]['close'] == 100
    assert result['data']['adjustment'] == 'split_dividend_adjusted'


def test_identity_and_nonfinite_fail_closed():
    with pytest.raises(ValueError, match='unverified_price_identity'):
        normalize_prices([], {**metadata(), 'symbol': 'OTHER.L'}, market='UK', symbol='TEST.L', exchange='LSE')
    result = normalize_prices([{'session_date': '2026-09-30', 'close': float('nan')}], metadata(), market='UK', symbol='TEST.L', exchange='LSE')
    assert not result['data']['bars']


@pytest.mark.parametrize('market,day,close_hour,close_minute', [
    ('UK', '2026-03-27', 16, 30), ('UK', '2026-03-30', 15, 30),
    ('US', '2026-03-06', 21, 0), ('US', '2026-03-09', 20, 0),
])
def test_completed_session_boundary_tracks_exchange_dst(market, day, close_hour, close_minute):
    from datetime import timedelta
    symbol, exchange = ('TEST.L', 'LSE') if market == 'UK' else ('TEST', 'NASDAQ')
    meta = {'symbol': symbol, 'exchangeName': exchange,
            'exchangeTimezoneName': 'Europe/London' if market == 'UK' else 'America/New_York',
            'currency': 'GBp' if market == 'UK' else 'USD'}
    bar = {'session_date': day, 'open': 99, 'high': 102, 'low': 98, 'close': 100, 'volume': 10}
    end = datetime.fromisoformat(day).replace(hour=close_hour, minute=close_minute, tzinfo=timezone.utc)
    before = normalize_prices([bar], meta, market=market, symbol=symbol, exchange=exchange, now=end - timedelta(seconds=1))
    after = normalize_prices([bar], meta, market=market, symbol=symbol, exchange=exchange, now=end)
    assert not before['data']['bars'] and len(after['data']['bars']) == 1


def test_provider_verified_half_day_close_is_used():
    meta = {'symbol': 'TEST', 'exchangeName': 'NMS', 'exchangeTimezoneName': 'America/New_York',
            'currency': 'USD', 'currentTradingPeriod': {'regular': {'end': 1795802400}}}
    # 2026-11-27 18:00 UTC, the provider-declared 13:00 New York close.
    end = datetime.fromtimestamp(meta['currentTradingPeriod']['regular']['end'], timezone.utc)
    bar = {'session_date': end.date().isoformat(), 'open': 99, 'high': 102, 'low': 98, 'close': 100, 'volume': 10}
    result = normalize_prices([bar], meta, market='US', symbol='TEST', exchange='NASDAQ', now=end)
    assert len(result['data']['bars']) == 1


@pytest.mark.parametrize('exchange', ['NMS','NGM','NCM','NYQ','ASE','PCX','BTS'])
def test_real_directory_exchange_codes(exchange):
    from app.data_providers.earnings_evidence import exchange_aliases
    meta = {'symbol': 'TEST', 'exchangeName': exchange, 'exchangeTimezoneName': 'America/New_York', 'currency': 'USD'}
    result = normalize_prices([], meta, market='US', symbol='TEST', exchange=exchange)
    assert result['status'] == 'unavailable'
    assert exchange_aliases(exchange)


def test_news_bounds_dates_and_plain_text():
    rss = b'<rss><channel><item><title>Story</title><description>&lt;script&gt;bad&lt;/script&gt;Company report</description><link>https://example.com/story</link><pubDate>Mon, 05 Oct 2026 10:00:00 GMT</pubDate></item></channel></rss>'
    result = normalize_news(rss, now=datetime(2026, 10, 5, 11, tzinfo=timezone.utc))
    assert result['data']['items'][0]['title'] == 'Story'
    assert result['data']['items'][0]['published_at'].startswith('2026-10-05')
    assert 'script' not in result['data']['items'][0]['summary']
    with pytest.raises(ValueError):
        normalize_news(b'<!DOCTYPE rss><rss/>')


def test_chart_adjustment_policy_is_explicit():
    from app.data_providers.earnings_evidence import normalize_chart
    chart = {'chart': {'result': [{'meta': metadata(), 'timestamp': [1759222800], 'indicators': {
        'quote': [{'open': [9], 'high': [11], 'low': [8], 'close': [10], 'volume': [100]}],
        'adjclose': [{'adjclose': [5]}]}}]}}
    result = normalize_chart(chart, {'market':'UK','provider_symbol':'TEST.L','exchange':'LSE'})
    assert result['data']['bars'][0]['close'] == 5
    assert result['data']['bars'][0]['high'] == 5.5
    assert result['data']['bars'][0]['volume'] == 100


def test_every_raw_source_has_a_decoded_body_limit(monkeypatch):
    from app.data_providers.earnings_evidence import _public_bytes
    from tests.test_research_web import Response, fixture_pools
    response = Response(b'x' * 5_000_001)
    pools = fixture_pools(monkeypatch, [response])
    with pytest.raises(ValueError, match='source_too_large'):
        _public_bytes('https://example.com/large', 'Owner')
    assert response.closed and pools[0].closed


def test_raw_source_redirect_rejects_private_destination(monkeypatch):
    from app.data_providers.earnings_evidence import _public_bytes
    from app.services import research_web as web
    from tests.test_research_web import Response, fixture_pools
    fixture_pools(monkeypatch, [Response(status=302, headers={'Location':'https://private.example/'})])
    monkeypatch.setattr(web.socket, 'getaddrinfo', lambda host, *a, **k: [(None,None,None,None,('127.0.0.1' if host == 'private.example' else '93.184.216.34',443))])
    with pytest.raises(ValueError, match='public addresses'):
        _public_bytes('https://example.com/', 'Owner')


def test_provider_deadline_kills_and_reaps_real_child(monkeypatch):
    import subprocess
    import sys
    from app.data_providers import earnings_evidence as adapter
    original = subprocess.Popen
    children = []
    def launch(command, **kwargs):
        child = original([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(adapter.subprocess, 'Popen', launch)
    result = adapter.fetch_evidence({'kind':'prices'}, timeout=.2)
    assert result['status'] == 'retry'
    assert all(child.poll() is not None for child in children)


def test_verified_uk_pdf_stays_linked_with_an_extraction_gap(monkeypatch):
    from app.services import research_web
    from app.data_providers.earnings_evidence import _documents
    def unsupported(*args, **kwargs):
        raise ValueError('Unsupported document content type')
    monkeypatch.setattr(research_web, 'read_public_document', unsupported)
    result = _documents({'market': 'UK', 'issuer_url': 'https://example.com/annual.pdf'})
    assert result['status'] == 'unavailable'
    assert result['data']['documents'][0]['url'] == 'https://example.com/annual.pdf'
    assert 'pdf_or_binary_document_not_extracted' in result['gaps']


def test_uk_search_leads_are_never_automatically_read(monkeypatch):
    from app.services import research_web
    from app.data_providers import earnings_evidence as adapter
    monkeypatch.setattr(adapter, '_discovery_leads', lambda _: [{'url': 'https://example.com/ir', 'title': 'Possible source'}] * 20)
    monkeypatch.setattr(research_web, 'read_public_document', lambda *a, **k: pytest.fail('unverified leads must not be fetched'))
    result = adapter._documents({'market': 'UK', 'name': 'Fixture Company'})
    assert len(result['data']['leads']) == 5 and result['status'] == 'unavailable'
    assert 'unverified_issuer_url' in result['gaps']
