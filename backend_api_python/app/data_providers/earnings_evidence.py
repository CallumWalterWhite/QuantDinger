"""Research-only, bounded, free-source adapters. Never use a tradable registry."""
from datetime import date, datetime, time, timezone, timedelta
from email.utils import parsedate_to_datetime
import json
import math
import os
import re
import signal
import subprocess
import sys
from contextlib import redirect_stdout
from time import monotonic, sleep
from urllib.parse import urlencode, urlsplit, parse_qs
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

EXCHANGES = {'LSE': {'LSE', 'LONDON'}, 'NASDAQ': {'NMS', 'NGM', 'NCM', 'NASDAQ'},
             'NYSE': {'NYQ', 'NYSE'}, 'AMEX': {'ASE', 'AMEX', 'NYSEAMERICAN'},
             'ARCA': {'PCX', 'ARCA', 'NYSEARCA'}, 'BATS': {'BTS', 'BATS'}}


def exchange_aliases(exchange):
    return next((aliases for key, aliases in EXCHANGES.items() if exchange == key or exchange in aliases), set())


def normalize_prices(rows, metadata, *, market, symbol, exchange, now=None):
    now = now or datetime.now(timezone.utc)
    tzname = 'Europe/London' if market == 'UK' else 'America/New_York'
    provider_exchange = re.sub(r'[^A-Z]', '', str(metadata.get('exchangeName', '')).upper())
    if metadata.get('symbol', '').upper() != symbol.upper() or provider_exchange not in exchange_aliases(exchange) or metadata.get('exchangeTimezoneName') != tzname:
        raise ValueError('unverified_price_identity')
    currency = metadata.get('currency')
    if currency not in {'USD', 'GBP', 'GBp', 'EUR', 'CAD', 'CHF'}:
        raise ValueError('unverified_price_currency')
    zone = ZoneInfo(tzname)
    local_now = now.astimezone(zone)
    close = time(16, 30) if market == 'UK' else time(16)
    regular = (metadata.get('currentTradingPeriod') or {}).get('regular') or {}
    close_at = datetime.combine(local_now.date(), close, zone)
    if isinstance(regular.get('end'), int):
        known_close = datetime.fromtimestamp(regular['end'], timezone.utc).astimezone(zone)
        if known_close.date() == local_now.date():
            close_at = known_close
    bars = []
    for row in rows[-600:]:
        try:
            session = date.fromisoformat(row['session_date'])
            if session > local_now.date() or session.weekday() > 4 or (session == local_now.date() and local_now < close_at):
                continue
            values = {key: float(row[key]) for key in ('open', 'high', 'low', 'close', 'volume')}
            if any(not math.isfinite(value) for value in values.values()) or min(values[k] for k in ('open','high','low','close')) <= 0 or values['volume'] < 0:
                continue
            if not values['low'] <= min(values['open'], values['close']) <= max(values['open'], values['close']) <= values['high']:
                continue
            bars.append({'session_date': session.isoformat(), **values,
                         'dividend': float(row.get('dividend') or 0), 'split': float(row.get('split') or 0)})
        except (ValueError, KeyError, TypeError):
            continue
    bars = sorted({row['session_date']: row for row in bars}.values(), key=lambda row: row['session_date'])
    gaps = [] if len(bars) >= 61 else ['insufficient_price_history']
    state = 'ready' if not gaps else 'partial' if bars else 'unavailable'
    if bars and (local_now.date() - date.fromisoformat(bars[-1]['session_date'])).days > 7:
        state = 'partial'
        gaps.append('stale_price_sessions')
    return {'status': state, 'provider': 'yahoo', 'url': 'https://finance.yahoo.com/quote/' + symbol,
            'data': {'bars': bars, 'currency': currency, 'exchange_timezone': tzname,
                     'adjustment': 'split_dividend_adjusted', 'session_policy': 'provider_regular_close_or_conservative_standard_close',
                     'actions_requested': True, 'latest_session': bars[-1]['session_date'] if bars else None}, 'gaps': gaps}


def normalize_news(body, *, now=None):
    now = now or datetime.now(timezone.utc)
    if len(body) > 5_000_000 or b'<!DOCTYPE' in body.upper() or b'<!ENTITY' in body.upper():
        raise ValueError('invalid_news_response')
    root = ET.fromstring(body)
    rows, seen = [], set()
    for item in root.findall('.//item'):
        link = (item.findtext('link') or '').strip()
        parsed = urlsplit(link)
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or link in seen:
            continue
        try:
            when = parsedate_to_datetime(item.findtext('pubDate') or '')
            if when.tzinfo is None or when > now or when < now - timedelta(days=30):
                continue
        except (ValueError, TypeError):
            continue
        soup = BeautifulSoup(item.findtext('description') or '', 'html.parser')
        for node in soup(['script', 'style']):
            node.decompose()
        title = (item.findtext('title') or '').strip()[:300]
        if not title:
            continue
        rows.append({'title': title, 'summary': soup.get_text(' ', strip=True)[:600],
                     'url': link[:8192], 'published_at': when.isoformat(), 'kind': 'news_metadata_not_verified_company_fact'})
        seen.add(link)
        if len(rows) == 20:
            break
    return {'status': 'ready' if rows else 'unavailable', 'provider': 'yahoo_rss',
            'data': {'items': rows}, 'gaps': [] if rows else ['news_unavailable']}


def normalize_chart(payload, item):
    """Apply the explicit adjclose/close factor to OHLC, retaining action data."""
    chart = payload.get('chart') or {}
    result = chart.get('result') or []
    if chart.get('error') or len(result) != 1:
        raise ValueError('invalid_price_history')
    root = result[0]
    metadata = root.get('meta') or {}
    stamps = root.get('timestamp') or []
    if len(stamps) > 600:
        raise ValueError('invalid_price_history')
    quote = root['indicators']['quote'][0]
    adjusted = root['indicators']['adjclose'][0]['adjclose']
    if len(adjusted) != len(stamps) or any(len(quote.get(key, [])) != len(stamps) for key in ('open','high','low','close','volume')):
        raise ValueError('invalid_price_history')
    zone = ZoneInfo(metadata['exchangeTimezoneName'])
    events = root.get('events') or {}
    rows = []
    for index, stamp in enumerate(stamps):
        try:
            factor = float(adjusted[index]) / float(quote['close'][index])
            if not math.isfinite(factor) or factor <= 0:
                continue
            session = datetime.fromtimestamp(stamp, zone).date().isoformat()
            dividend = (events.get('dividends') or {}).get(str(stamp), {}).get('amount', 0)
            split = (events.get('splits') or {}).get(str(stamp), {})
            ratio = float(split['numerator']) / float(split['denominator']) if split else 0
            row = {key: float(quote[key][index])*factor for key in ('open','high','low','close')}
            rows.append({**row, 'volume': quote['volume'][index], 'session_date': session, 'dividend': dividend, 'split': ratio})
        except (ValueError, TypeError, ZeroDivisionError):
            continue
    return normalize_prices(rows, metadata, market=item['market'], symbol=item['provider_symbol'], exchange=item['exchange'])


def _discovery_leads(name):
    url = 'https://html.duckduckgo.com/html/?' + urlencode({'q': name + ' investor relations annual results'})
    soup = BeautifulSoup(_public_bytes(url, 'QuantDinger Research'), 'html.parser')
    leads = []
    for anchor in soup.select('a.result__a')[:5]:
        target = anchor.get('href', '')
        parsed = urlsplit(target)
        if parsed.hostname and parsed.hostname.endswith('duckduckgo.com'):
            target = parse_qs(parsed.query).get('uddg', [''])[0]
        parsed = urlsplit(target)
        if parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.password:
            leads.append({'title': anchor.get_text(' ', strip=True)[:300], 'url': target[:8192]})
    return leads


def _public_bytes(url, user_agent, timeout=8):
    """Fixed-size raw fetch with the same public-DNS pinning as document reads."""
    import certifi
    import urllib3
    from app.services.research_web import public_target
    deadline = monotonic() + timeout
    for _ in range(3):
        host, address = public_target(url)
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError()
        parsed = urlsplit(url)
        pool = urllib3.HTTPSConnectionPool(address, port=443, server_hostname=host, assert_hostname=host,
                                         cert_reqs='CERT_REQUIRED', ca_certs=certifi.where(),
                                         timeout=urllib3.Timeout(connect=min(3, remaining), read=remaining))
        response = None
        try:
            response = pool.request('GET', (parsed.path or '/') + ('?' + parsed.query if parsed.query else ''),
                                    headers={'Host': host, 'User-Agent': user_agent}, redirect=False,
                                    retries=False, preload_content=False)
            if response.status in (301, 302, 303, 307, 308):
                from urllib.parse import urljoin
                url = urljoin(url, response.headers.get('Location', ''))
                continue
            if response.status in (429, 500, 502, 503, 504):
                raise TimeoutError()
            if response.status != 200:
                raise ValueError('source_unavailable')
            chunks, size = [], 0
            while True:
                if monotonic() > deadline:
                    raise TimeoutError()
                part = response.read(min(65536, 5_000_001-size), decode_content=True)
                if not part:
                    break
                size += len(part)
                if size > 5_000_000:
                    raise ValueError('source_too_large')
                chunks.append(part)
            return b''.join(chunks)
        finally:
            if response is not None:
                response.close()
            pool.close()
    raise ValueError('redirect_limit')


def _company_name(name):
    parts = re.findall(r'[a-z0-9]+', name.lower())
    return ' '.join(part for part in parts if part not in {'inc','incorporated','corp','corporation','ltd','limited','plc','co','company'})


def _documents(item):
    from app.services.research_web import read_public_document
    ua = os.getenv('SEC_EDGAR_USER_AGENT', '').strip()
    if item['market'] == 'UK':
        url = item.get('issuer_url')
        if not url:
            # One free discovery query; results stay unverified leads.
            try:
                leads = _discovery_leads(item['name'])
            except Exception:
                leads = []
            bounded = [{'title': str(row.get('title', ''))[:300], 'url': str(row.get('url', ''))[:8192]} for row in leads[:5]]
            return {'status': 'unavailable', 'provider': 'issuer_ir', 'data': {'leads': bounded}, 'gaps': ['unverified_issuer_url', 'uk_pdf_and_interim_not_extracted']}
        try:
            document = read_public_document(url, timeout=8)
        except ValueError as exc:
            if str(exc) != 'Unsupported document content type':
                raise
            return {'status': 'unavailable', 'provider': 'issuer_ir', 'url': url,
                    'data': {'documents': [{'url': url, 'evidence_kind': 'metadata_only'}],
                             'identity_basis': 'admin_verified_issuer_url'},
                    'gaps': ['pdf_or_binary_document_not_extracted', 'uk_interim_unavailable']}
        return {'status': 'ready', 'provider': 'issuer_ir', 'url': url,
                'data': {'documents': [document], 'identity_basis': 'admin_verified_issuer_url'},
                'gaps': ['uk_pdf_and_interim_not_extracted']}
    if not ua or '@' not in ua or 'support@quantdinger.com' in ua:
        return {'status': 'unavailable', 'provider': 'sec', 'data': {}, 'gaps': ['owner_sec_contact_required']}
    if len(ua) > 512 or any(ord(ch) < 32 for ch in ua):
        raise ValueError('invalid_sec_contact')
    # A single child executes SEC requests serially with a one-second minimum.
    last_request = 0.0
    def sec_json(url):
        nonlocal last_request
        sleep(max(0, last_request + 1 - monotonic()))
        last_request = monotonic()
        return json.loads(_public_bytes(url, ua))
    mapping = sec_json('https://www.sec.gov/files/company_tickers_exchange.json')
    fields = mapping.get('fields', [])
    companies = [dict(zip(fields, row)) for row in mapping.get('data', [])]
    names = exchange_aliases(item['exchange'])
    matches = [row for row in companies if str(row.get('ticker', '')).upper() == item['symbol'].upper()
               and re.sub(r'[^A-Z]', '', str(row.get('exchange', '')).upper()) in names
               and _company_name(str(row.get('name', ''))) == _company_name(item['name'])]
    if len(matches) != 1:
        return {'status': 'unsupported', 'provider': 'sec', 'data': {}, 'gaps': ['unverified_sec_identity']}
    cik = int(matches[0]['cik'])
    payload = sec_json(f'https://data.sec.gov/submissions/CIK{cik:010d}.json')
    if int(payload.get('cik', 0)) != cik or _company_name(payload.get('name', '')) != _company_name(item['name']):
        raise ValueError('unverified_sec_identity')
    recent = (payload.get('filings') or {}).get('recent') or {}
    docs, seen = [], set()
    for index, form in enumerate(recent.get('form', [])):
        if form not in {'8-K', '10-Q', '10-K', '20-F'} or form in seen:
            continue
        filed = date.fromisoformat(recent['filingDate'][index])
        if filed > datetime.now(timezone.utc).date():
            continue
        accession = recent['accessionNumber'][index].replace('-', '')
        filename = recent['primaryDocument'][index]
        if not re.fullmatch(r'\d+', accession) or not re.fullmatch(r'[A-Za-z0-9_.-]+', filename):
            continue
        url = f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}'
        sleep(max(0, last_request + 1 - monotonic()))
        last_request = monotonic()
        try:
            doc = read_public_document(url, timeout=8, user_agent=ua)
            docs.append({**doc, 'form': form, 'published_at': filed.isoformat()})
        except ValueError:
            docs.append({'url': url, 'form': form, 'published_at': filed.isoformat(), 'gap': 'document_not_extracted'})
        seen.add(form)
        if len(docs) == 3:
            break
    return {'status': 'ready' if any(row.get('text') for row in docs) else 'unavailable',
            'provider': 'sec', 'url': f'https://data.sec.gov/submissions/CIK{cik:010d}.json',
            'data': {'documents': docs, 'cik': cik, 'identity_basis': 'ticker_exchange_name_verified'},
            'gaps': ['bounded_primary_excerpts_not_complete_filings']}


def _fetch(item):
    if item['kind'] == 'documents':
        return _documents(item)
    if item['kind'] == 'news':
        url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?' + urlencode({'s': item['provider_symbol'], 'region': 'GB' if item['market'] == 'UK' else 'US', 'lang': 'en-GB' if item['market'] == 'UK' else 'en-US'})
        result = normalize_news(_public_bytes(url, 'QuantDinger Research'))
        return {**result, 'url': url}
    from urllib.parse import quote
    symbol = quote(item['provider_symbol'], safe='')
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + symbol + '?' + urlencode({
        'range': '2y', 'interval': '1d', 'events': 'div,splits', 'includeAdjustedClose': 'true', 'includePrePost': 'false'})
    return normalize_chart(json.loads(_public_bytes(url, 'QuantDinger Research')), item)


def fetch_evidence(item, timeout=20):
    """One killable operation, including library initialization and DNS."""
    deadline = monotonic() + timeout
    child = subprocess.Popen([sys.executable, '-m', 'app.data_providers.earnings_evidence', '--child'],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, start_new_session=True)
    request = {key: item.get(key) for key in ('kind','market','exchange','symbol','provider_symbol','name','issuer_url')}
    request['parent_pid'] = os.getpid()
    try:
        output, _ = child.communicate(json.dumps(request), timeout=max(.01, deadline-monotonic()))
        if child.returncode or len(output.encode()) > 280000:
            raise ValueError('invalid_provider_result')
        return json.loads(output)
    except subprocess.TimeoutExpired:
        return {'status': 'retry', 'provider': 'bounded_adapter', 'data': {}, 'gaps': ['provider_deadline']}
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        for stream in (child.stdin, child.stdout):
            if stream:
                stream.close()


def _child():
    # Linux parent-death protection prevents an orphan retaining a provider slot
    # when a Celery worker is killed, even before the database lease expires.
    if sys.platform == 'linux':
        import ctypes
        parent = os.getppid()
        ctypes.CDLL(None).prctl(1, signal.SIGKILL)
        if os.getppid() != parent:
            os._exit(1)
    try:
        item = json.loads(sys.stdin.read(32768))
        if sys.platform == 'linux' and os.getppid() != item.get('parent_pid'):
            os._exit(1)
        with open(os.devnull, 'w') as sink, redirect_stdout(sink):
            result = _fetch(item)
        encoded = json.dumps(result, allow_nan=False)
        if len(encoded.encode()) > 280000:
            raise ValueError('provider_payload_too_large')
    except ValueError:
        encoded = json.dumps({'status': 'unavailable', 'provider': 'bounded_adapter', 'data': {}, 'gaps': ['provider_data_unavailable_or_unverified']})
    except Exception:
        encoded = json.dumps({'status': 'retry', 'provider': 'bounded_adapter', 'data': {}, 'gaps': ['provider_unavailable']})
    sys.stdout.write(encoded)


if __name__ == '__main__' and sys.argv[1:] == ['--child']:
    _child()
