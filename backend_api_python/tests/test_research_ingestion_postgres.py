"""Real PostgreSQL proof, isolated in an owned temporary schema."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import os
from pathlib import Path
from uuid import uuid4
from threading import Event

import pytest

from app.services import research_ingestion as service
from app.services.fundamental_data import FUNDAMENTAL_FIELDS
from app.utils.db_postgres import PostgresCursor
from app.data_providers.research_financials import FinancialRetryable, normalize_financials
from tests.test_research_financials import payload as financial_payload


def payload(*args, **kwargs):
    value = financial_payload(*args, **kwargs)
    for series in value['timeseries']['result']:
        series[series['meta']['type'][0]][0]['asOfDate'] = (date.today() - timedelta(days=60)).isoformat()
    return value


@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv('QD_EVENTS_TEST_DATABASE_URL')
    if not dsn:
        pytest.skip('QD_EVENTS_TEST_DATABASE_URL must identify a disposable database')
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor
    schema = 'research_test_' + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    counts = {'open': 0}
    @contextmanager
    def connect():
        raw = psycopg2.connect(dsn)
        try:
            with raw.cursor() as cur:
                cur.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            raw.commit()
            with raw.cursor() as cur:
                cur.execute('SELECT 1')  # Production pool health probe.
            counts['open'] += 1
            class Connection:
                def cursor(self):
                    return PostgresCursor(raw.cursor(cursor_factory=RealDictCursor))
                def commit(self):
                    raw.commit()
                def rollback(self):
                    raw.rollback()
            yield Connection()
        finally:
            counts['open'] -= 1
            raw.close()
    monkeypatch.setattr(service, 'get_db_connection', connect)
    migration = Path(__file__).resolve().parents[1] / 'migrations'
    with service.transaction() as cur:
        cur.execute('CREATE TABLE qd_users(id BIGINT PRIMARY KEY)')
        cur.execute('INSERT INTO qd_users VALUES (1),(2)')
        cur.execute(f'''CREATE TABLE qd_fundamental_snapshots(id BIGSERIAL PRIMARY KEY,
            market TEXT,symbol TEXT,period_end DATE,available_at DATE,frequency TEXT,currency TEXT,
            {','.join(field + ' DOUBLE PRECISION' for field in FUNDAMENTAL_FIELDS if field != 'net_income_ttm')},
            source TEXT,source_version TEXT,metadata_json JSONB,ingested_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(market,symbol,period_end,available_at,source))''')
        cur.execute((migration / '20261005_market_earnings.sql').read_text())
        cur.execute("INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,source,revenue) VALUES ('USStock','PRESERVE',CURRENT_DATE,CURRENT_DATE,'migration_preservation_fixture',42) RETURNING id")
        preserved_id = cur.fetchone()['id']
        cur.execute((migration / '20261005_research_ingestion.sql').read_text())
        cur.execute((migration / '20261005_research_ingestion.sql').read_text())
        cur.execute('SELECT id,revenue,net_income_ttm FROM qd_fundamental_snapshots WHERE id=?', (preserved_id,))
        assert cur.fetchone() == {'id': preserved_id, 'revenue': 42, 'net_income_ttm': None}
        cur.execute('SELECT COUNT(*) AS n FROM qd_users')
        assert cur.fetchone()['n'] == 2
        cur.execute('DELETE FROM qd_fundamental_snapshots WHERE id=?', (preserved_id,))
        for market in ('US', 'UK'):
            cur.execute("INSERT INTO qd_earnings_sync_runs(market,window_start,window_end,status,finished_at) VALUES (?,CURRENT_DATE,CURRENT_DATE+90,'success',NOW())", (market,))
    try:
        yield counts
    finally:
        with admin.cursor() as cur:
            cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        admin.close()


def listing(symbol='TEST.L', market='UK', exchange='LSE', provider=None):
    return service.query('''INSERT INTO qd_earnings_listings(market,exchange,symbol,provider_symbol,name)
        VALUES (?,?,?,?,?) RETURNING id''', (market, exchange, symbol, provider or symbol, symbol))['id']


def reset_cooldown():
    with service.transaction() as cur:
        cur.execute("UPDATE qd_research_ingestion_schedules SET cooldown_until=NOW()-INTERVAL '1 second'")


def observations(**kwargs):
    return normalize_financials(payload(symbol=kwargs['symbol']), **kwargs)


def test_large_directory_membership_and_unknown_dates(database):
    with service.transaction() as cur:
        cur.execute("INSERT INTO qd_earnings_listings(market,exchange,symbol,provider_symbol,name) SELECT 'US','NASDAQ','T'||n,'T'||n,'Company'||n FROM generate_series(1,6001) n")
    job = service.start_job(1, 'US', 'large')
    assert service.capture_one()
    assert service.query('SELECT expected_count FROM qd_research_ingestion_jobs WHERE id=?', (job['job_id'],))['expected_count'] == 6001
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_items')['n'] == 6001
    assert service.query('SELECT COUNT(*) AS n FROM qd_market_earnings')['n'] == 0
    listing('NEW', 'US', 'NASDAQ')
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_items')['n'] == 6001
    from app.services import research_ingestion_read as reads
    page = reads.listings('US', page=2, page_size=50)
    assert page['total'] == 6002 and len(page['items']) == 50
    assert all(row['event_date'] is None for row in page['items'])


def test_catalog_publication_during_capture_is_snapshot_consistent(database, monkeypatch):
    first = listing('A.L')
    listing('B.L')
    job = service.start_job(1, 'UK', 'concurrent-catalog')['job_id']
    published = service.query("SELECT MAX(id) AS id FROM qd_earnings_sync_runs WHERE market='UK'")['id']
    entered, release = Event(), Event()
    original = service.transaction
    @contextmanager
    def paused(snapshot=False):
        with original(snapshot=snapshot) as cur:
            class Cursor:
                def execute(self, sql, args=()):
                    result = cur.execute(sql, args)
                    if sql.startswith("SELECT id FROM qd_earnings_sync_runs"):
                        entered.set()
                        assert release.wait(5)
                    return result
                def __getattr__(self, name):
                    return getattr(cur, name)
            yield Cursor()
    monkeypatch.setattr(service, 'transaction', paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        capture = pool.submit(service.capture_one)
        assert entered.wait(5)
        with original() as cur:
            cur.execute("UPDATE qd_earnings_listings SET is_active=FALSE WHERE id=?", (first,))
            cur.execute("INSERT INTO qd_earnings_listings(market,exchange,symbol,provider_symbol,name) VALUES ('UK','LSE','NEW.L','NEW.L','New')")
            cur.execute("INSERT INTO qd_earnings_sync_runs(market,window_start,window_end,status,finished_at) VALUES ('UK',CURRENT_DATE,CURRENT_DATE+90,'success',NOW())")
        release.set()
        assert capture.result()
    result = service.query('SELECT expected_count,catalog_run_id FROM qd_research_ingestion_jobs WHERE id=?', (job,))
    assert result == {'expected_count': 2, 'catalog_run_id': published}
    assert [row['symbol'] for row in service.query('SELECT symbol FROM qd_research_ingestion_items ORDER BY symbol', many=True)] == ['A.L', 'B.L']


def test_interrupted_capture_rolls_back_and_resumes(database, monkeypatch):
    listing()
    service.start_job(1, 'UK', 'interrupt')
    original = service.transaction
    @contextmanager
    def interrupted(snapshot=False):
        with original(snapshot=snapshot) as cur:
            yield cur
            raise RuntimeError('simulated worker interruption before commit')
    with monkeypatch.context() as patch:
        patch.setattr(service, 'transaction', interrupted)
        with pytest.raises(RuntimeError):
            service.capture_one()
    assert service.query('SELECT status FROM qd_research_ingestion_jobs')['status'] == 'queued'
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_items')['n'] == 0
    assert service.capture_one()
    assert service.query('SELECT expected_count FROM qd_research_ingestion_jobs')['expected_count'] == 1


def test_request_replay_active_alias_and_concurrent_starts(database):
    listing()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda key: service.start_job(1, 'UK', key), ['first', 'second']))
    assert len({r['job_id'] for r in results}) == 1
    assert service.start_job(1, 'UK', 'second')['job_id'] == results[0]['job_id']
    with pytest.raises(ValueError, match='request_id_conflict'):
        service.start_job(1, 'US', 'second')
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_jobs')['n'] == 1
    with pytest.raises(ValueError, match='job_active'):
        service.start_job(1, 'UK', 'different-policy', False)


def test_paginated_read_models_without_provider(database, monkeypatch):
    from app.services import research_ingestion_read as reads
    for symbol in ('A.L', 'B.L', 'C.L'):
        listing(symbol)
    response = reads.listings('UK', page_size=2)
    assert response['total'] == 3
    assert [row['symbol'] for row in response['items']] == ['A.L', 'B.L']
    assert response['items'][0]['event_date'] is None
    assert response['items'][0]['state'] == 'no_data'
    assert reads.listings('UK', 2, 2)['items'][0]['symbol'] == 'C.L'
    assert reads.listings('UK', query='%')['total'] == 0
    service.start_job(1, 'UK', 'read')
    service.capture_one()
    item = service.claim_one()
    service.publish(item, observations(market='UK', symbol=item['provider_symbol'], exchange='LSE'))
    assert reads.listings('UK', state='ready')['total'] == 1
    job = reads.job_detail(item['job_id'], page_size=1)
    assert job['total'] == 3 and job['counts']['success'] == 1
    assert 'requester_id' not in job['job'] and 'request_keys' not in job['job']
    monkeypatch.setattr(reads, 'list_market_earnings', lambda **kwargs: {'coverage': []})
    uk = reads.overview()['markets'][1]
    assert uk['listing_count'] == 3 and uk['financial_coverage']['ready'] == 1
    assert uk['interim_coverage'] == 'unavailable'


def test_authenticated_admin_api_to_durable_cache(database, client, monkeypatch):
    from app.utils import auth
    from app.services import events_read
    monkeypatch.setattr(auth, 'verify_token', lambda token: {
        'user_id': 1, '_verified_user_role': 'admin', '_verified_username': 'test'})
    monkeypatch.setattr(events_read, 'get_db_connection', service.get_db_connection)
    listing()
    headers = {'Authorization': 'Bearer isolated-admin-test'}
    prefix = '/api/settings/research-ingestion'
    request = {'market': 'UK', 'request_id': 'authenticated', 'requester_id': 2}
    response = client.post(prefix + '/sync', json=request, headers=headers)
    assert response.status_code == 200
    job_id = response.json['data']['job_id']
    assert service.query('SELECT requester_id FROM qd_research_ingestion_jobs')['requester_id'] == 1
    assert client.post(prefix + '/sync', json=request, headers=headers).json['data']['replayed']
    service.run_one(observations)
    service.run_one(observations)
    overview = client.get(prefix + '/overview', headers=headers).json['data']
    assert overview['markets'][1]['financial_coverage']['ready'] == 1
    assert not overview['markets'][1]['schedule']['enabled']
    rows = client.get(prefix + '/listings?market=UK&state=ready', headers=headers).json['data']
    assert rows['total'] == 1 and rows['items'][0]['event_date'] is None
    assert client.get(prefix + f'/jobs/{job_id}', headers=headers).json['data']['counts']['success'] == 1
    assert client.put(prefix + '/schedule', json={'market': 'UK', 'enabled': True}, headers=headers).status_code == 200
    assert client.put(prefix + '/schedule', json={'market': 'UK', 'enabled': False}, headers=headers).status_code == 200


def test_retry_only_failed_active_members_and_durable_replay(database):
    listing('GOOD.L')
    listing('FAIL.L')
    inactive = listing('REMOVED.L')
    job_id = service.start_job(1, 'UK', 'parent')['job_id']
    service.capture_one()
    with service.transaction() as cur:
        cur.execute("UPDATE qd_research_ingestion_items SET status=CASE WHEN symbol='GOOD.L' THEN 'success' ELSE 'failed' END")
        cur.execute("UPDATE qd_research_ingestion_jobs SET status='partial'")
        cur.execute('UPDATE qd_earnings_listings SET is_active=FALSE WHERE id=?', (inactive,))
    retry = service.start_job(1, 'UK', 'retry-key', False, job_id)['job_id']
    assert service.start_job(1, 'UK', 'retry-key', False, job_id)['job_id'] == retry
    service.capture_one()
    assert service.query('SELECT symbol FROM qd_research_ingestion_items WHERE job_id=?', (retry,))['symbol'] == 'FAIL.L'


def test_immutable_restatement_and_legacy_reader_isolation(database, monkeypatch):
    from app.services import fundamental_data as legacy
    listing()
    service.query("INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,source,revenue) VALUES ('UKStock','TEST.L',CURRENT_DATE,CURRENT_DATE,'manual',42) RETURNING id")
    service.start_job(1, 'UK', 'version')
    service.capture_one()
    item = service.claim_one()
    original = observations(market='UK', symbol='TEST.L', exchange='LSE')
    assert service.publish(item, original)
    reset_cooldown()
    service.start_job(1, 'UK', 'revision', False)
    service.capture_one()
    item = service.claim_one()
    changed = payload(symbol='TEST.L')
    changed['timeseries']['result'][0]['annualTotalRevenue'][0]['reportedValue']['raw'] = 999
    revision = normalize_financials(changed, market='UK', symbol='TEST.L', exchange='LSE')
    assert service.publish(item, revision)
    assert service.query('SELECT COUNT(*) AS n FROM qd_fundamental_snapshots')['n'] == 3
    assert service.query('SELECT revenue FROM qd_fundamental_snapshots WHERE source_version=?', (original[0]['source_version'],))['revenue'] == original[0]['revenue']
    monkeypatch.setattr(legacy, 'get_db_connection', service.get_db_connection)
    monkeypatch.setattr(legacy.FundamentalDataService, 'ensure_schema', staticmethod(lambda: None))
    assert [row['revenue'] for row in legacy.FundamentalDataService._load_rows('UKStock', 'TEST.L', date.today())] == [42]


def test_legacy_ai_and_private_coverage_do_not_consume_research_rows(database, monkeypatch):
    from app.services import fundamental_data as legacy, fundamental_coverage
    listing('TEST', 'US', 'NASDAQ')
    service.query("INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,source,revenue) VALUES ('USStock','TEST',CURRENT_DATE,CURRENT_DATE,'manual',42) RETURNING id")
    service.start_job(1, 'US', 'research-us')
    service.capture_one()
    item = service.claim_one()
    rows = normalize_financials(payload('quarterly', 'USD', '3M', 'TEST'), market='US', symbol='TEST', exchange='NASDAQ')
    service.publish(item, rows)
    monkeypatch.setattr(legacy, 'get_db_connection', service.get_db_connection)
    monkeypatch.setattr(legacy.FundamentalDataService, 'ensure_schema', staticmethod(lambda: None))
    monkeypatch.setattr(fundamental_coverage, 'query', service.query)
    assert legacy.FundamentalDataService().latest_for_analysis(market='USStock', symbol='TEST')['payload']['revenue'] == 42
    coverage = fundamental_coverage.member_coverage([{'market': 'USStock', 'symbol': 'TEST'}], ['revenue'], date.today())
    assert coverage[0]['source'] == 'manual'


@pytest.mark.skipif(os.getenv('QD_RESEARCH_PROVIDER_PILOT') != '1', reason='Read-only live provider pilot requires explicit opt-in')
def test_representative_provider_to_cache_pilot(database):
    import resource
    import time
    import yfinance
    from app.data_providers.research_financials import fetch_financials
    from app.services.research_ingestion_read import listings
    for symbol, market, exchange in [('TSLA','US','NASDAQ'), ('TSCO.L','UK','LSE'), ('VOD.L','UK','LSE'), ('YOU.L','UK','LSE')]:
        listing(symbol, market, exchange)
    service.start_job(1, 'US', 'pilot-us')
    service.start_job(1, 'UK', 'pilot-uk')
    service.capture_one()
    service.capture_one()
    baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    timings = []
    def measured(**kwargs):
        assert database['open'] == 0
        start = time.monotonic()
        result = fetch_financials(**kwargs)
        timings.append(round(time.monotonic() - start, 3))
        return result
    for _ in range(4):
        reset_cooldown()  # Pilot excludes deliberate pacing; include it in projections.
        assert service.run_one(measured)
    rows = listings('US')['items'] + listings('UK')['items']
    assert len(rows) == 4 and all(row['snapshot_id'] for row in rows)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert peak - baseline < 1024 * 1024
    print({'yfinance': yfinance.__version__, 'company_seconds': timings,
           'peak_rss_mib': round(peak / 1024, 1), 'additional_peak_mib': round((peak-baseline)/1024, 1),
           'timeseries_calls': 4, 'rows': [(row['symbol'],row['currency'],row['state'],row['period_end']) for row in rows]})


def test_provider_io_without_db_and_financial_coverage(database):
    listing()
    service.start_job(1, 'UK', 'initial')
    def fetch(**kwargs):
        assert database['open'] == 0
        return observations(**kwargs)
    assert service.run_one(fetch)
    assert service.run_one(fetch)
    job = service.query('SELECT * FROM qd_research_ingestion_jobs')
    assert job['status'] == 'complete'
    item = service.query('SELECT * FROM qd_research_ingestion_items')
    assert item['coverage_json']['state'] == 'ready'
    assert service.query('SELECT currency FROM qd_fundamental_snapshots')['currency'] == 'GBP'


def test_single_operation_and_expired_writer_cannot_publish(database):
    listing()
    service.start_job(1, 'UK', 'initial')
    service.capture_one()
    first = service.claim_one()
    assert service.claim_one() is None
    with service.transaction() as cur:
        cur.execute("UPDATE qd_research_ingestion_items SET lease_until=NOW()-INTERVAL '1 second'")
    second = service.claim_one()
    assert second['token'] != first['token']
    assert not service.publish(first, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))
    assert service.query('SELECT COUNT(*) AS n FROM qd_fundamental_snapshots')['n'] == 0
    assert service.publish(second, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))


def test_two_worker_claims_across_markets_share_one_operation(database):
    listing()
    listing('TEST', 'US', 'NASDAQ')
    service.start_job(1, 'UK', 'worker-uk')
    service.start_job(1, 'US', 'worker-us')
    service.capture_one()
    service.capture_one()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: service.claim_one(), range(2)))
    assert sum(item is not None for item in results) == 1
    assert service.query("SELECT COUNT(*) AS n FROM qd_research_ingestion_items WHERE status='running'")['n'] == 1


def test_failure_cooldown_preserves_rows_and_exhausts_bounded_retries(database):
    listing()
    service.start_job(1, 'UK', 'initial')
    service.capture_one()
    item = service.claim_one()
    service.publish(item, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))
    reset_cooldown()
    service.start_job(1, 'UK', 'refresh', incremental=False)
    service.capture_one()
    for _ in range(3):
        reset_cooldown()
        with service.transaction() as cur:
            cur.execute("UPDATE qd_research_ingestion_items SET retry_at=NOW() WHERE status='pending'")
        item = service.claim_one()
        assert item
        service.publish(item, error=FinancialRetryable('provider_unavailable'))
        assert service.claim_one() is None
    assert service.query('SELECT COUNT(*) AS n FROM qd_fundamental_snapshots')['n'] == 1
    assert service.query('SELECT status FROM qd_research_ingestion_jobs ORDER BY id DESC LIMIT 1')['status'] == 'partial'


def test_ambiguous_listing_is_accounted_for_without_network(database):
    listing('ONE.L', provider='ALIAS.L')
    listing('TWO.L', provider='ALIAS.L')
    service.start_job(1, 'UK', 'ambiguous')
    service.capture_one()
    def forbidden(**kwargs):
        pytest.fail('ambiguous mapping must not call provider')
    service.run_one(forbidden)
    service.run_one(forbidden)
    assert service.query("SELECT COUNT(*) AS n FROM qd_research_ingestion_items WHERE status='unavailable'")['n'] == 2


def test_first_observed_records_not_rewritten_and_incremental_skip(database):
    listing()
    service.start_job(1, 'UK', 'initial')
    service.capture_one()
    item = service.claim_one()
    rows = observations(market='UK', symbol='TEST.L', exchange='LSE')
    rows[0]['available_at'] = date.today() - timedelta(days=3)
    service.publish(item, observations=rows)
    reset_cooldown()
    service.start_job(1, 'UK', 'incremental')
    service.capture_one()
    assert service.claim_one() == {'skipped': True}
    reset_cooldown()
    service.start_job(1, 'UK', 'full', incremental=False)
    service.capture_one()
    item = service.claim_one()
    service.publish(item, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))
    assert service.query('SELECT COUNT(*) AS n FROM qd_fundamental_snapshots')['n'] == 1
    assert service.query('SELECT available_at FROM qd_fundamental_snapshots')['available_at'] == date.today() - timedelta(days=3)


def test_schedule_disabled_and_empty_directory_rejected(database):
    service.enqueue_scheduled()
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_jobs')['n'] == 0
    with pytest.raises(ValueError, match='directory_unavailable'):
        service.start_job(1, 'UK', 'empty')
    listing()
    service.set_schedule(1, 'UK', True)
    service.enqueue_scheduled()
    service.enqueue_scheduled()
    assert service.query('SELECT COUNT(*) AS n FROM qd_research_ingestion_jobs')['n'] == 1
