"""Research membership and publication proof in an owned disposable schema."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import pytest

from tests.test_research_ingestion_postgres import database, listing
from app.services import research_ingestion as financials
from app.services.earnings_research import repository as repo, read, evidence


@pytest.fixture
def research_db(database):
    migration = Path(__file__).resolve().parents[1] / 'migrations/20261005_earnings_research.sql'
    with financials.transaction() as cur:
        cur.execute(migration.read_text())
        cur.execute(migration.read_text())
    yield database


def event(listing_id, days=5):
    return financials.query('''INSERT INTO qd_market_earnings(listing_id,event_date)
        VALUES (?,CURRENT_DATE+?) RETURNING id''', (listing_id, days))['id']


def test_complete_directory_and_dated_membership(research_db):
    with financials.transaction() as cur:
        cur.execute("INSERT INTO qd_earnings_listings(market,exchange,symbol,provider_symbol,name) SELECT 'US','NASDAQ','T'||n,'T'||n,'Company'||n FROM generate_series(1,6001) n")
        cur.execute('INSERT INTO qd_market_earnings(listing_id,event_date) SELECT id,CURRENT_DATE+5 FROM qd_earnings_listings WHERE id%2=0')
    listing('UK.L')
    directory = read.listings(1, page=2, page_size=200)
    assert directory['total'] == 6002 and len(directory['items']) == 200
    summary = read.coverage(1)
    assert summary['directory_total'] == 6002
    for kind in repo.KINDS:
        assert sum(row['n'] for row in summary['source_counts'] if row['kind'] == kind) == 6002
    result = repo.start_job(1, 'all', 30, 'large')
    assert result['expected_count'] == 3000
    assert financials.query('SELECT COUNT(*) AS n FROM qd_earnings_research_items')['n'] == 3000
    job = read.job_detail(1, result['job_id'])
    for kind in repo.KINDS:
        assert sum(row['n'] for row in job['source_counts'] if row['kind'] == kind) == 3000
    assert read.listings(1, mode='candidates')['total'] == 3000
    assert read.listings(1, market='UK')['items'][0]['event_date'] is None


def test_request_replay_ownership_and_cancel(research_db):
    event(listing())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: repo.start_job(1, 'UK', 30, 'same'), range(2)))
    assert len({row['job_id'] for row in results}) == 1
    job = results[0]['job_id']
    assert read.job_detail(1, job)['total'] == 1
    with pytest.raises(ValueError, match='job_not_found'):
        read.job_detail(2, job)
    with pytest.raises(ValueError, match='request_id_conflict'):
        repo.start_job(1, 'US', 30, 'same')
    repo.cancel_job(1, job)
    assert repo.claim_one() is None
    assert read.job_detail(1, job)['job']['status'] == 'cancelled'


def test_fenced_publication_and_stage_independence(research_db):
    target = listing()
    event(target)
    repo.start_job(1, 'UK', 30, 'fenced')
    item = repo.claim_one()
    assert repo.claim_one() is None
    result = {'status': 'ready', 'provider': 'fixture', 'url': 'https://example.com/price',
              'data': {'bars': [], 'currency': 'GBp', 'adjustment': 'split_dividend_adjusted'}, 'gaps': []}
    assert not repo.publish({**item, 'token': 'stale'}, result)
    assert repo.publish(item, result)
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_research_lease SET cooldown_until=NOW()-INTERVAL '1 second'")
    news = repo.claim_one()
    assert news['kind'] == 'news'
    assert repo.publish(news, {'status': 'unavailable', 'provider': 'fixture', 'data': {}, 'gaps': ['outage']})
    bundle = evidence.bundle(target, datetime.now(timezone.utc))
    assert bundle['sources']['prices']['status'] == 'ready'
    assert bundle['sources']['news']['status'] == 'unavailable'


def test_yahoo_yields_to_financial_jobs_but_documents_progress(research_db):
    event(listing())
    financials.start_job(1, 'UK', 'financial')
    repo.start_job(1, 'UK', 30, 'yield')
    item = repo.claim_one()
    assert item['kind'] == 'documents'
    rows = read.listings(1)['items']
    assert rows[0]['waiting_for_financial_sync']


def test_cutoff_identity_and_annual_financials(research_db):
    target = listing('SAME.L')
    listing('SAME.L', exchange='OTHER', provider='OTHER.L')
    with financials.transaction() as cur:
        cur.execute("""INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,frequency,currency,source,source_version,metadata_json,revenue,ingested_at)
            VALUES ('UKStock','SAME.L',CURRENT_DATE-90,CURRENT_DATE,'annual','GBP','research_yahoo_fixture','1','{"exchange":"LSE"}',42,NOW()-INTERVAL '1 hour'),
                   ('UKStock','SAME.L',CURRENT_DATE-90,CURRENT_DATE,'annual','GBP','research_yahoo_revision','2','{"exchange":"LSE"}',999,NOW()+INTERVAL '1 hour')""")
        cur.execute('''UPDATE qd_fundamental_snapshots SET metadata_json=metadata_json || jsonb_build_object('listingIdentity',
            jsonb_build_object('market','UK','exchange','LSE','provider_symbol','SAME.L','symbol','SAME.L','name','SAME.L'))
            WHERE symbol='SAME.L' ''')
    bundle = evidence.bundle(target, datetime.now(timezone.utc))
    assert bundle['financials'][0]['revenue'] == 42
    assert bundle['financials'][0]['frequency'] == 'annual'
    assert 'uk_interim_unavailable' in bundle['gaps']


def test_ambiguous_identity_is_visible_without_provider_work(research_db):
    first = listing('A.L', provider='SAME.L')
    listing('B.L', provider='SAME.L')
    event(first)
    repo.start_job(1, 'UK', 30, 'ambiguous')
    row = repo.claim_one()
    assert row.get('skipped')
    assert read.listings(1)['items'][0]['state'] == 'unsupported'


def test_calendar_republication_preserves_membership_and_work(research_db):
    target = listing()
    original = event(target)
    repo.start_job(1, 'UK', 30, 'republication')
    with financials.transaction() as cur:
        cur.execute('DELETE FROM qd_market_earnings WHERE listing_id=?', (target,))
    replacement = event(target)
    assert replacement != original
    claimed = repo.claim_one()
    assert not claimed.get('skipped')
    assert claimed['event_id'] == original
    assert repo.publish(claimed, {'status': 'unavailable', 'provider': 'fixture', 'data': {}, 'gaps': ['fixture']})


def _reset():
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_research_lease SET cooldown_until=NOW()-INTERVAL '1 second'")


def _price_result(bars):
    return {'status': 'partial', 'provider': 'fixture', 'url': 'https://example.com/price',
            'data': {'bars': bars, 'currency': 'GBp', 'adjustment': 'split_dividend_adjusted'}, 'gaps': []}


def test_exact_price_manifest_no_mixed_adjustment_vintages(research_db):
    target = listing()
    event(target)
    repo.start_job(1, 'UK', 30, 'first-prices')
    first = repo.claim_one()
    bars = [{'session_date': '2026-09-29', 'close': 100}, {'session_date': '2026-09-30', 'close': 101}]
    assert repo.publish(first, _price_result(bars))
    cutoff = datetime.now(timezone.utc)
    repo.cancel_job(1, first['job_id'])
    repo.start_job(1, 'UK', 30, 'revised-prices')
    _reset()
    second = repo.claim_one()
    assert repo.publish(second, _price_result([{'session_date': '2026-09-30', 'close': 50.5}]))
    latest = evidence.bundle(target)
    assert len(latest['bars']) == 1 and latest['bars'][0]['close'] == 50.5
    previous = evidence.bundle(target, cutoff)
    assert len(previous['bars']) == 2 and previous['bars'][0]['close'] == 100


def test_unchanged_contents_deduplicated_but_fresh_observation_recorded(research_db):
    target = listing()
    event(target)
    ids = []
    for index in range(2):
        job = repo.start_job(1, 'UK', 30, 'same-content-' + str(index))['job_id']
        _reset()
        item = repo.claim_one()
        assert repo.publish(item, _price_result([]))
        repo.cancel_job(1, job)
        ids.append(financials.query('SELECT MAX(id) AS id FROM qd_research_observations')['id'])
    assert ids[1] > ids[0]
    assert financials.query('SELECT COUNT(*) AS n FROM qd_research_evidence')['n'] == 1
    assert financials.query('SELECT COUNT(*) AS n FROM qd_research_observations')['n'] == 2


def test_expired_claim_and_cancel_during_operation(research_db):
    event(listing())
    job = repo.start_job(1, 'UK', 30, 'interrupted')['job_id']
    old = repo.claim_one()
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_research_lease SET lease_until=NOW()-INTERVAL '1 second'")
    new = repo.claim_one()
    assert old['token'] != new['token']
    assert not repo.publish(old, _price_result([]))
    repo.cancel_job(1, job)
    assert not repo.publish(new, _price_result([]))
    assert financials.query('SELECT COUNT(*) AS n FROM qd_research_evidence')['n'] == 0


def test_both_directions_of_yahoo_operation_coordination(research_db):
    event(listing())
    repo.start_job(1, 'UK', 30, 'advisory-first')
    claim = repo.claim_one()
    assert claim['kind'] == 'prices'
    financials.start_job(1, 'UK', 'financial-second')
    assert financials.capture_one()
    assert financials.claim_one() is None
    assert repo.publish(claim, _price_result([]))
    assert financials.claim_one() is None  # Shared two-second pacing.
    _reset()
    assert financials.claim_one()


def test_issuer_attestation_bound_to_listing_and_no_fetch(research_db):
    target = listing()
    event(target)
    with pytest.raises(ValueError, match='invalid_issuer_source'):
        repo.verify_issuer_source(1, target, 'https://example.com/ir', False)
    repo.verify_issuer_source(1, target, 'https://example.com/ir', True)
    repo.start_job(1, 'UK', 30, 'verified')
    assert repo.claim_one()['issuer_url'] == 'https://example.com/ir'


def test_cached_sources_do_not_follow_in_place_company_identity_change(research_db):
    target = listing()
    event(target)
    repo.start_job(1, 'UK', 30, 'old-company')
    item = repo.claim_one()
    repo.publish(item, _price_result([{'session_date':'2026-09-30','close':10}]))
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_listings SET name='Different Company',provider_symbol='OTHER.L' WHERE id=?", (target,))
    assert evidence.bundle(target)['sources']['prices']['status'] == 'unavailable'
    assert not evidence.bundle(target)['bars']
    assert read.listings(1)['items'][0]['prices'] is None


def test_new_ambiguity_during_fetch_cannot_publish(research_db):
    target = listing()
    event(target)
    repo.start_job(1, 'UK', 30, 'ambiguity-race')
    item = repo.claim_one()
    listing('OTHER.L', provider='TEST.L')
    assert not repo.publish(item, _price_result([]))
    assert not evidence.bundle(target)['bars']


def test_issuer_attestation_cannot_follow_a_company_name_change(research_db):
    target = listing()
    event(target)
    repo.verify_issuer_source(1, target, 'https://example.com/old-company', True)
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_listings SET name='Different Issuer' WHERE id=?", (target,))
    repo.start_job(1, 'UK', 30, 'changed-issuer')
    assert repo.claim_one()['issuer_url'] is None


def test_publication_fences_catalogue_writes_until_commit(research_db, monkeypatch):
    import psycopg2
    target = listing()
    event(target)
    repo.start_job(1, 'UK', 30, 'catalogue-fence')
    item = repo.claim_one()
    entered, release = Event(), Event()
    original = repo.transaction

    @contextmanager
    def paused(*args, **kwargs):
        with original(*args, **kwargs) as cur:
            class Cursor:
                def execute(self, sql, params=None):
                    if 'SELECT l.is_active' in sql:
                        entered.set()
                        assert release.wait(10), 'publication test was not released'
                    return cur.execute(sql, params)
                def __getattr__(self, name):
                    return getattr(cur, name)
            yield Cursor()

    monkeypatch.setattr(repo, 'transaction', paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        published = pool.submit(repo.publish, item, _price_result([]))
        try:
            assert entered.wait(5)
            # A concurrent directory insert/update must not pass validation and
            # then commit before this evidence publication commits.
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                with financials.transaction() as cur:
                    cur.execute('LOCK TABLE qd_earnings_listings IN ROW EXCLUSIVE MODE NOWAIT')
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                with financials.transaction() as cur:
                    cur.execute('LOCK TABLE qd_market_earnings IN ROW EXCLUSIVE MODE NOWAIT')
        finally:
            release.set()
        assert published.result(timeout=5)
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_listings SET name='New Issuer' WHERE id=?", (target,))
    assert evidence.bundle(target)['sources']['prices']['status'] == 'unavailable'


def test_financials_require_immutable_issuer_identity(research_db):
    target = listing()
    financials.start_job(1, 'UK', 'identity-financial')
    financials.capture_one()
    item = financials.claim_one()
    from tests.test_research_ingestion_postgres import observations
    financials.publish(item, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))
    assert evidence.bundle(target)['financials']
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_listings SET name='Different Issuer' WHERE id=?", (target,))
    changed = evidence.bundle(target)
    assert not changed['financials']
    assert 'financial_issuer_identity_unverified' in changed['gaps']
    assert changed['financial_trends']['revenue_growth_pct'] is None
    assert read.listings(1)['items'][0]['snapshot_id'] is None


def test_legacy_financials_remain_unchanged_and_are_not_issuer_verified(research_db):
    target = listing()
    with financials.transaction() as cur:
        cur.execute('''INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,frequency,currency,source,source_version,metadata_json,revenue)
            VALUES ('UKStock','TEST.L',CURRENT_DATE-90,CURRENT_DATE,'annual','GBP','research_yahoo_old','1','{"exchange":"LSE"}',42)''')
    bundle = evidence.bundle(target)
    assert not bundle['financials'] and 'financial_issuer_identity_unverified' in bundle['gaps']
    assert financials.query("SELECT revenue,metadata_json FROM qd_fundamental_snapshots WHERE source='research_yahoo_old'") == {'revenue': 42, 'metadata_json': {'exchange': 'LSE'}}


def test_financial_issuer_change_during_fetch_cannot_publish(research_db):
    target = listing()
    financials.start_job(1, 'UK', 'changed-during-financial')
    financials.capture_one()
    item = financials.claim_one()
    with financials.transaction() as cur:
        cur.execute("UPDATE qd_earnings_listings SET name='Different Issuer' WHERE id=?", (target,))
    from tests.test_research_ingestion_postgres import observations
    assert financials.publish(item, observations=observations(market='UK', symbol='TEST.L', exchange='LSE'))
    assert financials.query('SELECT COUNT(*) AS n FROM qd_fundamental_snapshots')['n'] == 0
    assert financials.query('SELECT status,error FROM qd_research_ingestion_items')['error'] == 'catalog_identity_changed'


def test_retry_budget_and_cooldown_are_durable(research_db):
    target = listing()
    event(target)
    job = repo.start_job(1, 'UK', 30, 'three-attempts')['job_id']
    for attempt in range(1, 4):
        item = repo.claim_one()
        assert item['kind'] == 'prices' and item['stages']['prices']['attempts'] == attempt
        assert repo.publish(item, {'status': 'retry', 'provider': 'fixture', 'data': {}, 'gaps': ['provider_deadline']})
        state = read.job_detail(1, job)['items'][0]['stages']['prices']
        assert state['status'] == ('failed' if attempt == 3 else 'pending')
        assert repo.claim_one() is None
        with financials.transaction() as cur:
            cur.execute("UPDATE qd_earnings_research_lease SET cooldown_until=NOW()-INTERVAL '1 second'")
            cur.execute("UPDATE qd_earnings_research_items SET retry_at=NOW()-INTERVAL '1 second'")
    assert repo.claim_one()['kind'] == 'news'
    assert evidence.bundle(target)['sources']['prices']['status'] == 'failed'
