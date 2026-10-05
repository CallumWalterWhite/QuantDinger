"""Public-market financial jobs with durable membership and fenced publication.

Network work runs outside database contexts. A shared database claim prevents
overlapping provider operations; stale claims cannot publish observations.
"""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
import math
import re
from uuid import uuid4

from app.data_providers.research_financials import (
    CORE_FIELDS, FinancialRetryable, FinancialUnavailable, fetch_financials,
)
from app.services.fundamental_data import FUNDAMENTAL_FIELDS
from app.utils.db import get_db_connection

LOCK = 2026100508
ACTIVE = ('queued', 'expanding', 'running')


@contextmanager
def transaction(snapshot=False):
    with get_db_connection() as db:
        if hasattr(db, 'rollback_only'):
            raise RuntimeError('research_requires_independent_connection')
        if snapshot:
            db.rollback()  # Pool health probe opens a transaction.
        cur = db.cursor()
        try:
            if snapshot:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            yield cur
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


def query(sql, args=(), many=False):
    with transaction() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if many else cur.fetchone()


def validate_market(market):
    if not isinstance(market, str) or market not in {'US', 'UK'}:
        raise ValueError('invalid_market')


def request_key(user_id, request_id, retry_job=None):
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9:_\-]{1,100}', request_id):
        raise ValueError('invalid_request_id')
    return f'{user_id}:retry:{retry_job}:{request_id}' if retry_job else f'{user_id}:sync:{request_id}'


def start_job(user_id, market, request_id, incremental=True, retry_job=None):
    validate_market(market)
    if type(incremental) is not bool:
        raise ValueError('invalid_policy')
    key = request_key(user_id, request_id, retry_job)
    with transaction() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute('SELECT * FROM qd_research_ingestion_jobs WHERE request_keys @> ?::text[]', ([key],))
        previous = cur.fetchone()
        if previous:
            if previous['market'] != market or previous['incremental'] != incremental:
                raise ValueError('request_id_conflict')
            return {'started': False, 'replayed': True, 'job_id': previous['id']}
        if retry_job:
            cur.execute('SELECT * FROM qd_research_ingestion_jobs WHERE id=?', (retry_job,))
            parent = cur.fetchone()
            if not parent or parent['market'] != market or parent['status'] in ACTIVE:
                raise ValueError('invalid_retry')
            cur.execute("SELECT 1 FROM qd_research_ingestion_items WHERE job_id=? AND status='failed' LIMIT 1", (retry_job,))
            if not cur.fetchone():
                raise ValueError('invalid_retry')
        cur.execute("SELECT id,incremental FROM qd_research_ingestion_jobs WHERE market=? AND status IN ('queued','expanding','running')", (market,))
        active = cur.fetchone()
        if active:
            if retry_job or active['incremental'] != incremental:
                raise ValueError('job_active')
            cur.execute('UPDATE qd_research_ingestion_jobs SET request_keys=array_append(request_keys,?) WHERE id=?', (key, active['id']))
            return {'started': False, 'job_id': active['id']}
        cur.execute("SELECT 1 FROM qd_earnings_sync_runs WHERE market=? AND status='success' LIMIT 1", (market,))
        if not cur.fetchone():
            raise ValueError('directory_unavailable')
        cur.execute('SELECT 1 FROM qd_earnings_listings WHERE market=? AND is_active LIMIT 1', (market,))
        if not cur.fetchone():
            raise ValueError('directory_unavailable')
        cur.execute('''INSERT INTO qd_research_ingestion_jobs
            (market,requester_id,request_keys,incremental,retry_job_id,fields_json)
            VALUES (?,?,?::text[],?,?,?::jsonb) RETURNING id''',
            (market, user_id, [key], incremental, retry_job, json.dumps(CORE_FIELDS)))
        return {'started': True, 'job_id': cur.fetchone()['id']}


def observation_coverage(row, market, today=None):
    today = today or date.today()
    missing = [field for field in CORE_FIELDS if not row or not isinstance(row.get(field), (float, int))
               or not math.isfinite(row[field]) or (field == 'shares_outstanding' and row[field] <= 0)]
    old = bool(row and (today - row['period_end']).days > (550 if market == 'UK' else 200))
    return {'state': 'no_data' if not row else 'stale' if old else 'partial' if missing else 'ready',
            'missing': missing, 'period_end': row['period_end'].isoformat() if row else None,
            'currency': row.get('currency') if row else None,
            'frequency': row.get('frequency') if row else None,
            'availability_basis': 'first_observed', 'historical_comparability': 'unverified'}


def refresh_due(previous, market, now=None):
    now = now or datetime.now(timezone.utc)
    if not previous:
        return True
    interval = 1 if previous['status'] in {'failed', 'unavailable'} else 7
    coverage = previous.get('coverage_json') or {}
    period = coverage.get('period_end')
    old_period = period and (now.date() - date.fromisoformat(period)).days >= (400 if market == 'UK' else 100)
    if coverage.get('state') in {'stale', 'partial', 'no_data'} or old_period:
        interval = 1
    return now - previous['updated_at'] >= timedelta(days=interval)


def capture_one():
    with transaction(snapshot=True) as cur:
        cur.execute("SELECT * FROM qd_research_ingestion_jobs WHERE status='queued' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1")
        job = cur.fetchone()
        if not job:
            return False
        cur.execute("UPDATE qd_research_ingestion_jobs SET status='expanding' WHERE id=?", (job['id'],))
        cur.execute("SELECT id FROM qd_earnings_sync_runs WHERE market=? AND status='success' ORDER BY id DESC LIMIT 1", (job['market'],))
        published = cur.fetchone()
        cur.execute('''INSERT INTO qd_research_ingestion_items(job_id,listing_id,symbol,exchange,provider_symbol,ambiguous)
            SELECT ?,l.id,l.symbol,l.exchange,l.provider_symbol,
                   COUNT(*) OVER(PARTITION BY l.market,l.provider_symbol)>1
            FROM qd_earnings_listings l WHERE l.market=? AND l.is_active
              AND (?::bigint IS NULL OR l.id IN (SELECT listing_id FROM qd_research_ingestion_items
                   WHERE job_id=? AND status='failed'))
            ON CONFLICT DO NOTHING''', (job['id'], job['market'], job['retry_job_id'], job['retry_job_id']))
        # Ambiguity counts use the full directory, not the retry subset.
        cur.execute('''UPDATE qd_research_ingestion_items i SET ambiguous=TRUE FROM
            (SELECT provider_symbol FROM qd_earnings_listings WHERE market=? AND is_active
             GROUP BY provider_symbol HAVING COUNT(*)>1) a
            WHERE i.job_id=? AND i.provider_symbol=a.provider_symbol''', (job['market'], job['id']))
        cur.execute('SELECT COUNT(*) AS n FROM qd_research_ingestion_items WHERE job_id=?', (job['id'],))
        count = cur.fetchone()['n']
        cur.execute('''UPDATE qd_research_ingestion_jobs SET catalog_run_id=?,expected_count=?,
            status=?,error=?,updated_at=NOW() WHERE id=?''',
            (published['id'] if published else None, count, 'running' if count and published else 'failed',
             '' if count and published else 'directory_unavailable', job['id']))
    return True


def _finish(cur):
    cur.execute('''UPDATE qd_research_ingestion_jobs j SET status=CASE
        WHEN EXISTS(SELECT 1 FROM qd_research_ingestion_items i WHERE i.job_id=j.id AND i.status IN ('failed','unavailable')) THEN 'partial'
        ELSE 'complete' END,updated_at=NOW()
        WHERE j.status='running' AND NOT EXISTS(SELECT 1 FROM qd_research_ingestion_items i
             WHERE i.job_id=j.id AND i.status IN ('pending','running'))''')


def claim_one():
    with transaction() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute('''UPDATE qd_research_ingestion_items SET status=CASE WHEN attempts>=3 THEN 'failed' ELSE 'pending' END,
            token=NULL,lease_until=NULL,error='interrupted',retry_at=NOW(),updated_at=NOW()
            WHERE status='running' AND lease_until<NOW()''')
        _finish(cur)
        cur.execute("SELECT 1 FROM qd_research_ingestion_items WHERE status='running' LIMIT 1")
        if cur.fetchone():
            return None
        # Coordinate with optional advisory Yahoo collection. Older disposable
        # schemas do not contain the additive evidence migration.
        cur.execute("SELECT to_regclass('qd_earnings_research_lease') AS table_name")
        if cur.fetchone()['table_name']:
            cur.execute("SELECT 1 FROM qd_earnings_research_lease WHERE (lease_until>NOW() OR cooldown_until>NOW()) AND kind IN ('prices','news') LIMIT 1")
            if cur.fetchone():
                return None
        cur.execute('SELECT 1 FROM qd_research_ingestion_schedules WHERE cooldown_until>NOW() LIMIT 1')
        if cur.fetchone():
            return None
        cur.execute('''SELECT i.*,j.market,j.incremental FROM qd_research_ingestion_items i
            JOIN qd_research_ingestion_jobs j ON j.id=i.job_id
            WHERE i.status='pending' AND i.retry_at<=NOW() AND j.status='running'
            ORDER BY i.attempts,i.id FOR UPDATE OF i SKIP LOCKED LIMIT 1''')
        row = cur.fetchone()
        if not row:
            return None
        if row['ambiguous']:
            cur.execute("UPDATE qd_research_ingestion_items SET status='unavailable',error='ambiguous_identity',updated_at=NOW() WHERE id=?", (row['id'],))
            _finish(cur)
            return {'skipped': True}
        if row['incremental'] and row['attempts'] == 0:
            cur.execute('''SELECT i.status,i.updated_at,i.coverage_json FROM qd_research_ingestion_items i
                JOIN qd_research_ingestion_jobs j ON j.id=i.job_id
                WHERE i.listing_id=? AND i.job_id<>? AND j.market=? AND i.provider_symbol=?
                  AND i.status IN ('success','unavailable','failed') ORDER BY i.updated_at DESC LIMIT 1''',
                (row['listing_id'], row['job_id'], row['market'], row['provider_symbol']))
            previous = cur.fetchone()
            if not refresh_due(previous, row['market']):
                cur.execute("UPDATE qd_research_ingestion_items SET status='skipped',coverage_json=?::jsonb,updated_at=NOW() WHERE id=?",
                            (json.dumps(previous['coverage_json']), row['id']))
                _finish(cur)
                return {'skipped': True}
        token = uuid4().hex
        cur.execute('SELECT market,exchange,provider_symbol,symbol,name,is_active FROM qd_earnings_listings WHERE id=?', (row['listing_id'],))
        current = cur.fetchone()
        identity = {key: current[key] for key in ('market','exchange','provider_symbol','symbol','name')}
        cur.execute("UPDATE qd_research_ingestion_items SET status='running',attempts=attempts+1,token=?,lease_until=NOW()+INTERVAL '4 minutes',updated_at=NOW() WHERE id=?", (token, row['id']))
        return {**dict(row), 'token': token, 'attempts': row['attempts'] + 1, 'identity': identity}


def publish(item, observations=None, error=None):
    with transaction() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute("SELECT * FROM qd_research_ingestion_items WHERE id=? AND token=? AND status='running' AND lease_until>NOW() FOR UPDATE", (item['id'], item['token']))
        owned = cur.fetchone()
        if not owned:
            return False
        identity = item.get('identity')
        if not error and identity:
            # New observations carry immutable issuer provenance. Older rows
            # remain untouched and cannot be promoted to verified research.
            cur.execute('LOCK TABLE qd_earnings_listings,qd_market_earnings IN SHARE MODE')
            cur.execute('SELECT * FROM qd_earnings_listings WHERE id=?', (item['listing_id'],))
            current = cur.fetchone()
            cur.execute('SELECT COUNT(*) AS n FROM qd_earnings_listings WHERE is_active AND market=? AND provider_symbol=?', (identity['market'], identity['provider_symbol']))
            ambiguous = cur.fetchone()['n'] > 1
            if (ambiguous or not current['is_active']
                    or any(current[key] != value for key, value in identity.items())
                    or any(identity[key] != item[key] for key in ('market','exchange','provider_symbol','symbol'))):
                error = FinancialUnavailable('catalog_identity_changed')
        coverage = {}
        if error:
            transient = isinstance(error, FinancialRetryable)
            status = ('failed' if owned['attempts'] >= 3 else 'pending') if transient else 'unavailable'
            if transient:
                cur.execute("UPDATE qd_research_ingestion_schedules SET cooldown_until=NOW()+INTERVAL '5 minutes'")
            reason = str(error) if isinstance(error, (FinancialUnavailable, FinancialRetryable)) else 'provider_unavailable'
        else:
            status, reason = 'success', ''
            for row in observations:
                if identity:
                    import hashlib
                    version = hashlib.sha256((row['source_version'] + json.dumps(identity, sort_keys=True)).encode()).hexdigest()
                    row = {**row,
                           'source': f"research_yahoo_{row['frequency']}:{version[:16]}",
                           'metadata': {**row['metadata'], 'listingIdentity': identity}}
                # Preserve first observation, including same-day revisions as distinct sources.
                cur.execute('''SELECT 1 FROM qd_fundamental_snapshots WHERE market=? AND symbol=?
                    AND period_end=? AND source=? AND source_version=? LIMIT 1''',
                    (row['market'], row['symbol'], row['period_end'], row['source'], row['source_version']))
                if cur.fetchone():
                    continue
                cur.execute(f'''INSERT INTO qd_fundamental_snapshots
                    (market,symbol,period_end,available_at,frequency,currency,{','.join(FUNDAMENTAL_FIELDS)},source,source_version,metadata_json)
                    VALUES (?,?,?,?,?,?,{','.join('?' for _ in FUNDAMENTAL_FIELDS)},?,?,?::jsonb)
                    ON CONFLICT DO NOTHING''',
                    (row['market'], row['symbol'], row['period_end'], row['available_at'], row['frequency'], row['currency'],
                     *(row[field] for field in FUNDAMENTAL_FIELDS), row['source'], row['source_version'], json.dumps(row['metadata'])))
            coverage = observation_coverage(max(observations, key=lambda row: row['period_end']), item['market'])
        cur.execute('''UPDATE qd_research_ingestion_items SET status=?,error=?,coverage_json=?::jsonb,
            token=NULL,lease_until=NULL,retry_at=NOW()+INTERVAL '5 minutes',updated_at=NOW() WHERE id=? AND token=?''',
            (status, reason, json.dumps(coverage), item['id'], item['token']))
        cur.execute("UPDATE qd_research_ingestion_schedules SET cooldown_until=GREATEST(cooldown_until,NOW()+INTERVAL '2 seconds')")
        _finish(cur)
    return True


def run_one(fetch=fetch_financials):
    if capture_one():
        return True
    item = claim_one()
    if not item:
        return False
    if item.get('skipped'):
        return True
    try:
        observations = fetch(market=item['market'], symbol=item['provider_symbol'], exchange=item['exchange'])
    except FinancialUnavailable as exc:
        publish(item, error=exc)
    except Exception:
        publish(item, error=FinancialRetryable('provider_unavailable'))
    else:
        publish(item, observations=observations)
    return True


def set_schedule(user_id, market, enabled):
    validate_market(market)
    if type(enabled) is not bool:
        raise ValueError('invalid_schedule')
    with transaction() as cur:
        cur.execute('UPDATE qd_research_ingestion_schedules SET enabled=?,requester_id=?,next_at=NOW() WHERE market=?', (enabled, user_id, market))
    return {'market': market, 'enabled': enabled}


def enqueue_scheduled():
    rows = query('SELECT market,requester_id,next_at FROM qd_research_ingestion_schedules WHERE enabled AND next_at<=NOW()', many=True)
    for row in rows:
        key = 'daily:' + row['market'] + ':' + str(int(row['next_at'].timestamp()))
        try:
            result = start_job(row['requester_id'], row['market'], key)
        except ValueError:
            continue
        with transaction() as cur:
            cur.execute("UPDATE qd_research_ingestion_schedules SET next_at=NOW()+INTERVAL '1 day',last_job_id=? WHERE market=? AND next_at=? AND enabled",
                        (result['job_id'], row['market'], row['next_at']))
