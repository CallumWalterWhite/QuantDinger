"""Durable membership and a shared, fenced, finite evidence operation lease."""
from datetime import datetime, timezone
import hashlib
import json
from uuid import uuid4

from app.services import research_ingestion as financials

LOCK = 2026100503
KINDS = ('prices', 'news', 'documents')
FINAL = ('ready', 'partial', 'unavailable', 'unsupported', 'failed')
transaction = financials.transaction
query = financials.query


def validate(market='all', days=30):
    if not isinstance(market, str) or market not in ('all', 'US', 'UK'):
        raise ValueError('invalid_market')
    if type(days) is not int or not 1 <= days <= 90:
        raise ValueError('invalid_days')


def start_job(user_id, market, days, request_id):
    validate(market, days)
    key = financials.request_key(user_id, request_id)
    # Retrying a repeatable-read transaction after a concurrent command is safe;
    # request_key is unique and no provider operation occurs in this transaction.
    import psycopg2
    for attempt in range(3):
        try:
            with transaction(snapshot=True) as cur:
                cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
                cur.execute('SELECT * FROM qd_earnings_research_jobs WHERE request_key=?', (key,))
                prior = cur.fetchone()
                if prior:
                    if prior['market'] != market or prior['days'] != days:
                        raise ValueError('request_id_conflict')
                    return {'job_id': prior['id'], 'replayed': True, 'expected_count': prior['expected_count']}
                cur.execute("SELECT DISTINCT ON(market) market,id FROM qd_earnings_sync_runs WHERE status='success' ORDER BY market,id DESC")
                versions = {row['market']: row['id'] for row in cur.fetchall()}
                if not versions or (market != 'all' and market not in versions):
                    raise ValueError('directory_unavailable')
                cur.execute('''INSERT INTO qd_earnings_research_jobs(requester_id,request_key,market,days,catalog_versions,
                    window_start,window_end,status) VALUES (?,?,?,?,?::jsonb,CURRENT_DATE,CURRENT_DATE+?,'running') RETURNING id''',
                            (user_id, key, market, days, json.dumps(versions), days))
                job_id = cur.fetchone()['id']
                cur.execute('''WITH directory AS (
                    SELECT l.*,COUNT(*) OVER(PARTITION BY market,provider_symbol)>1 AS ambiguous
                    FROM qd_earnings_listings l WHERE is_active
                ) INSERT INTO qd_earnings_research_items(job_id,listing_id,event_id,event_date,identity_json)
                SELECT ?,l.id,e.id,e.event_date,jsonb_build_object('market',l.market,'exchange',l.exchange,
                    'symbol',l.symbol,'provider_symbol',l.provider_symbol,'name',l.name,'ambiguous',l.ambiguous)
                FROM directory l JOIN qd_market_earnings e ON e.listing_id=l.id
                WHERE (?='all' OR l.market=?) AND e.event_type='earnings'
                  AND e.event_date BETWEEN CURRENT_DATE AND CURRENT_DATE+?''', (job_id, market, market, days))
                cur.execute('SELECT COUNT(*) AS n FROM qd_earnings_research_items WHERE job_id=?', (job_id,))
                count = cur.fetchone()['n']
                cur.execute("UPDATE qd_earnings_research_jobs SET expected_count=?,status=? WHERE id=?", (count, 'running' if count else 'complete', job_id))
                return {'job_id': job_id, 'replayed': False, 'expected_count': count}
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.UniqueViolation):
            if attempt == 2:
                raise


def cancel_job(user_id, job_id):
    with transaction() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute('SELECT id FROM qd_earnings_research_jobs WHERE id=? AND requester_id=? FOR UPDATE', (job_id, user_id))
        if not cur.fetchone():
            raise ValueError('job_not_found')
        cur.execute("UPDATE qd_earnings_research_jobs SET status='cancelled',updated_at=NOW() WHERE id=?", (job_id,))
        cur.execute("UPDATE qd_earnings_research_items SET status='cancelled',updated_at=NOW() WHERE job_id=? AND status='pending'", (job_id,))
    return {'job_id': job_id, 'status': 'cancelled'}


def verify_issuer_source(user_id, listing_id, issuer_url, confirmed):
    from urllib.parse import urlsplit
    if confirmed is not True or not isinstance(issuer_url, str) or len(issuer_url) > 8192:
        raise ValueError('invalid_issuer_source')
    parsed = urlsplit(issuer_url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise ValueError('invalid_issuer_source')
    # This is an explicit identity attestation, not a network request. Fetch-time
    # public-DNS checks remain mandatory even for administrator-supplied URLs.
    with transaction() as cur:
        cur.execute('SELECT * FROM qd_earnings_listings WHERE id=? AND is_active FOR SHARE', (listing_id,))
        listing = cur.fetchone()
        if not listing or listing['market'] != 'UK':
            raise ValueError('listing_not_found')
        identity = {key: listing[key] for key in ('market','exchange','provider_symbol','symbol','name')}
        cur.execute('''INSERT INTO qd_research_issuer_sources(listing_id,market,exchange,provider_symbol,identity_json,issuer_url,verified_by)
            VALUES (?,?,?,?,?::jsonb,?,?) RETURNING id''', (listing_id, listing['market'], listing['exchange'], listing['provider_symbol'], json.dumps(identity), issuer_url, user_id))
        return {'source_id': cur.fetchone()['id']}


def _finish(cur):
    cur.execute('''UPDATE qd_earnings_research_jobs j SET status=CASE WHEN EXISTS(
        SELECT 1 FROM qd_earnings_research_items i WHERE i.job_id=j.id AND i.status<>'complete') THEN 'partial' ELSE 'complete' END,
        updated_at=NOW() WHERE status='running' AND NOT EXISTS(
        SELECT 1 FROM qd_earnings_research_items i WHERE i.job_id=j.id AND i.status='pending')''')


def claim_one():
    with transaction() as cur:
        # The financial lock coordinates new Yahoo claims with existing ingestion.
        cur.execute('SELECT pg_advisory_xact_lock(?)', (financials.LOCK,))
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute('SELECT * FROM qd_earnings_research_lease FOR UPDATE')
        lease = cur.fetchone()
        now = datetime.now(timezone.utc)
        if lease['lease_until'] and lease['lease_until'] > now:
            return None
        if lease['token']:
            cur.execute("SELECT stages FROM qd_earnings_research_items WHERE id=? AND status='pending' FOR UPDATE", (lease['item_id'],))
            prior = cur.fetchone()
            if prior:
                stages = prior['stages']
                stage = stages[lease['kind']]
                stage.update(status='failed' if stage['attempts'] >= 3 else 'pending', gaps=['interrupted'])
                cur.execute('UPDATE qd_earnings_research_items SET stages=?::jsonb WHERE id=?', (json.dumps(stages), lease['item_id']))
                _settle(cur, lease['item_id'], stages)
            cur.execute('UPDATE qd_earnings_research_lease SET token=NULL,lease_until=NULL')
        if lease['cooldown_until'] > now:
            return None
        cur.execute("SELECT 1 FROM qd_research_ingestion_jobs WHERE status IN ('queued','expanding','running') LIMIT 1")
        yahoo_busy = bool(cur.fetchone())
        cur.execute('SELECT 1 FROM qd_research_ingestion_schedules WHERE cooldown_until>NOW() LIMIT 1')
        yahoo_busy = yahoo_busy or bool(cur.fetchone())
        kinds = ('documents',) if yahoo_busy else KINDS
        for kind in kinds:
            cur.execute('''SELECT i.*,j.requester_id FROM qd_earnings_research_items i
                JOIN qd_earnings_research_jobs j ON j.id=i.job_id
                WHERE i.status='pending' AND j.status='running' AND i.retry_at<=NOW()
                  AND i.stages->?->>'status'='pending'
                ORDER BY i.event_date,(i.identity_json->>'market'=?)::int,i.updated_at,i.id
                FOR UPDATE OF i SKIP LOCKED LIMIT 1''', (kind, lease['last_market']))
            item = cur.fetchone()
            if not item:
                continue
            identity = item['identity_json']
            cur.execute('SELECT * FROM qd_earnings_listings WHERE id=?', (item['listing_id'],))
            current = cur.fetchone()
            cur.execute("SELECT event_date FROM qd_market_earnings WHERE listing_id=? AND event_date=? AND event_type='earnings'", (item['listing_id'], item['event_date']))
            event = cur.fetchone()
            if not current['is_active'] or any(current[key] != identity[key] for key in ('market','exchange','provider_symbol','symbol','name')) or not event or event['event_date'] != item['event_date']:
                cur.execute("UPDATE qd_earnings_research_items SET status='superseded' WHERE id=?", (item['id'],))
                _finish(cur)
                return {'skipped': True}
            cur.execute('SELECT COUNT(*) AS n FROM qd_earnings_listings WHERE is_active AND market=? AND provider_symbol=?', (identity['market'], identity['provider_symbol']))
            if identity['ambiguous'] or cur.fetchone()['n'] > 1:
                cur.execute("UPDATE qd_earnings_research_items SET status='unsupported' WHERE id=?", (item['id'],))
                _finish(cur)
                return {'skipped': True}
            stages = item['stages']
            stage = stages[kind]
            stage.update(status='running', attempts=stage['attempts'] + 1)
            token = uuid4().hex
            cur.execute('''SELECT issuer_url FROM qd_research_issuer_sources WHERE listing_id=? AND market=? AND exchange=?
                AND provider_symbol=? AND identity_json @> ?::jsonb ORDER BY verified_at DESC,id DESC LIMIT 1''',
                        (item['listing_id'], identity['market'], identity['exchange'], identity['provider_symbol'],
                         json.dumps({key: identity[key] for key in ('market','exchange','provider_symbol','symbol','name')})))
            verified = cur.fetchone()
            cur.execute('UPDATE qd_earnings_research_items SET stages=?::jsonb,updated_at=NOW() WHERE id=?', (json.dumps(stages), item['id']))
            cur.execute("UPDATE qd_earnings_research_lease SET token=?,item_id=?,kind=?,lease_until=NOW()+INTERVAL '3 minutes',last_market=?", (token, item['id'], kind, identity['market']))
            return {**dict(item), **identity, 'stages': stages, 'token': token, 'kind': kind,
                    'issuer_url': verified['issuer_url'] if verified else None}
        _finish(cur)
        return None


def _settle(cur, item_id, stages):
    if all(stage['status'] in FINAL for stage in stages.values()):
        status = 'complete' if all(stage['status'] == 'ready' for stage in stages.values()) else 'partial'
        cur.execute('UPDATE qd_earnings_research_items SET status=?,cutoff=NOW(),updated_at=NOW() WHERE id=?', (status, item_id))
    _finish(cur)


def publish(item, result):
    encoded = json.dumps(result, sort_keys=True, allow_nan=False, default=str)
    if len(encoded.encode()) > 280000 or result.get('status') not in FINAL + ('retry',):
        raise ValueError('invalid_provider_result')
    if len((result.get('data') or {}).get('bars', [])) > 600:
        raise ValueError('invalid_provider_result')
    with transaction() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(?)', (LOCK,))
        cur.execute('SELECT * FROM qd_earnings_research_lease WHERE token=? AND item_id=? AND lease_until>NOW() FOR UPDATE', (item['token'], item['id']))
        if not cur.fetchone():
            return False
        # Fence catalogue writes (including newly inserted ambiguous mappings)
        # through validation and commit. These brief locks cover DB publication
        # only: no provider or network operation runs while they are held.
        # Calendar refresh writes listings before events; keep that lock order.
        cur.execute('LOCK TABLE qd_earnings_listings,qd_market_earnings IN SHARE MODE')
        cur.execute('''SELECT i.*,j.status AS job_status FROM qd_earnings_research_items i
            JOIN qd_earnings_research_jobs j ON j.id=i.job_id WHERE i.id=? FOR UPDATE OF i''', (item['id'],))
        owned = cur.fetchone()
        cur.execute('''SELECT l.is_active,l.market,l.exchange,l.provider_symbol,l.symbol,l.name,e.event_date
            FROM qd_earnings_listings l JOIN qd_market_earnings e ON e.listing_id=l.id
            WHERE l.id=? AND e.event_date=? AND e.event_type='earnings' ''', (item['listing_id'], item['event_date']))
        current = cur.fetchone()
        cur.execute('SELECT COUNT(*) AS n FROM qd_earnings_listings WHERE is_active AND market=? AND provider_symbol=?', (item['market'], item['provider_symbol']))
        ambiguous = cur.fetchone()['n'] > 1
        valid = not ambiguous and owned['job_status'] == 'running' and owned['status'] == 'pending' and current and current['is_active'] and current['event_date'] == item['event_date'] and all(current[key] == item[key] for key in ('market','exchange','provider_symbol','symbol','name'))
        if not valid:
            cur.execute("UPDATE qd_earnings_research_items SET status=CASE WHEN ?='cancelled' THEN 'cancelled' ELSE 'superseded' END WHERE id=?", (owned['job_status'], item['id']))
            cur.execute('UPDATE qd_earnings_research_lease SET token=NULL,lease_until=NULL')
            _finish(cur)
            return False
        stages = owned['stages']
        retry = result['status'] == 'retry' and stages[item['kind']]['attempts'] < 3
        if result['status'] == 'retry':
            result = {**result, 'status': 'pending' if retry else 'failed'}
        stage = {**stages[item['kind']], 'status': result['status'], 'gaps': result.get('gaps', []), 'checked_at': datetime.now(timezone.utc).isoformat()}
        if not retry:
            data = dict(result.get('data') or {})
            bars = data.pop('bars', []) if item['kind'] == 'prices' else []
            identity = {key: item[key] for key in ('market','exchange','provider_symbol','symbol','name')}
            stored = {**result, 'data': data, 'identity': identity}
            # Bars are versioned separately; evidence retains their content hash.
            if bars:
                stored['bars_hash'] = hashlib.sha256(json.dumps(bars, sort_keys=True).encode()).hexdigest()
            digest = hashlib.sha256(json.dumps(stored, sort_keys=True, default=str).encode()).hexdigest()
            bar_ids = []
            for bar in bars[:600]:
                content = json.dumps(bar, sort_keys=True, allow_nan=False)
                hashed = hashlib.sha256(content.encode()).hexdigest()
                cur.execute('''SELECT id,content_hash FROM qd_research_price_bars WHERE listing_id=? AND session_date=?
                    AND provider=? AND adjustment=? AND currency=? ORDER BY observed_at DESC,id DESC LIMIT 1''',
                            (item['listing_id'], bar['session_date'], result['provider'], data['adjustment'], data['currency']))
                prior = cur.fetchone()
                if prior and prior['content_hash'] == hashed:
                    bar_ids.append(prior['id'])
                    continue
                cur.execute('''INSERT INTO qd_research_price_bars(listing_id,session_date,provider,currency,adjustment,content_hash,payload)
                    VALUES (?,?,?,?,?,?,?::jsonb) RETURNING id''', (item['listing_id'], bar['session_date'], result['provider'], data['currency'], data['adjustment'], hashed, content))
                bar_ids.append(cur.fetchone()['id'])
            if item['kind'] == 'prices':
                stored['data']['bar_ids'] = bar_ids
            # Immutable content is deduplicated; each collection retains its own
            # timestamp/reference so A -> B -> A refreshes still read as A.
            cur.execute('''INSERT INTO qd_research_evidence(listing_id,kind,provider,source_url,content_hash,payload)
                VALUES (?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING''', (item['listing_id'], item['kind'], result.get('provider', 'unknown'), result.get('url', ''), digest, json.dumps(stored)))
            cur.execute('SELECT id FROM qd_research_evidence WHERE listing_id=? AND kind=? AND provider=? AND content_hash=?', (item['listing_id'], item['kind'], result.get('provider', 'unknown'), digest))
            stage['evidence_id'] = cur.fetchone()['id']
            collection_key = f"{item['id']}:{item['kind']}:{stage['attempts']}"
            cur.execute('''INSERT INTO qd_research_observations(listing_id,kind,evidence_id,collection_key,identity_json)
                VALUES (?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING''', (item['listing_id'], item['kind'], stage['evidence_id'], collection_key, json.dumps(identity)))
        stages[item['kind']] = stage
        cur.execute("UPDATE qd_earnings_research_items SET stages=?::jsonb,retry_at=NOW()+INTERVAL '5 minutes'*?::int,updated_at=NOW() WHERE id=?", (json.dumps(stages), int(retry), item['id']))
        cur.execute("UPDATE qd_earnings_research_lease SET token=NULL,lease_until=NULL,cooldown_until=NOW()+INTERVAL '1 second'*?::int", (300 if retry else 2,))
        _settle(cur, item['id'], stages)
    return True
