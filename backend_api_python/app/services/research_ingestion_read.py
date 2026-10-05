"""Bounded admin reads; readiness is independent of transport/job completion."""
from datetime import date, datetime

from app.data_providers.research_financials import CORE_FIELDS
from app.services import research_ingestion as jobs
from app.services.events_read import list_market_earnings

STATES = ('ready', 'partial', 'stale', 'no_data', 'unsupported')


def json_values(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_values(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_values(item) for item in value]
    return value


def validate_page(page, page_size):
    if type(page) is not int or not 1 <= page <= 1000000:
        raise ValueError('invalid_page')
    if type(page_size) is not int or not 1 <= page_size <= 200:
        raise ValueError('invalid_page_size')


MISSING = ','.join(f"CASE WHEN s.{field} IS NULL OR s.{field}::text IN ('NaN','Infinity','-Infinity')"
                   + (' OR s.shares_outstanding<=0' if field == 'shares_outstanding' else '')
                   + f" THEN '{field}' END" for field in CORE_FIELDS)
BASE = f'''WITH directory AS (
    SELECT l.*,COUNT(*) OVER(PARTITION BY market,provider_symbol)>1 AS ambiguous
    FROM qd_earnings_listings l WHERE is_active AND market=?
), observations AS (
    SELECT l.id AS listing_id,l.market,l.exchange,l.symbol,l.name,l.ambiguous,
           s.id AS snapshot_id,s.period_end,s.available_at,s.frequency,s.currency,s.source,
           s.ingested_at,ARRAY_REMOVE(ARRAY[{MISSING}],NULL) AS missing,
           e.event_date,e.date_status,t.status AS refresh_status,t.error AS refresh_error,t.updated_at AS last_checked_at
    FROM directory l
    LEFT JOIN LATERAL (
        SELECT * FROM qd_fundamental_snapshots s
        WHERE s.market=CASE WHEN l.market='US' THEN 'USStock' ELSE 'UKStock' END
          AND s.symbol=l.provider_symbol AND LEFT(s.source,15)='research_yahoo_'
          AND s.metadata_json->>'exchange'=l.exchange
        ORDER BY s.period_end DESC,s.ingested_at DESC,s.id DESC LIMIT 1
    ) s ON NOT l.ambiguous
    LEFT JOIN LATERAL (
        SELECT event_date,date_status FROM qd_market_earnings e
        WHERE e.listing_id=l.id AND e.event_date>=CURRENT_DATE ORDER BY event_date,id LIMIT 1
    ) e ON TRUE
    LEFT JOIN LATERAL (
        SELECT status,error,updated_at FROM qd_research_ingestion_items i
        WHERE i.listing_id=l.id ORDER BY i.updated_at DESC,i.id DESC LIMIT 1
    ) t ON TRUE
), coverage AS (
    SELECT *,CASE WHEN ambiguous THEN 'unsupported' WHEN snapshot_id IS NULL THEN 'no_data'
        WHEN period_end<CURRENT_DATE-CASE WHEN market='UK' THEN 550 ELSE 200 END THEN 'stale'
        WHEN CARDINALITY(missing)>0 THEN 'partial' ELSE 'ready' END AS state,
        'first_observed' AS availability_basis,'unverified' AS historical_comparability
    FROM observations
) '''


def listings(market, page=1, page_size=50, query='', state=''):
    jobs.validate_market(market)
    validate_page(page, page_size)
    if not isinstance(query, str) or len(query.strip()) > 100:
        raise ValueError('invalid_query')
    if state and state not in STATES:
        raise ValueError('invalid_state')
    conditions, args = ['TRUE'], [market]
    if query.strip():
        pattern = '%' + query.strip().replace('!', '!!').replace('%', '!%').replace('_', '!_') + '%'
        conditions.append("(symbol ILIKE ? ESCAPE '!' OR name ILIKE ? ESCAPE '!')")
        args.extend((pattern, pattern))
    if state:
        conditions.append('state=?')
        args.append(state)
    where = ' WHERE ' + ' AND '.join(conditions)
    with jobs.transaction(snapshot=True) as cur:
        cur.execute(BASE + 'SELECT COUNT(*) AS n FROM coverage' + where, tuple(args))
        total = cur.fetchone()['n']
        cur.execute(BASE + 'SELECT * FROM coverage' + where + ' ORDER BY market,exchange,symbol,listing_id LIMIT ? OFFSET ?',
                    (*args, page_size, (page - 1) * page_size))
        items = cur.fetchall()
    return json_values({'items': items, 'total': total, 'page': page, 'page_size': page_size,
                        'acceptance_fields': CORE_FIELDS})


def job_detail(job_id, page=1, page_size=50):
    validate_page(page, page_size)
    with jobs.transaction(snapshot=True) as cur:
        cur.execute('''SELECT id,market,dataset,status,error,expected_count,catalog_run_id,incremental,
            retry_job_id,created_at,updated_at FROM qd_research_ingestion_jobs WHERE id=?''', (job_id,))
        job = cur.fetchone()
        if not job:
            raise ValueError('job_not_found')
        cur.execute('SELECT status,COUNT(*) AS n FROM qd_research_ingestion_items WHERE job_id=? GROUP BY status', (job_id,))
        counts = {row['status']: row['n'] for row in cur.fetchall()}
        cur.execute('''SELECT listing_id,symbol,exchange,status,attempts,error,coverage_json,updated_at
            FROM qd_research_ingestion_items WHERE job_id=? ORDER BY exchange,symbol,id LIMIT ? OFFSET ?''',
            (job_id, page_size, (page - 1) * page_size))
        items = cur.fetchall()
    return json_values({'job': job, 'counts': counts, 'items': items, 'total': sum(counts.values()),
                        'page': page, 'page_size': page_size})


def overview():
    # Existing calendar reader already handles shared snapshot freshness safely.
    calendar = list_market_earnings(days=90, page_size=1)['coverage']
    result = []
    with jobs.transaction(snapshot=True) as cur:
        for market in ('US', 'UK'):
            cur.execute(BASE + 'SELECT state,COUNT(*) AS n FROM coverage GROUP BY state', (market,))
            counts = {state: 0 for state in STATES}
            counts.update({row['state']: row['n'] for row in cur.fetchall()})
            cur.execute('''SELECT id,market,status,error,expected_count,created_at,updated_at
                FROM qd_research_ingestion_jobs WHERE market=? ORDER BY id DESC LIMIT 1''', (market,))
            job = cur.fetchone()
            cur.execute('SELECT market,enabled,next_at,cooldown_until,last_job_id FROM qd_research_ingestion_schedules WHERE market=?', (market,))
            schedule = cur.fetchone()
            result.append({'market': market, 'listing_count': sum(counts.values()), 'financial_coverage': counts,
                           'calendar': next((row for row in calendar if row['market'] == market), None),
                           'job': job, 'schedule': schedule, 'acceptance_fields': CORE_FIELDS,
                           'financial_frequency': 'quarterly' if market == 'US' else 'annual',
                           'interim_coverage': 'unavailable' if market == 'UK' else 'not_applicable',
                           'historical_comparability': 'unverified'})
    return json_values({'markets': result, 'mode': 'best_effort', 'venue_verified': False})
