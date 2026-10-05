"""Cached administrative coverage with owner-scoped durable job details."""
from app.services import research_ingestion as financials
from app.services.research_ingestion_read import json_values, validate_page, MISSING
from app.services.earnings_research.repository import validate

STATES = ('ready', 'partial', 'stale', 'no_data', 'unsupported')
BASE = f'''WITH directory AS (
    SELECT l.*,COUNT(*) OVER(PARTITION BY market,provider_symbol)>1 AS ambiguous
    FROM qd_earnings_listings l WHERE is_active
), observations AS (
    SELECT l.id AS listing_id,l.market,l.exchange,l.symbol,l.name,l.ambiguous,
           e.event_date,e.date_status,s.id AS snapshot_id,s.frequency,s.currency,s.period_end,s.ingested_at,
           ARRAY_REMOVE(ARRAY[{MISSING}],NULL) AS missing,
           p.payload->>'status' AS prices,p.observed_at AS price_observed_at,
           n.payload->>'status' AS news,d.payload->>'status' AS documents,
           i.status AS work_status,i.stages,i.job_id,
           EXISTS(SELECT 1 FROM qd_research_ingestion_jobs WHERE status IN ('queued','expanding','running')) AS waiting_for_financial_sync
    FROM directory l
    LEFT JOIN LATERAL (SELECT * FROM qd_market_earnings WHERE listing_id=l.id AND event_type='earnings'
        AND event_date BETWEEN CURRENT_DATE AND CURRENT_DATE+? ORDER BY event_date,id LIMIT 1) e ON TRUE
    LEFT JOIN LATERAL (SELECT * FROM qd_fundamental_snapshots s
        WHERE s.market=CASE WHEN l.market='US' THEN 'USStock' ELSE 'UKStock' END AND s.symbol=l.provider_symbol
          AND s.metadata_json->>'exchange'=l.exchange AND LEFT(s.source,15)='research_yahoo_'
          AND s.metadata_json->'listingIdentity' @> jsonb_build_object('market',l.market,'exchange',l.exchange,'provider_symbol',l.provider_symbol,'symbol',l.symbol,'name',l.name)
          AND s.ingested_at<=NOW() ORDER BY period_end DESC,ingested_at DESC,id DESC LIMIT 1) s ON NOT l.ambiguous
    LEFT JOIN LATERAL (SELECT e.payload,o.observed_at FROM qd_research_observations o JOIN qd_research_evidence e ON e.id=o.evidence_id
        WHERE o.listing_id=l.id AND o.kind='prices' AND o.identity_json @> jsonb_build_object('market',l.market,'exchange',l.exchange,'symbol',l.symbol,'provider_symbol',l.provider_symbol,'name',l.name)
        ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) p ON NOT l.ambiguous
    LEFT JOIN LATERAL (SELECT e.payload FROM qd_research_observations o JOIN qd_research_evidence e ON e.id=o.evidence_id
        WHERE o.listing_id=l.id AND o.kind='news' AND o.identity_json @> jsonb_build_object('market',l.market,'exchange',l.exchange,'symbol',l.symbol,'provider_symbol',l.provider_symbol,'name',l.name)
        ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) n ON NOT l.ambiguous
    LEFT JOIN LATERAL (SELECT e.payload FROM qd_research_observations o JOIN qd_research_evidence e ON e.id=o.evidence_id
        WHERE o.listing_id=l.id AND o.kind='documents' AND o.identity_json @> jsonb_build_object('market',l.market,'exchange',l.exchange,'symbol',l.symbol,'provider_symbol',l.provider_symbol,'name',l.name)
        ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) d ON NOT l.ambiguous
    LEFT JOIN LATERAL (SELECT i.* FROM qd_earnings_research_items i JOIN qd_earnings_research_jobs j ON j.id=i.job_id
        WHERE i.listing_id=l.id AND j.requester_id=? AND i.identity_json @> jsonb_build_object('market',l.market,'exchange',l.exchange,'symbol',l.symbol,'provider_symbol',l.provider_symbol,'name',l.name)
        ORDER BY i.updated_at DESC,i.id DESC LIMIT 1) i ON TRUE
), coverage AS (
    SELECT *,CASE WHEN ambiguous THEN 'unsupported' WHEN snapshot_id IS NULL AND prices IS NULL THEN 'no_data'
        WHEN period_end<CURRENT_DATE-CASE WHEN market='UK' THEN 550 ELSE 200 END OR price_observed_at<NOW()-INTERVAL '7 days' THEN 'stale'
        WHEN snapshot_id IS NOT NULL AND CARDINALITY(missing)=0 AND prices='ready' AND news='ready' AND documents='ready' THEN 'ready'
        ELSE 'partial' END AS state FROM observations
) '''


def listings(user_id, market='all', days=30, page=1, page_size=50, query='', state='', mode='directory'):
    validate(market, days)
    validate_page(page, page_size)
    if not isinstance(query, str) or len(query) > 100:
        raise ValueError('invalid_query')
    if state and state not in STATES:
        raise ValueError('invalid_state')
    if mode not in ('directory', 'candidates'):
        raise ValueError('invalid_mode')
    where, args = ["(?='all' OR market=?)"], [days, user_id, market, market]
    if mode == 'candidates':
        where.append('event_date IS NOT NULL')
    if query:
        pattern = '%' + query.replace('!', '!!').replace('%', '!%').replace('_', '!_') + '%'
        where.append("(symbol ILIKE ? ESCAPE '!' OR name ILIKE ? ESCAPE '!')")
        args.extend((pattern, pattern))
    if state:
        where.append('state=?')
        args.append(state)
    predicate = ' WHERE ' + ' AND '.join(where)
    with financials.transaction(snapshot=True) as cur:
        cur.execute(BASE + 'SELECT COUNT(*) AS n FROM coverage' + predicate, tuple(args))
        total = cur.fetchone()['n']
        cur.execute(BASE + 'SELECT * FROM coverage' + predicate + ' ORDER BY market,exchange,symbol,listing_id LIMIT ? OFFSET ?', (*args, page_size, (page - 1) * page_size))
        items = cur.fetchall()
    return json_values({'items': items, 'total': total, 'page': page, 'page_size': page_size, 'mode': mode})


def coverage(user_id, market='all', days=30):
    validate(market, days)
    with financials.transaction(snapshot=True) as cur:
        cur.execute(BASE + "SELECT market,state,COUNT(*) AS n,COUNT(event_date) AS dated FROM coverage WHERE (?='all' OR market=?) GROUP BY market,state", (days, user_id, market, market))
        rows = cur.fetchall()
        cur.execute(BASE + '''SELECT source.kind,COALESCE(source.status,'unavailable') AS status,COUNT(*) AS n
            FROM coverage CROSS JOIN LATERAL (VALUES ('prices',prices),('news',news),('documents',documents))
            AS source(kind,status) WHERE (?='all' OR market=?) GROUP BY source.kind,source.status''',
                    (days, user_id, market, market))
        source_counts = cur.fetchall()
        cur.execute('SELECT id,status,expected_count,created_at FROM qd_earnings_research_jobs WHERE requester_id=? ORDER BY id DESC LIMIT 10', (user_id,))
        jobs = cur.fetchall()
        cur.execute('''SELECT COUNT(*) AS n FROM qd_market_earnings e JOIN qd_earnings_listings l ON l.id=e.listing_id
            WHERE l.is_active AND (?='all' OR l.market=?) AND e.event_type='earnings'
              AND e.event_date BETWEEN CURRENT_DATE AND CURRENT_DATE+?''', (market, market, days))
        event_total = cur.fetchone()['n']
    from app.services.earnings_research.worker import enabled
    return json_values({'directory_total': sum(row['n'] for row in rows), 'candidate_total': sum(row['dated'] for row in rows),
                        'candidate_event_total': event_total, 'counts': rows, 'source_counts': source_counts, 'jobs': jobs, 'best_effort': True,
                        'ai_enabled': False, 'evidence_enabled': enabled()})


def job_detail(user_id, job_id, page=1, page_size=50):
    validate_page(page, page_size)
    with financials.transaction(snapshot=True) as cur:
        cur.execute('SELECT id,market,days,status,expected_count,catalog_versions,created_at,updated_at FROM qd_earnings_research_jobs WHERE id=? AND requester_id=?', (job_id, user_id))
        job = cur.fetchone()
        if not job:
            raise ValueError('job_not_found')
        cur.execute('SELECT status,COUNT(*) AS n FROM qd_earnings_research_items WHERE job_id=? GROUP BY status', (job_id,))
        counts = {row['status']: row['n'] for row in cur.fetchall()}
        cur.execute('''SELECT source.kind,source.stage->>'status' AS status,COUNT(*) AS n
            FROM qd_earnings_research_items i CROSS JOIN LATERAL jsonb_each(i.stages) AS source(kind,stage)
            WHERE job_id=? GROUP BY source.kind,source.stage->>'status' ''', (job_id,))
        source_counts = cur.fetchall()
        cur.execute('SELECT id,listing_id,event_id,event_date,identity_json,stages,status,cutoff FROM qd_earnings_research_items WHERE job_id=? ORDER BY id LIMIT ? OFFSET ?', (job_id, page_size, (page-1)*page_size))
        items = cur.fetchall()
    return json_values({'job': job, 'counts': counts, 'source_counts': source_counts, 'items': items, 'total': job['expected_count'], 'page': page, 'page_size': page_size})
