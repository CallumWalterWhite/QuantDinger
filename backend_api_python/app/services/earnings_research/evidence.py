"""Explicit research reads that exclude observations acquired after the cutoff."""
from datetime import datetime, timezone
import math
import hashlib
import json

from app.services import research_ingestion as financials
from app.services.research_ingestion_read import json_values
from app.services.earnings_research.features import financial_trends, price_features


def bundle(listing_id, cutoff=None):
    cutoff = cutoff or datetime.now(timezone.utc)
    if not isinstance(cutoff, datetime) or cutoff.tzinfo is None or cutoff > datetime.now(timezone.utc):
        raise ValueError('invalid_cutoff')
    with financials.transaction(snapshot=True) as cur:
        cur.execute('SELECT * FROM qd_earnings_listings WHERE id=?', (listing_id,))
        listing = cur.fetchone()
        if not listing:
            raise ValueError('listing_not_found')
        cur.execute('SELECT COUNT(*) AS n FROM qd_earnings_listings WHERE is_active AND market=? AND provider_symbol=?', (listing['market'], listing['provider_symbol']))
        ambiguous = cur.fetchone()['n'] > 1
        identity = {key: listing[key] for key in ('market','exchange','provider_symbol','symbol','name')}
        rows = []
        unverified_financials = False
        if not ambiguous:
            cur.execute('''SELECT 1 FROM qd_fundamental_snapshots WHERE market=? AND symbol=?
                AND metadata_json->>'exchange'=? AND LEFT(source,15)='research_yahoo_'
                AND ingested_at<=? AND NOT COALESCE(metadata_json->'listingIdentity' @> ?::jsonb,FALSE) LIMIT 1''',
                        ('USStock' if listing['market'] == 'US' else 'UKStock', listing['provider_symbol'], listing['exchange'], cutoff, json.dumps(identity)))
            unverified_financials = bool(cur.fetchone())
            cur.execute('''SELECT DISTINCT ON(period_end,frequency) id,period_end,frequency,currency,available_at,ingested_at,
                revenue,net_income,free_cash_flow,total_debt,shareholder_equity,shares_outstanding,source,source_version
                FROM qd_fundamental_snapshots WHERE market=? AND symbol=? AND metadata_json->>'exchange'=?
                  AND LEFT(source,15)='research_yahoo_' AND ingested_at<=? AND period_end<=?::date
                  AND metadata_json->'listingIdentity' @> ?::jsonb
                ORDER BY period_end DESC,frequency,ingested_at DESC,id DESC LIMIT 12''',
                        ('USStock' if listing['market'] == 'US' else 'UKStock', listing['provider_symbol'], listing['exchange'], cutoff, cutoff, json.dumps(identity)))
            rows = cur.fetchall()
        cur.execute('''SELECT DISTINCT ON(o.kind) e.id,o.kind,e.provider,e.source_url,o.observed_at,e.payload,e.content_hash
            FROM qd_research_observations o JOIN qd_research_evidence e ON e.id=o.evidence_id
            WHERE o.listing_id=? AND o.observed_at<=? AND (e.published_at IS NULL OR e.published_at<=?)
              AND NOT ? AND o.identity_json @> ?::jsonb
            ORDER BY o.kind,o.observed_at DESC,o.id DESC''', (listing_id, cutoff, cutoff, ambiguous, json.dumps(identity)))
        sources = {row['kind']: {**row['payload'], 'evidence_id': row['id'], 'observed_at': row['observed_at'],
                                 'content_hash': row['content_hash']} for row in cur.fetchall()}
        for kind in ('prices', 'news', 'documents'):
            sources.setdefault(kind, {'status': 'unavailable', 'data': {}, 'gaps': ['not_collected']})
        meta = sources['prices'].get('data') or {}
        ids = meta.get('bar_ids', [])
        cur.execute('''SELECT payload FROM qd_research_price_bars WHERE id=ANY(?::bigint[]) AND listing_id=?
            AND observed_at<=? AND session_date<=?::date ORDER BY session_date LIMIT 600''', (ids, listing_id, cutoff, cutoff))
        bars = [row['payload'] for row in cur.fetchall()]
        actual_hash = hashlib.sha256(json.dumps(bars, sort_keys=True).encode()).hexdigest()
        if ids and (len(bars) != len(ids) or actual_hash != sources['prices'].get('bars_hash')):
            bars = []
            sources['prices']['gaps'].append('price_manifest_mismatch')
    for row in rows:
        for key, value in row.items():
            if isinstance(value, float) and not math.isfinite(value):
                row[key] = None
    gaps = ['historical_availability_unverified']
    if listing['market'] == 'UK':
        gaps.append('uk_interim_unavailable')
    if ambiguous:
        gaps.append('ambiguous_identity')
    if unverified_financials:
        gaps.append('financial_issuer_identity_unverified')
    if not rows:
        gaps.append('financials_unavailable')
    financial_coverage = financials.observation_coverage(rows[0] if rows else None, listing['market'], today=cutoff.date())
    if financial_coverage['state'] in ('stale', 'partial'):
        gaps.append('financials_' + financial_coverage['state'])
    trends = financial_trends(rows)
    prices = price_features(bars, meta, cutoff)
    gaps.extend(trends['gaps'] + prices['gaps'])
    return json_values({'listing': listing, 'cutoff': cutoff, 'availability_basis': 'first_observed',
                        'financials': rows, 'financial_coverage': financial_coverage,
                        'sources': sources, 'bars': bars, 'financial_trends': trends, 'price_features': prices, 'gaps': gaps})
