"""Finite maintenance workflow with atomic per-market snapshot publication."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import json

from app.data_providers.market_earnings import fetch_calendar, fetch_listings
from app.services.upcoming_events import event_batch_lock
from app.utils.db import get_db_connection
from app.utils.logger import get_logger

logger = get_logger(__name__)


def start_run(market, start, end):
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute("""INSERT INTO qd_earnings_sync_runs (market, window_start, window_end, status)
                       VALUES (?, ?, ?, 'running') RETURNING id""", (market, start, end))
        run_id = cur.fetchone()["id"]
        db.commit()
        cur.close()
    return run_id


def fail_run(run_id):
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute("""UPDATE qd_earnings_sync_runs SET status = 'failed', finished_at = NOW()
                       WHERE id = ? AND status = 'running'""", (run_id,))
        db.commit()
        cur.close()


def publish_snapshot(run_id, market, start, end, directory, calendar):
    """Replace future rows and success metadata together, never on fetch failure."""
    with get_db_connection() as db:
        try:
            cur = db.cursor()
            cur.execute("UPDATE qd_earnings_listings SET is_active = FALSE WHERE market = ?", (market,))
            cur.execute("""
                INSERT INTO qd_earnings_listings
                    (market, exchange, symbol, provider_symbol, name, instrument_type, segment)
                SELECT ?, r.exchange, r.symbol, r.provider_symbol, r.name, r.instrument_type, r.segment
                FROM jsonb_to_recordset(?::jsonb) AS r(exchange TEXT, symbol TEXT, provider_symbol TEXT,
                    name TEXT, instrument_type TEXT, segment TEXT)
                ON CONFLICT (market, exchange, symbol) DO UPDATE SET
                    provider_symbol = EXCLUDED.provider_symbol, name = EXCLUDED.name,
                    instrument_type = EXCLUDED.instrument_type, segment = EXCLUDED.segment,
                    is_active = TRUE, catalog_at = NOW()
            """, (market, json.dumps(directory["items"])))
            cur.execute("SELECT id, provider_symbol FROM qd_earnings_listings WHERE market = ? AND is_active", (market,))
            identities = defaultdict(list)
            for listing in cur.fetchall():
                identities[listing["provider_symbol"]].append(listing["id"])
            events, unmapped = [], 0
            for item in calendar["items"]:
                candidates = identities.get(item["symbol"], [])
                if len(candidates) != 1:
                    unmapped += 1
                    continue
                events.append({**item, "listing_id": candidates[0]})
            cur.execute("""DELETE FROM qd_market_earnings e USING qd_earnings_listings l
                           WHERE e.listing_id = l.id AND l.market = ? AND e.event_date >= ?""", (market, start))
            cur.execute("""
                INSERT INTO qd_market_earnings (listing_id, event_type, event_date, reporting_period,
                    eps_estimate, revenue_estimate, estimate_currency, date_status, source)
                SELECT r.listing_id, r.event_type, r.event_date, r.reporting_period,
                    r.eps_estimate, r.revenue_estimate, r.estimate_currency, r.date_status, r.source
                FROM jsonb_to_recordset(?::jsonb) AS r(listing_id BIGINT, event_type TEXT, event_date DATE,
                    reporting_period TEXT, eps_estimate NUMERIC, revenue_estimate NUMERIC,
                    estimate_currency TEXT, date_status TEXT, source TEXT)
                ON CONFLICT (listing_id, event_type, event_date) DO NOTHING
            """, (json.dumps(events, default=str),))
            counts = {"raw_listing_count": directory["raw_count"], "listing_count": len(directory["items"]),
                      "excluded_count": directory["excluded_count"], "raw_event_count": calendar["raw_count"],
                      "event_count": len(events), "unmapped_count": unmapped}
            cur.execute("""UPDATE qd_earnings_sync_runs SET status = 'success', finished_at = NOW(),
                           raw_listing_count = ?, listing_count = ?, excluded_count = ?,
                           raw_event_count = ?, event_count = ?, unmapped_count = ? WHERE id = ?""",
                        (*counts.values(), run_id))
            db.commit()
            cur.close()
        except Exception:
            db.rollback()
            raise
    return {"status": "success", **counts}


def sync_market_earnings(*, today=None, directory=fetch_listings, calendar=fetch_calendar):
    today = today or date.today()
    end = today + timedelta(days=90)
    with event_batch_lock(2026100507) as acquired:
        if not acquired:
            return {"skipped": True}
        result = {}
        for market in ("US", "UK"):
            run_id = start_run(market, today, end)
            try:
                listings = directory(market)
                events = calendar(market, today, end)
                result[market] = publish_snapshot(run_id, market, today, end, listings, events)
            except Exception as exc:
                fail_run(run_id)
                logger.warning("market earnings refresh failed market=%s error_type=%s", market, type(exc).__name__)
                result[market] = {"status": "failed"}
        return result
