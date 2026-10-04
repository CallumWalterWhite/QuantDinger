# app/services/upcoming_events.py
"""Upcoming-event calendar for symbols users watch or hold."""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date
from typing import Any, Callable

from app.data_providers.earnings_calendar import fetch_next_earnings, normalize_symbol
from app.utils.db import get_db_connection
from app.utils.logger import get_logger

logger = get_logger(__name__)


@contextmanager
def event_batch_lock(key: int):
    """Use a dedicated transaction; rollback releases the lock before pooling."""
    with get_db_connection() as db:
        try:
            cur = db.cursor()
            cur.execute("SELECT pg_try_advisory_xact_lock(?) AS acquired", (key,))
            row = cur.fetchone()
            cur.close()
            yield bool(row and row["acquired"])
        finally:
            db.rollback()

_TRACKED_SQL = """
SELECT symbol FROM qd_watchlist WHERE market = 'USStock'
UNION
SELECT symbol FROM qd_manual_positions WHERE market = 'USStock'
"""

_DELETE_SQL = """
DELETE FROM qd_upcoming_events
WHERE symbol = ? AND event_type = 'earnings' AND event_date >= ?
"""

_INSERT_SQL = """
INSERT INTO qd_upcoming_events
    (symbol, market, event_type, event_date, eps_estimate, revenue_estimate, source, fetched_at)
VALUES (?, 'USStock', ?, ?, ?, ?, ?, NOW())
ON CONFLICT (symbol, event_type, event_date) DO UPDATE SET
    eps_estimate = EXCLUDED.eps_estimate,
    revenue_estimate = EXCLUDED.revenue_estimate,
    fetched_at = NOW()
"""


def list_tracked_symbols() -> list[str]:
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_TRACKED_SQL)
        rows = cur.fetchall() or []
        cur.close()
    return sorted({s for s in (normalize_symbol(r.get("symbol")) for r in rows) if s})


def replace_future_events(symbol: str, event: dict[str, Any] | None, *, today: date) -> None:
    """Drop stale future rows for ``symbol`` and store the fresh one (dates can move)."""
    symbol = normalize_symbol(symbol)
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_DELETE_SQL, (symbol, today))
        if event is not None:
            cur.execute(
                _INSERT_SQL,
                (
                    symbol,
                    event["event_type"],
                    event["event_date"],
                    event.get("eps_estimate"),
                    event.get("revenue_estimate"),
                    event.get("source", "yfinance"),
                ),
            )
        db.commit()
        cur.close()


def _sync_earnings_calendar(
    *,
    today: date | None = None,
    symbols: list[str] | None = None,
    fetch: Callable[..., dict[str, Any] | None] = fetch_next_earnings,
    sleep: Callable[[float], None] = time.sleep,
    delay_sec: float = 0.5,
) -> dict[str, int]:
    today = today or date.today()
    symbols = list_tracked_symbols() if symbols is None else symbols
    summary = {"symbols": len(symbols), "updated": 0, "empty": 0, "failed": 0}
    for symbol in symbols:
        try:
            event = fetch(symbol, today=today)
            replace_future_events(symbol, event, today=today)
            summary["updated" if event else "empty"] += 1
        except Exception as exc:
            summary["failed"] += 1
            logger.warning("earnings calendar sync failed for %s: %s", symbol, exc)
        sleep(delay_sec)
    return summary


def sync_earnings_calendar(**kwargs) -> dict:
    with event_batch_lock(2026100101) as acquired:
        if not acquired:
            return {"skipped": True, "reason": "already_running"}
        return _sync_earnings_calendar(**kwargs)
