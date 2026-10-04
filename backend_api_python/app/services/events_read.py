# app/services/events_read.py
"""Per-user read access to upcoming events, digest history and digest settings."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Any

from app.utils.db import get_db_connection

DEFAULT_LEAD_DAYS = 3

_UPCOMING_SQL = """
SELECT e.symbol, MAX(u.name) AS name, e.event_type, e.event_date, e.eps_estimate, e.revenue_estimate, e.fetched_at,
       BOOL_OR(u.src = 'watchlist') AS in_watchlist, BOOL_OR(u.src = 'position') AS in_positions
FROM qd_upcoming_events e
JOIN (
    SELECT UPPER(TRIM(symbol)) AS symbol, name, 'watchlist' AS src
    FROM qd_watchlist WHERE user_id = ? AND market = 'USStock'
    UNION ALL
    SELECT UPPER(TRIM(symbol)) AS symbol, name, 'position' AS src
    FROM qd_manual_positions WHERE user_id = ? AND market = 'USStock'
) u ON u.symbol = e.symbol
WHERE e.event_date BETWEEN ? AND ?
GROUP BY e.symbol, e.event_type, e.event_date, e.eps_estimate, e.revenue_estimate, e.fetched_at
ORDER BY e.event_date, e.symbol
"""

_DIGESTS_SQL = """
SELECT symbol, event_type, event_date, digest_json, channels_json, sent_at AS created_at
FROM qd_event_digests
WHERE user_id = ? AND sent_at IS NOT NULL
ORDER BY sent_at DESC
LIMIT ?
"""

_SETTINGS_GET_SQL = "SELECT enabled, lead_days FROM qd_event_digest_settings WHERE user_id = ?"

_SETTINGS_UPSERT_SQL = """
INSERT INTO qd_event_digest_settings (user_id, enabled, lead_days, updated_at)
VALUES (?, ?, ?, NOW())
ON CONFLICT (user_id) DO UPDATE SET
    enabled = EXCLUDED.enabled, lead_days = EXCLUDED.lead_days, updated_at = NOW()
RETURNING user_id
"""


def _float(value: Any) -> float | None:
    return None if value is None else float(value)


def _json_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _iso(value: Any) -> str:
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value or "")


def _global_enabled() -> bool:
    return os.getenv("ENABLE_PRE_EVENT_DIGEST", "false").strip().lower() in {"1", "true", "yes", "on"}


def list_upcoming_for_user(user_id: int, *, days: int = 30, today: date | None = None) -> list[dict]:
    today = today or date.today()
    days = min(max(int(days), 1), 90)
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_UPCOMING_SQL, (user_id, user_id, today, today + timedelta(days=days)))
        rows = cur.fetchall() or []
        cur.close()
    return [
        {
            "symbol": r["symbol"],
            "name": r.get("name") or "",
            "event_type": r["event_type"],
            "event_date": r["event_date"].isoformat(),
            "days_until": (r["event_date"] - today).days,
            "eps_estimate": _float(r.get("eps_estimate")),
            "revenue_estimate": _float(r.get("revenue_estimate")),
            "in_watchlist": bool(r.get("in_watchlist")),
            "in_positions": bool(r.get("in_positions")),
            "fetched_at": _iso(r.get("fetched_at")),
            "stale": not r.get("fetched_at") or (today - r["fetched_at"].date()).days > 1,
        }
        for r in rows
    ]


def list_digests_for_user(user_id: int, *, limit: int = 50) -> list[dict]:
    limit = min(max(int(limit), 1), 200)
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_DIGESTS_SQL, (user_id, limit))
        rows = cur.fetchall() or []
        cur.close()
    return [
        {
            "symbol": r["symbol"],
            "event_type": r["event_type"],
            "event_date": _iso(r["event_date"]),
            "digest": _json_dict(r.get("digest_json")),
            "channels": _json_dict(r.get("channels_json")),
            "created_at": _iso(r.get("created_at")),
        }
        for r in rows
    ]


def get_digest_settings(user_id: int) -> dict:
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_SETTINGS_GET_SQL, (user_id,))
        row = cur.fetchone()
        cur.close()
    return {
        "enabled": bool(row["enabled"]) if row else True,
        "lead_days": int(row["lead_days"]) if row else DEFAULT_LEAD_DAYS,
        "global_enabled": _global_enabled(),
    }


def save_digest_settings(user_id: int, *, enabled: bool, lead_days: int) -> dict:
    if not isinstance(enabled, bool):
        raise ValueError("invalid_enabled")
    if isinstance(lead_days, bool) or not isinstance(lead_days, int) or not 0 <= lead_days <= 7:
        raise ValueError("invalid_lead_days")
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(_SETTINGS_UPSERT_SQL, (user_id, bool(enabled), lead_days))
        db.commit()
        cur.close()
    return {"enabled": bool(enabled), "lead_days": lead_days, "global_enabled": _global_enabled()}
