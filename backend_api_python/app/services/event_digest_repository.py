"""Durable per-channel digest delivery state, serialized by the batch lock."""

from __future__ import annotations

import json
from datetime import date

from app.utils.db import get_db_connection


def json_dict(value):
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}


class DigestRepository:
    def due_events(self, today: date, lead_days: int) -> list[dict]:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute("""
                SELECT u.user_id, e.symbol, e.event_type, e.event_date,
                       e.eps_estimate, e.revenue_estimate
                FROM qd_upcoming_events e
                JOIN (
                    SELECT user_id, UPPER(TRIM(symbol)) AS symbol FROM qd_watchlist WHERE market = 'USStock'
                    UNION
                    SELECT user_id, UPPER(TRIM(symbol)) AS symbol FROM qd_manual_positions WHERE market = 'USStock'
                ) u ON u.symbol = e.symbol
                LEFT JOIN qd_event_digest_settings s ON s.user_id = u.user_id
                WHERE e.event_type = 'earnings' AND e.market = 'USStock'
                  AND e.event_date >= ?
                  AND (e.event_date - ?::date) <= COALESCE(s.lead_days, ?)
                  AND COALESCE(s.enabled, TRUE)
                ORDER BY e.event_date, e.symbol, u.user_id
            """, (today, today, min(7, max(0, lead_days))))
            rows = cur.fetchall() or []
            cur.close()
        return [dict(row) for row in rows]

    def get(self, item) -> dict | None:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute("""
                SELECT id, digest_json, channels_json FROM qd_event_digests
                WHERE user_id = ? AND symbol = ? AND event_type = ? AND event_date = ?
            """, self.key(item))
            row = cur.fetchone()
            cur.close()
        if not row:
            return None
        return {"id": row["id"], "digest": json_dict(row["digest_json"]),
                "channels": json_dict(row["channels_json"])}

    @staticmethod
    def key(item):
        return (item["user_id"], item["symbol"], item["event_type"], item["event_date"])

    def prepare(self, item, digest) -> dict:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute("""
                INSERT INTO qd_event_digests (user_id, symbol, event_type, event_date, digest_json)
                VALUES (?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (user_id, symbol, event_type, event_date) DO NOTHING RETURNING id
            """, (*self.key(item), json.dumps(digest, default=str)))
            db.commit()
            cur.close()
        return self.get(item)

    def state(self, digest_id: int, channel: str, state: str) -> None:
        with get_db_connection() as db:
            self._state(db, digest_id, channel, state)
            db.commit()

    @staticmethod
    def _state(db, digest_id, channel, state):
        cur = db.cursor()
        cur.execute("""
            UPDATE qd_event_digests
            SET channels_json = channels_json || jsonb_build_object(?::text, ?::text),
                sent_at = CASE WHEN ? = 'sent' THEN COALESCE(sent_at, NOW()) ELSE sent_at END
            WHERE id = ? AND (COALESCE(channels_json ->> ?::text, '') <> 'sent' OR ? = 'sent')
        """, (channel, state, state, digest_id, channel, state))
        cur.close()

    def begin(self, digest_id: int, channel: str) -> bool:
        """Claim before sending. Never reclaim a successful or uncertain send."""
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute("""
                UPDATE qd_event_digests
                SET channels_json = channels_json || jsonb_build_object(?::text, 'sending'::text)
                WHERE id = ? AND COALESCE(channels_json ->> ?::text, 'failed') = 'failed'
                RETURNING id
            """, (channel, digest_id, channel))
            claimed = bool(cur.fetchone())
            db.commit()
            cur.close()
        return claimed

    def browser(self, digest_id: int, user_id: int, symbol: str, title: str, text: str) -> None:
        """The notification and its success marker commit together."""
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute("SELECT channels_json FROM qd_event_digests WHERE id = ? FOR UPDATE", (digest_id,))
            row = cur.fetchone()
            if not row or json_dict(row["channels_json"]).get("browser") == "sent":
                cur.close()
                return
            cur.execute("""
                INSERT INTO qd_strategy_notifications
                    (user_id, strategy_id, symbol, signal_type, channels, title, message, payload_json, created_at)
                VALUES (?, NULL, ?, 'pre_event_digest', 'browser', ?, ?, ?::jsonb, NOW()) RETURNING id
            """, (user_id, symbol, title[:255], text,
                  json.dumps({"kind": "pre_event_digest", "digest_id": digest_id})))
            cur.close()
            self._state(db, digest_id, "browser", "sent")
            db.commit()
