# tests/test_events_read.py
from datetime import date, datetime

import pytest

from app.services import events_read as module


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, args=None):
        self.db.statements.append((" ".join(sql.split()), args))

    def fetchall(self):
        return self.db.rows

    def fetchone(self):
        return self.db.rows[0] if self.db.rows else None

    def close(self):
        pass


class FakeDB:
    def __init__(self, rows=None):
        self.statements = []
        self.rows = rows or []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _use(monkeypatch, db):
    monkeypatch.setattr(module, "get_db_connection", lambda: db)
    return db


def test_upcoming_is_scoped_to_user_and_serialized(monkeypatch):
    db = _use(monkeypatch, FakeDB(rows=[{
        "symbol": "GOOGL", "name": "Alphabet", "event_type": "earnings", "event_date": date(2026, 10, 27),
        "eps_estimate": 2.3, "revenue_estimate": 9.4e10, "in_watchlist": True, "in_positions": False,
    }]))
    rows = module.list_upcoming_for_user(7, days=30, today=date(2026, 10, 4))
    sql, args = db.statements[0]
    assert "user_id = ?" in sql
    assert args == (7, 7, date(2026, 10, 4), date(2026, 11, 3))
    assert rows == [{
        "symbol": "GOOGL", "name": "Alphabet", "event_type": "earnings", "event_date": "2026-10-27",
        "days_until": 23, "eps_estimate": 2.3, "revenue_estimate": 94000000000.0,
        "in_watchlist": True, "in_positions": False,
        "fetched_at": "", "stale": True,
    }]


@pytest.mark.parametrize("days,expected_end", [(0, date(2026, 10, 5)), (500, date(2027, 1, 2))])
def test_upcoming_clamps_days(monkeypatch, days, expected_end):
    db = _use(monkeypatch, FakeDB())
    module.list_upcoming_for_user(7, days=days, today=date(2026, 10, 4))
    assert db.statements[0][1][3] == expected_end


def test_digests_are_scoped_and_parse_json_strings(monkeypatch):
    db = _use(monkeypatch, FakeDB(rows=[{
        "symbol": "GOOGL", "event_type": "earnings", "event_date": date(2026, 10, 27),
        "digest_json": '{"headline": "h"}', "channels_json": {"browser": True},
        "created_at": datetime(2026, 10, 24, 9, 0),
    }]))
    rows = module.list_digests_for_user(7, limit=5000)
    sql, args = db.statements[0]
    assert "WHERE user_id = ?" in sql
    assert args == (7, 200)
    assert rows[0]["digest"] == {"headline": "h"}
    assert rows[0]["channels"] == {"browser": True}
    assert rows[0]["event_date"] == "2026-10-27"
    assert rows[0]["created_at"] == "2026-10-24T09:00:00"


def test_settings_default_when_no_row(monkeypatch):
    _use(monkeypatch, FakeDB(rows=[]))
    monkeypatch.setenv("ENABLE_PRE_EVENT_DIGEST", "false")
    assert module.get_digest_settings(7) == {"enabled": True, "lead_days": 3, "global_enabled": False}


def test_save_settings_upserts_and_validates(monkeypatch):
    db = _use(monkeypatch, FakeDB())
    monkeypatch.setenv("ENABLE_PRE_EVENT_DIGEST", "true")
    result = module.save_digest_settings(7, enabled=False, lead_days=5)
    assert db.statements[0][0].startswith("INSERT INTO qd_event_digest_settings")
    assert db.statements[0][1] == (7, False, 5)
    assert db.commits == 1
    assert result == {"enabled": False, "lead_days": 5, "global_enabled": True}


def test_market_calendar_never_rolls_back_an_outer_write_transaction(monkeypatch):
    db = _use(monkeypatch, FakeDB())
    db.rollback_only = False
    with pytest.raises(RuntimeError, match="independent_connection"):
        module.list_market_earnings()
    assert db.rollback_only is False
    assert db.statements == []


@pytest.mark.parametrize("bad", [-1, 8, "3", 2.5, None, True])
def test_save_settings_rejects_bad_lead_days(monkeypatch, bad):
    _use(monkeypatch, FakeDB())
    with pytest.raises(ValueError, match="invalid_lead_days"):
        module.save_digest_settings(7, enabled=True, lead_days=bad)
