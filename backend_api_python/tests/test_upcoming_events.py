# tests/test_upcoming_events.py
from datetime import date

from app.services import upcoming_events as module

TODAY = date(2026, 10, 1)


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, args=None):
        self.db.statements.append((" ".join(sql.split()), args))

    def fetchall(self):
        return self.db.rows

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


def test_list_tracked_symbols_normalizes_and_dedupes(monkeypatch):
    db = FakeDB(rows=[{"symbol": " googl "}, {"symbol": "GOOGL"}, {"symbol": "aapl"}, {"symbol": ""}])
    monkeypatch.setattr(module, "get_db_connection", lambda: db)
    assert module.list_tracked_symbols() == ["AAPL", "GOOGL"]
    assert "qd_watchlist" in db.statements[0][0]
    assert "qd_manual_positions" in db.statements[0][0]


def test_replace_future_events_deletes_stale_then_inserts(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(module, "get_db_connection", lambda: db)
    event = {
        "symbol": "GOOGL", "event_type": "earnings", "event_date": date(2026, 10, 27),
        "eps_estimate": 2.3, "revenue_estimate": 9.4e10, "source": "yfinance",
    }
    module.replace_future_events("GOOGL", event, today=TODAY)
    assert db.statements[0][0].startswith("DELETE FROM qd_upcoming_events")
    assert db.statements[0][1] == ("GOOGL", TODAY)
    assert db.statements[1][0].startswith("INSERT INTO qd_upcoming_events")
    assert db.commits == 1


def test_replace_future_events_with_none_only_deletes(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(module, "get_db_connection", lambda: db)
    module.replace_future_events("SPY", None, today=TODAY)
    assert len(db.statements) == 1
    assert db.commits == 1


def test_sync_counts_and_survives_per_symbol_failures(monkeypatch):
    written = []
    monkeypatch.setattr(module, "replace_future_events", lambda s, e, *, today: written.append((s, e)))

    def fetch(symbol, *, today=None):
        if symbol == "BAD":
            raise RuntimeError("boom")
        if symbol == "SPY":
            return None
        return {"symbol": symbol, "event_type": "earnings", "event_date": date(2026, 10, 27),
                "eps_estimate": None, "revenue_estimate": None, "source": "yfinance"}

    result = module._sync_earnings_calendar(
        today=TODAY, symbols=["GOOGL", "BAD", "SPY"], fetch=fetch, sleep=lambda _: None,
    )
    assert result == {"symbols": 3, "updated": 1, "empty": 1, "failed": 1}
    assert [s for s, _ in written] == ["GOOGL", "SPY"]
