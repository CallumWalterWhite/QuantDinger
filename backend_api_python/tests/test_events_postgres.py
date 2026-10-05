"""Optional integration proof against PostgreSQL, isolated in a temporary schema.

Run with QD_EVENTS_TEST_DATABASE_URL pointing at a disposable PostgreSQL database.
Every connection uses the real placeholder/RETURNING cursor wrapper without
initializing the application's production connection pool.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
import os
from uuid import uuid4

import pytest

from app.data_providers import earnings_calendar
from app.services import event_digest_repository, events_read, pre_event_digest, upcoming_events, user_preferences
from app.services import market_earnings as market_service
from app.utils.db_postgres import PostgresCursor


TODAY = date(2026, 10, 4)
DIGEST = {
    "headline": "Margins deserve attention", "stance": "neutral", "confidence": 0.4,
    "suggested_action": "watch", "bull_case": [], "bear_case": [],
    "what_to_watch": ["Margins"], "options_note": "", "risks": [],
}


class Connection:
    def __init__(self, raw, cursor_factory):
        self.raw = raw
        self.cursor_factory = cursor_factory

    def cursor(self):
        return PostgresCursor(self.raw.cursor(cursor_factory=self.cursor_factory))

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()


@pytest.fixture
def postgres(monkeypatch):
    dsn = os.getenv("QD_EVENTS_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("QD_EVENTS_TEST_DATABASE_URL is not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor

    schema = "events_test_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    @contextmanager
    def connect():
        raw = psycopg2.connect(dsn)
        try:
            with raw.cursor() as cur:
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            yield Connection(raw, RealDictCursor)
        finally:
            # Roll back open reads and failed writes; only explicit commits persist.
            raw.rollback()
            raw.close()

    try:
        with connect() as db:
            cur = db.cursor()
            cur.execute("""
                CREATE TABLE qd_users (
                    id INTEGER PRIMARY KEY, email TEXT DEFAULT '', notification_settings TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE qd_watchlist (
                    id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES qd_users(id),
                    symbol TEXT NOT NULL, market TEXT NOT NULL, name TEXT DEFAULT ''
                );
                CREATE TABLE qd_manual_positions (
                    id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES qd_users(id),
                    symbol TEXT NOT NULL, market TEXT NOT NULL, name TEXT DEFAULT '', quantity NUMERIC DEFAULT 1
                );
                CREATE TABLE qd_strategy_notifications (
                    id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES qd_users(id), strategy_id INTEGER,
                    symbol TEXT, signal_type TEXT, channels TEXT, title TEXT, message TEXT,
                    payload_json TEXT, created_at TIMESTAMP DEFAULT NOW()
                );
                INSERT INTO qd_users (id) VALUES (1), (2), (3), (4)
            """)
            migrations = Path(__file__).resolve().parents[1] / "migrations"
            for _ in range(2):
                for filename in ("20261001_earnings_digest.sql", "20261004_event_digest_settings.sql", "20261005_market_earnings.sql"):
                    cur.execute((migrations / filename).read_text())
            db.commit()
            cur.close()
        for module in (event_digest_repository, events_read, upcoming_events, user_preferences, market_service):
            monkeypatch.setattr(module, "get_db_connection", connect)
        yield connect
    finally:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        admin.close()


def execute(connect, statement, args=None):
    with connect() as db:
        cur = db.cursor()
        cur.execute(statement, args)
        db.commit()
        cur.close()


def rows(connect, statement, args=None):
    with connect() as db:
        cur = db.cursor()
        cur.execute(statement, args)
        result = cur.fetchall()
        cur.close()
        return result


def event(symbol="GOOGL", offset=0):
    return {"symbol": symbol, "event_type": "earnings", "event_date": TODAY + timedelta(days=offset),
            "eps_estimate": 2.3, "revenue_estimate": 9.4e10, "source": "yfinance"}


def item(user_id=1, symbol="GOOGL", offset=0):
    return {**event(symbol, offset), "user_id": user_id}


def test_calendar_and_digest_queries_isolate_users_and_deduplicate_sources(postgres):
    execute(postgres, """
        INSERT INTO qd_watchlist (user_id, symbol, market, name) VALUES
            (1, ' googl ', 'USStock', 'Alphabet'), (1, 'ETH', 'Crypto', 'Ether'),
            (2, 'MSFT', 'USStock', 'Microsoft');
        INSERT INTO qd_manual_positions (user_id, symbol, market, name) VALUES
            (1, 'GOOGL', 'USStock', 'Alphabet'), (1, 'NVDA', 'USStock', 'Nvidia')
    """)
    for symbol in ("GOOGL", "MSFT", "NVDA", "ETH"):
        upcoming_events.replace_future_events(symbol, event(symbol, 2), today=TODAY)
    mine = events_read.list_upcoming_for_user(1, today=TODAY)
    assert [row["symbol"] for row in mine] == ["GOOGL", "NVDA"]
    assert mine[0]["in_watchlist"] is True and mine[0]["in_positions"] is True
    assert mine[0]["eps_estimate"] == 2.3
    assert [row["symbol"] for row in events_read.list_upcoming_for_user(2, today=TODAY)] == ["MSFT"]
    assert events_read.list_upcoming_for_user(3, today=TODAY) == []
    repo = event_digest_repository.DigestRepository()
    for user_id, symbol in ((1, "GOOGL"), (2, "MSFT")):
        record = repo.prepare(item(user_id, symbol, 2), DIGEST)
        repo.state(record["id"], "browser", "sent")
    assert [row["symbol"] for row in events_read.list_digests_for_user(1)] == ["GOOGL"]
    assert [row["symbol"] for row in events_read.list_digests_for_user(2)] == ["MSFT"]
    assert events_read.list_digests_for_user(3) == []


def test_due_query_honors_zero_seven_default_and_disabled_settings(postgres):
    for user_id in (1, 2, 3, 4):
        execute(postgres, "INSERT INTO qd_watchlist (user_id, symbol, market) VALUES (?, 'GOOGL', 'USStock')",
                (user_id,))
    for offset in (0, 3, 7, 8):
        payload = event(offset=offset)
        execute(postgres, "INSERT INTO qd_upcoming_events (symbol, event_date) VALUES (?, ?)",
                (payload["symbol"], payload["event_date"]))
    events_read.save_digest_settings(1, enabled=True, lead_days=0)
    events_read.save_digest_settings(2, enabled=True, lead_days=7)
    events_read.save_digest_settings(3, enabled=False, lead_days=7)
    due = event_digest_repository.DigestRepository().due_events(TODAY, 3)
    by_user = {user_id: [(row["event_date"] - TODAY).days for row in due if row["user_id"] == user_id]
               for user_id in (1, 2, 3, 4)}
    assert by_user == {1: [0], 2: [0, 3, 7], 3: [], 4: [0, 3]}


def test_settings_commit_and_survive_notification_settings_save(postgres, monkeypatch):
    monkeypatch.setenv("ENABLE_PRE_EVENT_DIGEST", "true")
    events_read.save_digest_settings(1, enabled=False, lead_days=7)
    user_preferences.update_notification_settings(1, {"default_channels": ["browser", "email"], "email": "x@example.test"})
    assert events_read.get_digest_settings(1) == {"enabled": False, "lead_days": 7, "global_enabled": True}
    assert events_read.get_digest_settings(2) == {"enabled": True, "lead_days": 3, "global_enabled": True}
    with pytest.raises(Exception, match="check constraint"):
        execute(postgres, "UPDATE qd_event_digest_settings SET lead_days = 8 WHERE user_id = 1")
    assert events_read.get_digest_settings(1)["lead_days"] == 7


def test_date_replacement_rolls_back_bad_write_and_retains_data_on_provider_failure(postgres):
    upcoming_events.replace_future_events("GOOGL", event(offset=2), today=TODAY)
    upcoming_events.replace_future_events(" googl ", event(offset=5), today=TODAY)
    assert rows(postgres, "SELECT event_date FROM qd_upcoming_events") == [{"event_date": TODAY + timedelta(days=5)}]
    with pytest.raises(Exception):
        upcoming_events.replace_future_events("GOOGL", {**event(offset=6), "event_date": None}, today=TODAY)
    assert rows(postgres, "SELECT event_date FROM qd_upcoming_events") == [{"event_date": TODAY + timedelta(days=5)}]

    def unavailable(*args, **kwargs):
        raise RuntimeError("provider outage")

    summary = upcoming_events.sync_earnings_calendar(today=TODAY, symbols=["GOOGL"], fetch=unavailable,
                                                   sleep=lambda _: None)
    assert summary["failed"] == 1
    assert rows(postgres, "SELECT event_date FROM qd_upcoming_events") == [{"event_date": TODAY + timedelta(days=5)}]
    summary = upcoming_events.sync_earnings_calendar(today=TODAY, symbols=["GOOGL"], fetch=lambda *a, **k: None,
                                                   sleep=lambda _: None)
    assert summary["empty"] == 1
    assert rows(postgres, "SELECT * FROM qd_upcoming_events") == []


def test_silent_yfinance_empty_calendar_preserves_last_successful_event(postgres):
    upcoming_events.replace_future_events("GOOGL", event(offset=5), today=TODAY)
    before = rows(postgres, "SELECT event_date, fetched_at FROM qd_upcoming_events")

    class SilentFailureTicker:
        calendar = {}

    def fetch(symbol, *, today):
        return earnings_calendar.fetch_next_earnings(
            symbol, today=today, ticker_factory=lambda _: SilentFailureTicker())

    summary = upcoming_events.sync_earnings_calendar(today=TODAY, symbols=["GOOGL"], fetch=fetch,
                                                   sleep=lambda _: None)
    assert summary == {"symbols": 1, "updated": 0, "empty": 0, "failed": 1}
    assert rows(postgres, "SELECT event_date, fetched_at FROM qd_upcoming_events") == before


def test_batch_lock_rejects_concurrent_connection_and_releases_on_exception(postgres):
    key = int(uuid4().hex[:15], 16)

    def attempt():
        with upcoming_events.event_batch_lock(key) as acquired:
            return acquired

    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(RuntimeError, match="worker failed"):
            with upcoming_events.event_batch_lock(key) as acquired:
                assert acquired
                assert executor.submit(attempt).result(timeout=10) is False
                raise RuntimeError("worker failed")
        assert executor.submit(attempt).result(timeout=10) is True


def deliver(repo, record, settings):
    return pre_event_digest.deliver_digest(1, symbol="GOOGL", title="Earnings", text="Research", repo=repo,
                                          record=record, settings=settings)


def test_browser_insert_and_success_are_atomic_and_repeated_delivery_is_single(postgres, monkeypatch):
    repo = event_digest_repository.DigestRepository()
    record = repo.prepare(item(), DIGEST)

    def crash(*args, **kwargs):
        raise RuntimeError("crash before success marker")

    with monkeypatch.context() as patch:
        patch.setattr(repo, "_state", crash)
        with pytest.raises(RuntimeError):
            repo.browser(record["id"], 1, "GOOGL", "Earnings", "Research")
    assert rows(postgres, "SELECT * FROM qd_strategy_notifications") == []
    assert repo.get(item())["channels"] == {}
    settings = {"default_channels": ["browser"]}
    assert deliver(repo, repo.get(item()), settings) == {"browser": "sent"}
    assert deliver(repo, repo.get(item()), settings) == {"browser": "sent"}
    assert rows(postgres, "SELECT user_id, signal_type FROM qd_strategy_notifications") == [
        {"user_id": 1, "signal_type": "pre_event_digest"}]
    # The storage boundary also prevents duplication when called directly.
    repo.browser(record["id"], 1, "GOOGL", "Earnings", "Research")
    assert len(rows(postgres, "SELECT * FROM qd_strategy_notifications")) == 1
    assert len(events_read.list_digests_for_user(1)) == 1


def test_browser_commit_acknowledgment_failure_cannot_regress_sent_or_duplicate(postgres, monkeypatch):
    execute(postgres, "INSERT INTO qd_watchlist (user_id, symbol, market) VALUES (1, 'GOOGL', 'USStock')")
    upcoming_events.replace_future_events("GOOGL", event(), today=TODAY)
    repo = event_digest_repository.DigestRepository()
    original_browser = repo.browser
    builds = []

    def committed_then_lost_ack(*args, **kwargs):
        original_browser(*args, **kwargs)
        raise RuntimeError("commit succeeded but acknowledgment was lost")

    def build(evidence):
        builds.append(evidence)
        return DIGEST

    run_kwargs = {"today": TODAY, "repo": repo, "gather": lambda *a, **k: {}, "build": build,
                  "settings_fn": lambda _: {"default_channels": ["browser"]}}
    with monkeypatch.context() as patch:
        patch.setattr(repo, "browser", committed_then_lost_ack)
        pre_event_digest.run_pre_event_digests(**run_kwargs)
    assert repo.get(item())["channels"] == {"browser": "sent"}
    assert len(rows(postgres, "SELECT * FROM qd_strategy_notifications")) == 1
    pre_event_digest.run_pre_event_digests(**run_kwargs)
    assert repo.get(item())["channels"] == {"browser": "sent"}
    assert len(rows(postgres, "SELECT * FROM qd_strategy_notifications")) == 1
    assert len(builds) == 1


def test_failed_channel_retries_without_resending_browser_or_rebuilding_digest(postgres, monkeypatch):
    execute(postgres, "INSERT INTO qd_watchlist (user_id, symbol, market) VALUES (1, 'GOOGL', 'USStock')")
    upcoming_events.replace_future_events("GOOGL", event(), today=TODAY)
    calls = []
    builds = []

    class Notifier:
        def _notify_email(self, **kwargs):
            calls.append(kwargs)
            return (False, "missing_SMTP_HOST") if len(calls) == 1 else (True, "")

    monkeypatch.setattr(pre_event_digest, "SignalNotifier", Notifier)
    settings = {"default_channels": ["browser", "email"], "email": "x@example.test"}

    def build(evidence):
        builds.append(evidence)
        return DIGEST

    for _ in range(3):
        pre_event_digest.run_pre_event_digests(today=TODAY, gather=lambda *a, **k: {}, build=build,
                                              settings_fn=lambda _: settings)
    assert len(calls) == 2
    assert len(builds) == 1
    assert len(rows(postgres, "SELECT * FROM qd_strategy_notifications")) == 1
    assert event_digest_repository.DigestRepository().get(item())["channels"] == {"browser": "sent", "email": "sent"}


@pytest.mark.parametrize("initial", [None, "sending"])
def test_uncertain_external_send_is_not_retried(postgres, monkeypatch, initial):
    repo = event_digest_repository.DigestRepository()
    record = repo.prepare(item(), DIGEST)
    calls = []

    class Notifier:
        def _notify_telegram(self, **kwargs):
            calls.append(kwargs)
            return False, "Read timed out after provider may have accepted the message"

    monkeypatch.setattr(pre_event_digest, "SignalNotifier", Notifier)
    if initial:
        repo.state(record["id"], "telegram", initial)
    settings = {"default_channels": ["telegram"], "telegram_bot_token": "test-token", "telegram_chat_id": "123"}
    for _ in range(2):
        assert deliver(repo, repo.get(item()), settings) == {"telegram": "unknown"}
    assert len(calls) == (0 if initial else 1)
    assert events_read.list_digests_for_user(1) == []


def market_snapshot(market="UK", symbol="TSCO.L", offset=4):
    directory = {"items": [{"market": market, "exchange": "LSE" if market == "UK" else "NMS",
                            "symbol": symbol, "provider_symbol": symbol, "name": "Tesco" if market == "UK" else "Tesla",
                            "instrument_type": "equity_unverified", "segment": "unknown"}],
                 "raw_count": 2, "excluded_count": 1}
    calendar = {"items": [{"symbol": symbol, "event_type": "earnings", "event_date": TODAY + timedelta(days=offset),
                           "reporting_period": "Interim results", "eps_estimate": None, "revenue_estimate": None,
                           "estimate_currency": None, "date_status": "unknown", "source": "yahoo"}], "raw_count": 1}
    return directory, calendar


def publish_market(market="UK", symbol="TSCO.L", offset=4):
    run_id = market_service.start_run(market, TODAY, TODAY + timedelta(days=90))
    market_service.publish_snapshot(run_id, market, TODAY, TODAY + timedelta(days=90),
                                    *market_snapshot(market, symbol, offset))
    return run_id


def test_market_discovery_without_watchlist_filters_pages_and_has_no_personal_data(postgres):
    publish_market()
    publish_market("US", "TSLA", 17)
    result = events_read.list_market_earnings(today=TODAY, page_size=1)
    assert result["total"] == 2 and result["items"][0]["symbol"] == "TSCO.L"
    second = events_read.list_market_earnings(today=TODAY, page_size=1, page=2)
    assert second["items"][0]["symbol"] == "TSLA"
    assert all("user_id" not in r and "in_watchlist" not in r for r in result["items"])
    assert events_read.list_market_earnings(today=TODAY, market="UK", query="tesco")["total"] == 1
    assert events_read.list_market_earnings(today=TODAY, days=7, market="US")["total"] == 0
    assert events_read.list_market_earnings(today=TODAY, query="%" )["total"] == 0
    assert events_read.list_market_earnings(today=TODAY, query="' OR TRUE --")["total"] == 0
    assert events_read.list_market_earnings(today=TODAY, page=1000000)["items"] == []


def test_market_changed_dates_and_empty_snapshot_are_atomic_and_idempotent(postgres):
    publish_market(offset=4)
    publish_market(offset=6)
    assert [r["event_date"] for r in rows(postgres, "SELECT event_date FROM qd_market_earnings")] == [TODAY + timedelta(days=6)]
    run_id = market_service.start_run("UK", TODAY, TODAY + timedelta(days=90))
    directory, _ = market_snapshot()
    market_service.publish_snapshot(run_id, "UK", TODAY, TODAY + timedelta(days=90), directory, {"items": [], "raw_count": 0})
    assert events_read.list_market_earnings(today=TODAY)["total"] == 0
    assert events_read.list_market_earnings(today=TODAY)["coverage"][1]["status"] == "ready"


def test_market_failed_publication_rolls_back_catalog_rows_and_metadata(postgres):
    publish_market()
    run_id = market_service.start_run("UK", TODAY, TODAY + timedelta(days=90))
    directory, calendar = market_snapshot(offset=6)
    directory["items"][0]["name"] = "Changed name"
    calendar["items"][0]["date_status"] = "invalid"
    with pytest.raises(Exception):
        market_service.publish_snapshot(run_id, "UK", TODAY, TODAY + timedelta(days=90), directory, calendar)
    market_service.fail_run(run_id)
    result = events_read.list_market_earnings(today=TODAY, market="UK")
    assert result["items"][0]["name"] == "Tesco" and result["items"][0]["days_until"] == 4
    assert result["coverage"][0]["status"] == "degraded"


def test_market_cache_never_enters_personal_calendar_or_digest_eligibility(postgres):
    publish_market()
    publish_market("US", "TSLA")
    execute(postgres, "INSERT INTO qd_watchlist (user_id, symbol, market, name) VALUES (1, 'TSLA', 'USStock', 'Tesla')")
    assert events_read.list_upcoming_for_user(1, today=TODAY) == []
    assert event_digest_repository.DigestRepository().due_events(TODAY, 7) == []
    assert rows(postgres, "SELECT * FROM qd_strategy_notifications") == []


def test_market_identity_collision_is_not_guessed(postgres):
    directory, calendar = market_snapshot("US", "ABC")
    directory["items"].append({**directory["items"][0], "exchange": "NYQ"})
    directory["raw_count"] = 3
    run_id = market_service.start_run("US", TODAY, TODAY + timedelta(days=90))
    result = market_service.publish_snapshot(run_id, "US", TODAY, TODAY + timedelta(days=90), directory, calendar)
    assert result["unmapped_count"] == 1 and result["event_count"] == 0
    assert len(rows(postgres, "SELECT * FROM qd_earnings_listings")) == 2


def test_market_coverage_unavailable_stale_and_running(postgres):
    assert all(r["status"] == "unavailable" for r in events_read.list_market_earnings(today=TODAY)["coverage"])
    run_id = publish_market()
    execute(postgres, "UPDATE qd_earnings_sync_runs SET finished_at = NOW() - INTERVAL '2 days' WHERE id = ?", (run_id,))
    assert events_read.list_market_earnings(today=TODAY, market="UK")["coverage"][0]["status"] == "stale"
    market_service.start_run("UK", TODAY, TODAY + timedelta(days=90))
    result = events_read.list_market_earnings(today=TODAY, market="UK")
    assert result["coverage"][0]["status"] == "refreshing" and result["total"] == 1
    assert result["coverage"][0]["mode"] == "best_effort" and not result["coverage"][0]["venue_verified"]
    execute(postgres, "UPDATE qd_earnings_sync_runs SET started_at = NOW() - INTERVAL '2 days' WHERE status = 'running'")
    assert events_read.list_market_earnings(today=TODAY, market="UK")["coverage"][0]["status"] == "degraded"


def test_market_lock_is_distributed_and_skips_network_work(postgres):
    with market_service.event_batch_lock(2026100507) as acquired:
        assert acquired
        assert market_service.sync_market_earnings(directory=lambda *_: pytest.fail("provider called")) == {"skipped": True}
