# tests/test_earnings_digest_migration.py
from pathlib import Path

MIGRATION = Path(__file__).resolve().parent.parent / "migrations" / "20261001_earnings_digest.sql"


def test_migration_defines_both_tables_idempotently():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS qd_upcoming_events" in sql
    assert "CREATE TABLE IF NOT EXISTS qd_event_digests" in sql
    assert "UNIQUE (symbol, event_type, event_date)" in sql
    assert "UNIQUE (user_id, symbol, event_type, event_date)" in sql


def test_migration_is_registered_in_db_bootstrap():
    db_py = Path(__file__).resolve().parent.parent / "app" / "utils" / "db.py"
    text = db_py.read_text(encoding="utf-8")
    assert "earnings-digest-20261001" in text
    assert "20261001_earnings_digest.sql" in text
