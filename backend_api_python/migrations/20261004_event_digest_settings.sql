-- migrations/20261004_event_digest_settings.sql
CREATE TABLE IF NOT EXISTS qd_event_digest_settings (
    user_id INTEGER PRIMARY KEY REFERENCES qd_users(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    lead_days INTEGER NOT NULL DEFAULT 3 CHECK (lead_days BETWEEN 0 AND 7),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
