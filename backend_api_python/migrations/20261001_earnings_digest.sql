-- migrations/20261001_earnings_digest.sql
CREATE TABLE IF NOT EXISTS qd_upcoming_events (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    market VARCHAR(50) NOT NULL DEFAULT 'USStock',
    event_type VARCHAR(24) NOT NULL DEFAULT 'earnings',
    event_date DATE NOT NULL,
    eps_estimate DECIMAL(20, 6),
    revenue_estimate DECIMAL(24, 2),
    source VARCHAR(24) NOT NULL DEFAULT 'yfinance',
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, event_type, event_date)
);

CREATE INDEX IF NOT EXISTS idx_upcoming_events_date
    ON qd_upcoming_events(event_date, event_type);

CREATE TABLE IF NOT EXISTS qd_event_digests (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
    symbol VARCHAR(50) NOT NULL,
    event_type VARCHAR(24) NOT NULL DEFAULT 'earnings',
    event_date DATE NOT NULL,
    digest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    channels_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    sent_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, symbol, event_type, event_date)
);

CREATE INDEX IF NOT EXISTS idx_event_digests_user
    ON qd_event_digests(user_id, sent_at DESC) WHERE sent_at IS NOT NULL;
