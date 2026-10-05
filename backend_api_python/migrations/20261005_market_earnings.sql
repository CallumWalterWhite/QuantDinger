-- Additive discovery cache. It never changes personal calendar/digest tables.
CREATE TABLE IF NOT EXISTS qd_earnings_listings (
    id BIGSERIAL PRIMARY KEY,
    market VARCHAR(2) NOT NULL CHECK (market IN ('US', 'UK')),
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    provider_symbol VARCHAR(50) NOT NULL,
    name VARCHAR(300) NOT NULL,
    instrument_type VARCHAR(24) NOT NULL DEFAULT 'equity_unverified',
    segment VARCHAR(24) NOT NULL DEFAULT 'unknown',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    catalog_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, exchange, symbol)
);

CREATE TABLE IF NOT EXISTS qd_market_earnings (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    event_type VARCHAR(24) NOT NULL DEFAULT 'earnings',
    event_date DATE NOT NULL,
    reporting_period VARCHAR(200) NOT NULL DEFAULT '',
    eps_estimate NUMERIC NULL,
    revenue_estimate NUMERIC NULL,
    estimate_currency VARCHAR(12) NULL,
    date_status VARCHAR(12) NOT NULL DEFAULT 'unknown' CHECK (date_status IN ('unknown', 'estimated', 'confirmed')),
    source VARCHAR(24) NOT NULL DEFAULT 'yahoo',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (listing_id, event_type, event_date)
);
CREATE INDEX IF NOT EXISTS idx_market_earnings_date ON qd_market_earnings (event_date, listing_id);

CREATE TABLE IF NOT EXISTS qd_earnings_sync_runs (
    id BIGSERIAL PRIMARY KEY,
    market VARCHAR(2) NOT NULL CHECK (market IN ('US', 'UK')),
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    status VARCHAR(12) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    raw_listing_count INTEGER NOT NULL DEFAULT 0,
    listing_count INTEGER NOT NULL DEFAULT 0,
    excluded_count INTEGER NOT NULL DEFAULT 0,
    raw_event_count INTEGER NOT NULL DEFAULT 0,
    event_count INTEGER NOT NULL DEFAULT 0,
    unmapped_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_earnings_sync_market ON qd_earnings_sync_runs (market, id DESC);
