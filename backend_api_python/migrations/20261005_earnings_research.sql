-- Advisory evidence only. No changes to legacy financials, digests or orders.
CREATE TABLE IF NOT EXISTS qd_research_evidence (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    kind TEXT NOT NULL CHECK (kind IN ('prices','news','documents')),
    provider TEXT NOT NULL,
    source_url TEXT NOT NULL DEFAULT '',
    published_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash TEXT NOT NULL,
    payload JSONB NOT NULL CHECK (octet_length(payload::text) <= 300000),
    UNIQUE(listing_id,kind,provider,content_hash)
);
CREATE INDEX IF NOT EXISTS idx_research_evidence_cutoff ON qd_research_evidence(listing_id,kind,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS qd_research_observations (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    kind TEXT NOT NULL,
    evidence_id BIGINT NOT NULL REFERENCES qd_research_evidence(id),
    identity_json JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collection_key TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_research_observations_cutoff ON qd_research_observations(listing_id,kind,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS qd_research_price_bars (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    session_date DATE NOT NULL,
    provider TEXT NOT NULL,
    currency TEXT NOT NULL,
    adjustment TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash TEXT NOT NULL,
    payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_price_cutoff ON qd_research_price_bars(listing_id,session_date DESC,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS qd_earnings_research_jobs (
    id BIGSERIAL PRIMARY KEY,
    requester_id BIGINT NOT NULL REFERENCES qd_users(id),
    request_key TEXT NOT NULL UNIQUE,
    market TEXT NOT NULL CHECK (market IN ('all','US','UK')),
    days INTEGER NOT NULL CHECK (days BETWEEN 1 AND 90),
    catalog_versions JSONB NOT NULL DEFAULT '{}',
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    expected_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('running','complete','partial','cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS qd_earnings_research_items (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES qd_earnings_research_jobs(id),
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    -- The calendar replaces its cached rows on refresh. Capture the reference
    -- as history, not an FK that would prevent normal calendar publication.
    event_id BIGINT NOT NULL,
    event_date DATE NOT NULL,
    identity_json JSONB NOT NULL,
    stages JSONB NOT NULL DEFAULT '{"prices":{"status":"pending","attempts":0},"news":{"status":"pending","attempts":0},"documents":{"status":"pending","attempts":0}}',
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','complete','partial','unsupported','superseded','cancelled')),
    retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cutoff TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(job_id,listing_id,event_id)
);
CREATE INDEX IF NOT EXISTS idx_earnings_research_claim ON qd_earnings_research_items(status,retry_at,job_id);
CREATE TABLE IF NOT EXISTS qd_earnings_research_lease (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    token TEXT,
    item_id BIGINT REFERENCES qd_earnings_research_items(id),
    kind TEXT,
    lease_until TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_market TEXT NOT NULL DEFAULT 'UK'
);
INSERT INTO qd_earnings_research_lease(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;
-- Immutable administrative attestations, bound to the listing identity.
CREATE TABLE IF NOT EXISTS qd_research_issuer_sources (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    market TEXT NOT NULL,
    exchange TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    identity_json JSONB NOT NULL,
    issuer_url TEXT NOT NULL,
    verified_by BIGINT NOT NULL REFERENCES qd_users(id),
    verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_research_issuer_source ON qd_research_issuer_sources(listing_id,verified_at DESC,id DESC);
