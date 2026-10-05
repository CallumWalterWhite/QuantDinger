-- Additive public-market jobs. Legacy universe jobs and snapshot rows are unchanged.
ALTER TABLE qd_fundamental_snapshots ADD COLUMN IF NOT EXISTS net_income_ttm DOUBLE PRECISION;
CREATE INDEX IF NOT EXISTS idx_research_financial_read
    ON qd_fundamental_snapshots(market,symbol,(metadata_json->>'exchange'),period_end DESC,ingested_at DESC,id DESC)
    WHERE LEFT(source,15)='research_yahoo_';
CREATE INDEX IF NOT EXISTS idx_research_directory_provider
    ON qd_earnings_listings(market,provider_symbol) WHERE is_active;
CREATE TABLE IF NOT EXISTS qd_research_ingestion_jobs (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL CHECK (market IN ('US', 'UK')),
    dataset TEXT NOT NULL DEFAULT 'financials' CHECK (dataset = 'financials'),
    requester_id BIGINT NOT NULL REFERENCES qd_users(id),
    request_keys TEXT[] NOT NULL,
    catalog_run_id BIGINT REFERENCES qd_earnings_sync_runs(id),
    retry_job_id BIGINT REFERENCES qd_research_ingestion_jobs(id),
    incremental BOOLEAN NOT NULL DEFAULT TRUE,
    fields_json JSONB NOT NULL,
    expected_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','expanding','running','complete','partial','failed')),
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_ingestion_active
    ON qd_research_ingestion_jobs(market,dataset) WHERE status IN ('queued','expanding','running');
CREATE INDEX IF NOT EXISTS idx_research_ingestion_requests ON qd_research_ingestion_jobs USING GIN(request_keys);

CREATE TABLE IF NOT EXISTS qd_research_ingestion_items (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES qd_research_ingestion_jobs(id),
    listing_id BIGINT NOT NULL REFERENCES qd_earnings_listings(id),
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','success','unavailable','failed','skipped')),
    attempts INTEGER NOT NULL DEFAULT 0,
    token TEXT,
    lease_until TIMESTAMPTZ,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error TEXT NOT NULL DEFAULT '',
    coverage_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(job_id,listing_id)
);
CREATE INDEX IF NOT EXISTS idx_research_ingestion_pending ON qd_research_ingestion_items(status,retry_at,id);
CREATE INDEX IF NOT EXISTS idx_research_ingestion_listing ON qd_research_ingestion_items(listing_id,updated_at DESC,id DESC);

CREATE TABLE IF NOT EXISTS qd_research_ingestion_schedules (
    market TEXT PRIMARY KEY CHECK (market IN ('US','UK')),
    dataset TEXT NOT NULL DEFAULT 'financials' CHECK (dataset = 'financials'),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    requester_id BIGINT REFERENCES qd_users(id),
    fields_json JSONB NOT NULL DEFAULT '["revenue","net_income","shareholder_equity","total_debt","free_cash_flow","shares_outstanding"]'::jsonb,
    next_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cooldown_until TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_job_id BIGINT REFERENCES qd_research_ingestion_jobs(id)
);
INSERT INTO qd_research_ingestion_schedules(market) VALUES ('US'),('UK') ON CONFLICT DO NOTHING;
