-- BUG-423 Phase 2 rollback: recreate empty retired draft/cache tables only.
-- This does not restore dropped rows. Run with psql --single-transaction -v ON_ERROR_STOP=1.

CREATE TABLE IF NOT EXISTS assistant_issue_candidates (
    candidate_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    severity TEXT NOT NULL,
    module TEXT NOT NULL,
    problem_statement TEXT NOT NULL,
    reproduce_command TEXT,
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    dedupe_key TEXT,
    status TEXT NOT NULL DEFAULT 'needs_review',
    github_issue_number INTEGER,
    github_issue_url TEXT,
    github_sync_status TEXT NOT NULL DEFAULT 'not_requested',
    github_sync_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_by TEXT NOT NULL DEFAULT 'assistant',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_aic_status CHECK (status IN ('draft','needs_review','approved_for_github','rejected','synced_to_github','duplicate')),
    CONSTRAINT uq_aic_dedupe UNIQUE (dedupe_key)
);

CREATE TABLE IF NOT EXISTS assistant_validation_discovery_reports (
    discovery_report_id TEXT PRIMARY KEY,
    run_date DATE NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_issue_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation_run_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aic_status_updated
    ON assistant_issue_candidates(status, updated_at DESC);

COMMENT ON TABLE assistant_issue_candidates IS 'Non-authoritative conversation draft / explanation cache; retired by BUG-423 Phase 2. Formal submission must use AIstock issue workflow / Validation MCP.';
COMMENT ON TABLE assistant_validation_discovery_reports IS 'Non-authoritative conversation draft / explanation cache; retired by BUG-423 Phase 2. Discovery facts come from Validation/Nightly candidate sources.';
