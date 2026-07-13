CREATE TABLE IF NOT EXISTS app.advisory_research_batch (
    batch_id TEXT PRIMARY KEY,
    request_id UUID NOT NULL,
    batch_key TEXT NOT NULL UNIQUE,
    decision_trade_date DATE NOT NULL,
    program_ids JSONB NOT NULL,
    data_source TEXT NOT NULL CHECK (data_source = 'DB_HISTORICAL'),
    origin TEXT NOT NULL CHECK (origin = 'MANUAL_HISTORICAL_RESEARCH'),
    request_payload_sha256 TEXT NOT NULL,
    research_scope TEXT NOT NULL CHECK (research_scope = 'HISTORICAL_RESEARCH_ONLY'),
    execution_prohibited BOOLEAN NOT NULL CHECK (execution_prohibited IS TRUE),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'WAITING_INPUT', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app.advisory_research_program_run (
    program_run_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id),
    decision_trade_date DATE NOT NULL,
    research_scope TEXT NOT NULL CHECK (research_scope = 'HISTORICAL_RESEARCH_ONLY'),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'WAITING_INPUT', 'FAILED')),
    program_payload_sha256 TEXT,
    binding_version_id TEXT,
    binding_payload_hash TEXT,
    package_id TEXT,
    manifest_sha256 TEXT,
    policy_hash TEXT,
    effective_runtime_config_hash TEXT,
    source_watermark_hash TEXT,
    evidence_id TEXT,
    evidence_hash TEXT,
    artifact_id TEXT,
    artifact_payload_hash TEXT,
    research_list_version_id TEXT,
    research_candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    candidate_outcome TEXT,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (program_id, decision_trade_date, research_scope)
);

CREATE TABLE IF NOT EXISTS app.advisory_research_batch_receipt (
    receipt_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE REFERENCES app.advisory_research_batch(batch_id),
    batch_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'WAITING_INPUT', 'FAILED')),
    receipt_hash TEXT NOT NULL UNIQUE,
    program_run_ids JSONB NOT NULL,
    receipt_payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_advisory_research_program_run_date
    ON app.advisory_research_program_run(decision_trade_date DESC, status, program_id);

COMMENT ON TABLE app.advisory_research_batch IS
    'Manual historical research batch only. DB_HISTORICAL, MANUAL_HISTORICAL_RESEARCH and execution_prohibited=true are mandatory; no scheduler, broker, order, account, cash or position fields are allowed.';
COMMENT ON COLUMN app.advisory_research_batch.batch_key IS
    'Deterministic hash of historical decision date, sorted Program ids, DB_HISTORICAL, manual origin and research scope. Equivalent requests reuse the same batch.';
COMMENT ON COLUMN app.advisory_research_batch.execution_prohibited IS
    'Always true. This table cannot represent a trading instruction, position target, order or broker action.';
COMMENT ON TABLE app.advisory_research_program_run IS
    'One immutable or recoverable historical research result per Program/date/scope. Different completed input payloads for the same business key must fail closed.';
COMMENT ON COLUMN app.advisory_research_program_run.research_candidates_json IS
    'Research-only candidate projection containing symbol, rank, score, stock name and component scores. It excludes price, target, order, account, cash, position, broker and execution fields.';
COMMENT ON COLUMN app.advisory_research_program_run.source_watermark_hash IS
    'Hash of the persisted v2 source evidence consumed by this historical research result.';
COMMENT ON TABLE app.advisory_research_batch_receipt IS
    'Deterministic aggregate receipt referencing Program-run identities. It may advance only while the batch is PENDING, RUNNING or WAITING_INPUT; terminal payload conflicts fail closed.';
