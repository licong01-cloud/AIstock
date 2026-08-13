BEGIN;

CREATE TABLE IF NOT EXISTS app.advisory_forward_run (
    forward_run_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    decision_as_of_trade_date DATE NOT NULL,
    target_trade_date DATE NOT NULL,
    publication_status TEXT NOT NULL,
    settlement_status TEXT NOT NULL,
    selection_run_id TEXT,
    review_run_id TEXT REFERENCES app.advisory_review_run(review_run_id),
    list_version_id TEXT REFERENCES app.advisory_recommendation_list_version(list_version_id),
    active_episode_state_hash TEXT,
    publication_payload_sha256 TEXT,
    settlement_payload_sha256 TEXT,
    last_stage TEXT NOT NULL,
    last_reason_code TEXT,
    last_error_json JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    model_resolution_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    run_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    CONSTRAINT advisory_forward_run_date_order_check
        CHECK (decision_as_of_trade_date < target_trade_date),
    CONSTRAINT advisory_forward_run_publication_status_check
        CHECK (publication_status IN ('PENDING', 'PUBLISHED', 'WAITING_DATA', 'FAILED')),
    CONSTRAINT advisory_forward_run_settlement_status_check
        CHECK (settlement_status IN ('NOT_DUE', 'WAITING_DATA', 'SETTLED', 'NOT_ENTERED', 'FAILED')),
    CONSTRAINT advisory_forward_run_attempt_count_check CHECK (attempt_count > 0),
    CONSTRAINT advisory_forward_run_published_identity_check CHECK (
        publication_status <> 'PUBLISHED'
        OR (
            selection_run_id IS NOT NULL
            AND review_run_id IS NOT NULL
            AND list_version_id IS NOT NULL
            AND publication_payload_sha256 ~ '^[0-9a-f]{64}$'
            AND published_at IS NOT NULL
        )
    ),
    CONSTRAINT advisory_forward_run_settled_identity_check CHECK (
        settlement_status NOT IN ('SETTLED', 'NOT_ENTERED')
        OR (
            publication_status = 'PUBLISHED'
            AND settlement_payload_sha256 ~ '^[0-9a-f]{64}$'
            AND settled_at IS NOT NULL
        )
    ),
    CONSTRAINT advisory_forward_run_active_hash_check CHECK (
        active_episode_state_hash IS NULL OR active_episode_state_hash ~ '^[0-9a-f]{64}$'
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_forward_run_program_target
    ON app.advisory_forward_run(program_id, target_trade_date);
CREATE INDEX IF NOT EXISTS idx_advisory_forward_run_pending_settlement
    ON app.advisory_forward_run(target_trade_date, program_id)
    WHERE publication_status = 'PUBLISHED'
      AND settlement_status IN ('NOT_DUE', 'WAITING_DATA', 'FAILED');

COMMENT ON TABLE app.advisory_forward_run IS 'Advisory-only daily forward publication identity and target-open settlement summary; no orders or cash state.';
COMMENT ON COLUMN app.advisory_forward_run.decision_as_of_trade_date IS 'Closed trading day whose data cutoff produced the recommendation.';
COMMENT ON COLUMN app.advisory_forward_run.target_trade_date IS 'Next authoritative trading day targeted by the recommendation.';
COMMENT ON COLUMN app.advisory_forward_run.publication_status IS 'Baseline publication state; PUBLISHED identifies an immutable recommendation list.';
COMMENT ON COLUMN app.advisory_forward_run.settlement_status IS 'Target-open advisory episode transition state.';
COMMENT ON COLUMN app.advisory_forward_run.model_resolution_json IS 'Model descriptor identity or typed unavailable resolution frozen at publication.';
COMMENT ON COLUMN app.advisory_forward_run.run_payload_json IS 'Canonical publication and settlement summary payload; excludes orders, cash, and future outcome at publication.';

CREATE TABLE IF NOT EXISTS app.advisory_forward_model_observation (
    observation_id TEXT PRIMARY KEY,
    forward_run_id TEXT NOT NULL UNIQUE REFERENCES app.advisory_forward_run(forward_run_id) ON DELETE CASCADE,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    decision_as_of_trade_date DATE NOT NULL,
    target_trade_date DATE NOT NULL,
    status TEXT NOT NULL,
    reason_code TEXT,
    message TEXT,
    package_id TEXT,
    manifest_sha256 TEXT,
    style_profile_id TEXT,
    style_profile_hash TEXT,
    model_descriptor_sha256 TEXT,
    bundle_id TEXT,
    outcome_bundle_id TEXT,
    price_range_bundle_id TEXT,
    feature_schema_version TEXT,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    shortlist_count INTEGER NOT NULL DEFAULT 0,
    maturity_trade_date DATE,
    prediction_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    observation_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT advisory_forward_model_observation_date_order_check
        CHECK (decision_as_of_trade_date < target_trade_date),
    CONSTRAINT advisory_forward_model_observation_status_check
        CHECK (status IN ('EXPERIMENTAL_SHADOW', 'UNAVAILABLE', 'FAILED')),
    CONSTRAINT advisory_forward_model_observation_count_check
        CHECK (candidate_count >= 0 AND shortlist_count >= 0 AND shortlist_count <= candidate_count),
    CONSTRAINT advisory_forward_model_observation_payload_hash_check
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_advisory_forward_model_observation_program_target
    ON app.advisory_forward_model_observation(program_id, target_trade_date DESC);

COMMENT ON TABLE app.advisory_forward_model_observation IS 'Immutable-date Advisory model challenger observation separated from baseline list and trading execution.';
COMMENT ON COLUMN app.advisory_forward_model_observation.status IS 'EXPERIMENTAL_SHADOW, UNAVAILABLE, or FAILED; unavailable is a real forward-day fact.';
COMMENT ON COLUMN app.advisory_forward_model_observation.prediction_payload_json IS 'Top5, outcome, holding-period, and price-range research predictions without realized future outcome.';

DO $$
DECLARE
    relation_count INTEGER;
    required_column_count INTEGER;
    required_constraint_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO relation_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'app'
      AND c.relname IN ('advisory_forward_run', 'advisory_forward_model_observation')
      AND c.relkind = 'r';
    IF relation_count <> 2 THEN
        RAISE EXCEPTION 'advisory forward migration readback failed: expected 2 tables, got %', relation_count;
    END IF;

    SELECT COUNT(*) INTO required_column_count
    FROM information_schema.columns
    WHERE table_schema = 'app'
      AND (
          (table_name = 'advisory_forward_run' AND column_name IN (
              'forward_run_id', 'program_id', 'binding_version_id',
              'decision_as_of_trade_date', 'target_trade_date', 'publication_status',
              'settlement_status', 'publication_payload_sha256', 'settlement_payload_sha256',
              'model_resolution_json', 'run_payload_json'
          ))
          OR
          (table_name = 'advisory_forward_model_observation' AND column_name IN (
              'observation_id', 'forward_run_id', 'model_descriptor_sha256', 'bundle_id',
              'maturity_trade_date', 'prediction_payload_json', 'payload_sha256'
          ))
      );
    IF required_column_count <> 18 THEN
        RAISE EXCEPTION 'advisory forward migration readback failed: expected 18 required columns, got %', required_column_count;
    END IF;

    SELECT COUNT(*) INTO required_constraint_count
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'app'
      AND (
          (r.relname = 'advisory_forward_run' AND c.conname IN (
              'advisory_forward_run_pkey',
              'advisory_forward_run_date_order_check',
              'advisory_forward_run_publication_status_check',
              'advisory_forward_run_settlement_status_check',
              'advisory_forward_run_published_identity_check',
              'advisory_forward_run_settled_identity_check'
          ))
          OR
          (r.relname = 'advisory_forward_model_observation' AND c.conname IN (
              'advisory_forward_model_observation_pkey',
              'advisory_forward_model_observation_forward_run_id_key',
              'advisory_forward_model_observation_status_check',
              'advisory_forward_model_observation_payload_hash_check'
          ))
      );
    IF required_constraint_count <> 10 THEN
        RAISE EXCEPTION 'advisory forward migration readback failed: expected 10 required constraints, got %', required_constraint_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'app'
          AND tablename = 'advisory_forward_run'
          AND indexname = 'ux_advisory_forward_run_program_target'
          AND indexdef LIKE 'CREATE UNIQUE INDEX% (program_id, target_trade_date)%'
    ) THEN
        RAISE EXCEPTION 'advisory forward migration readback failed: program/target unique index is missing or incompatible';
    END IF;
END $$;

COMMIT;
