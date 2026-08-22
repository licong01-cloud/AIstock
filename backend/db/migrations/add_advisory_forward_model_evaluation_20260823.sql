BEGIN;

ALTER TABLE app.advisory_forward_model_observation
    ADD COLUMN IF NOT EXISTS evaluation_status TEXT NOT NULL DEFAULT 'EVIDENCE_IMMATURE',
    ADD COLUMN IF NOT EXISTS evaluation_reason_code TEXT,
    ADD COLUMN IF NOT EXISTS evaluation_error_json JSONB,
    ADD COLUMN IF NOT EXISTS evaluated_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'advisory_forward_model_observation_evaluation_status_check'
          AND conrelid = 'app.advisory_forward_model_observation'::regclass
    ) THEN
        ALTER TABLE app.advisory_forward_model_observation
            ADD CONSTRAINT advisory_forward_model_observation_evaluation_status_check
            CHECK (evaluation_status IN ('EVIDENCE_IMMATURE','WAITING_DATA','FAILED','READY'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS app.advisory_forward_model_evaluation (
    evaluation_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    model_descriptor_sha256 TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    shadow_policy_sha256 TEXT NOT NULL,
    cost_policy_sha256 TEXT NOT NULL,
    first_observation_id TEXT NOT NULL REFERENCES app.advisory_forward_model_observation(observation_id),
    last_due_observation_id TEXT NOT NULL REFERENCES app.advisory_forward_model_observation(observation_id),
    first_target_trade_date DATE NOT NULL,
    as_of_trade_date DATE NOT NULL,
    last_due_maturity_trade_date DATE NOT NULL,
    observation_count INTEGER NOT NULL,
    due_observation_count INTEGER NOT NULL,
    matured_outcome_count INTEGER NOT NULL,
    observation_roster_sha256 TEXT NOT NULL,
    selection_input_sha256 TEXT NOT NULL,
    market_input_sha256 TEXT NOT NULL,
    metrics_json JSONB NOT NULL,
    result_payload_json JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT advisory_forward_model_evaluation_schema_check
        CHECK (schema_version = 'advisory_forward_model_evaluation_v1'),
    CONSTRAINT advisory_forward_model_evaluation_date_check
        CHECK (first_target_trade_date <= last_due_maturity_trade_date
               AND last_due_maturity_trade_date <= as_of_trade_date),
    CONSTRAINT advisory_forward_model_evaluation_count_check
        CHECK (observation_count > 0 AND due_observation_count > 0
               AND matured_outcome_count >= 0
               AND due_observation_count <= observation_count
               AND matured_outcome_count <= due_observation_count),
    CONSTRAINT advisory_forward_model_evaluation_hash_check
        CHECK (model_descriptor_sha256 ~ '^[0-9a-f]{64}$'
               AND bundle_id ~ '^[0-9a-f]{64}$'
               AND shadow_policy_sha256 ~ '^[0-9a-f]{64}$'
               AND cost_policy_sha256 ~ '^[0-9a-f]{64}$'
               AND observation_roster_sha256 ~ '^[0-9a-f]{64}$'
               AND selection_input_sha256 ~ '^[0-9a-f]{64}$'
               AND market_input_sha256 ~ '^[0-9a-f]{64}$'
               AND payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_forward_model_evaluation_epoch_asof
    ON app.advisory_forward_model_evaluation(
        program_id, model_descriptor_sha256, first_observation_id, as_of_trade_date
    );
CREATE INDEX IF NOT EXISTS idx_advisory_forward_model_evaluation_program_latest
    ON app.advisory_forward_model_evaluation(program_id, as_of_trade_date DESC);

CREATE TABLE IF NOT EXISTS app.advisory_forward_model_observation_outcome (
    outcome_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    observation_id TEXT NOT NULL UNIQUE REFERENCES app.advisory_forward_model_observation(observation_id) ON DELETE CASCADE,
    evaluation_id TEXT NOT NULL REFERENCES app.advisory_forward_model_evaluation(evaluation_id) ON DELETE RESTRICT,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    model_descriptor_sha256 TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    target_trade_date DATE NOT NULL,
    maturity_trade_date DATE NOT NULL,
    status TEXT NOT NULL,
    entered_episode_count INTEGER NOT NULL,
    exited_episode_count INTEGER NOT NULL,
    completed_episode_hit_rate DOUBLE PRECISION,
    mean_net_return_bps DOUBLE PRECISION,
    outcome_payload_json JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT advisory_forward_model_observation_outcome_schema_check
        CHECK (schema_version = 'advisory_forward_model_observation_outcome_v1'),
    CONSTRAINT advisory_forward_model_observation_outcome_status_check
        CHECK (status IN ('MATURED','NO_ENTRY')),
    CONSTRAINT advisory_forward_model_observation_outcome_date_check
        CHECK (target_trade_date <= maturity_trade_date),
    CONSTRAINT advisory_forward_model_observation_outcome_count_check
        CHECK (entered_episode_count >= 0 AND exited_episode_count >= 0
               AND exited_episode_count <= entered_episode_count
               AND ((status = 'NO_ENTRY' AND entered_episode_count = 0 AND exited_episode_count = 0)
                    OR (status = 'MATURED' AND entered_episode_count > 0
                        AND exited_episode_count = entered_episode_count))),
    CONSTRAINT advisory_forward_model_observation_outcome_metric_check
        CHECK ((completed_episode_hit_rate IS NULL OR completed_episode_hit_rate BETWEEN 0 AND 1)
               AND (status <> 'NO_ENTRY' OR (completed_episode_hit_rate IS NULL AND mean_net_return_bps IS NULL))),
    CONSTRAINT advisory_forward_model_observation_outcome_hash_check
        CHECK (model_descriptor_sha256 ~ '^[0-9a-f]{64}$'
               AND bundle_id ~ '^[0-9a-f]{64}$'
               AND payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_advisory_forward_model_outcome_program_maturity
    ON app.advisory_forward_model_observation_outcome(program_id, maturity_trade_date DESC);

COMMENT ON TABLE app.advisory_forward_model_evaluation IS
    'Immutable P0-D challenger portfolio evaluation snapshot isolated from baseline Advisory Program metrics.';
COMMENT ON TABLE app.advisory_forward_model_observation_outcome IS
    'Immutable mature policy outcome for one natural P0-D forward observation; active/censored observations are not inserted.';
COMMENT ON COLUMN app.advisory_forward_model_observation.evaluation_status IS
    'Independent model-evaluation lifecycle; does not change the immutable prediction payload.';

DO $$
DECLARE
    table_count INTEGER;
    column_count INTEGER;
    constraint_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='app'
      AND c.relname IN ('advisory_forward_model_evaluation','advisory_forward_model_observation_outcome')
      AND c.relkind='r';
    IF table_count <> 2 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 2 tables, got %', table_count;
    END IF;

    SELECT COUNT(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema='app'
      AND table_name='advisory_forward_model_observation'
      AND column_name IN ('evaluation_status','evaluation_reason_code','evaluation_error_json','evaluated_at');
    IF column_count <> 4 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 4 observation columns, got %', column_count;
    END IF;

    SELECT COUNT(*) INTO constraint_count
    FROM pg_constraint c
    WHERE c.conname IN (
        'advisory_forward_model_observation_evaluation_status_check',
        'advisory_forward_model_evaluation_schema_check',
        'advisory_forward_model_evaluation_date_check',
        'advisory_forward_model_evaluation_count_check',
        'advisory_forward_model_evaluation_hash_check',
        'advisory_forward_model_observation_outcome_schema_check',
        'advisory_forward_model_observation_outcome_status_check',
        'advisory_forward_model_observation_outcome_count_check',
        'advisory_forward_model_observation_outcome_hash_check'
    );
    IF constraint_count <> 9 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 9 constraints, got %', constraint_count;
    END IF;
END $$;

COMMIT;
