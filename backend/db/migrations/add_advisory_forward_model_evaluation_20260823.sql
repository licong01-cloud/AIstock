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
COMMENT ON COLUMN app.advisory_forward_model_observation.evaluation_reason_code IS
    'Typed current model-evaluation blocker or failure code; NULL after READY.';
COMMENT ON COLUMN app.advisory_forward_model_observation.evaluation_error_json IS
    'Structured current evaluation error context; contains no prediction replacement or secret values.';
COMMENT ON COLUMN app.advisory_forward_model_observation.evaluated_at IS
    'Timestamp when an immutable mature outcome was committed; NULL while immature, waiting, or failed.';

COMMENT ON COLUMN app.advisory_forward_model_evaluation.evaluation_id IS 'Stable immutable evaluation snapshot identity.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.schema_version IS 'Payload contract version advisory_forward_model_evaluation_v1.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.program_id IS 'Advisory Program whose challenger epoch is evaluated.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.model_descriptor_sha256 IS 'Exact P0-D runtime descriptor SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.bundle_id IS 'Exact meta-label bundle content identity.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.shadow_policy_sha256 IS 'Canonical frozen transition-policy SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.cost_policy_sha256 IS 'Canonical frozen cost-policy SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.first_observation_id IS 'First observation of the continuous descriptor epoch.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.last_due_observation_id IS 'Latest due observation included at this watermark.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.first_target_trade_date IS 'First target trading date in the epoch replay.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.as_of_trade_date IS 'Inclusive PIT market-data watermark for this snapshot.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.last_due_maturity_trade_date IS 'Latest included observation maturity trading date.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.observation_count IS 'Total epoch observations visible at the watermark.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.due_observation_count IS 'Observations whose maturity is not later than the watermark.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.matured_outcome_count IS 'Due observations with immutable MATURED or NO_ENTRY outcomes.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.observation_roster_sha256 IS 'Canonical epoch observation and rank-context roster SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.selection_input_sha256 IS 'Canonical persisted Selection Top40 input SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.market_input_sha256 IS 'Canonical bounded market-source rows SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.metrics_json IS 'advisory_forward_model_metrics_v1 summary; completed hit rate excludes active/censored episodes.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.result_payload_json IS 'advisory_forward_model_evaluation_v1 replay evidence containing bounded daily and episode facts.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.payload_sha256 IS 'Canonical immutable evaluation payload SHA256 excluding created_at.';
COMMENT ON COLUMN app.advisory_forward_model_evaluation.created_at IS 'Database-independent UTC creation timestamp supplied by the evaluator.';

COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.outcome_id IS 'Stable immutable per-observation outcome identity.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.schema_version IS 'Payload contract version advisory_forward_model_observation_outcome_v1.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.observation_id IS 'Unique natural P0-D forward observation being labeled.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.evaluation_id IS 'Evaluation snapshot that first committed this outcome.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.program_id IS 'Advisory Program owning the observation.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.model_descriptor_sha256 IS 'Exact P0-D runtime descriptor SHA256.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.bundle_id IS 'Exact meta-label bundle content identity.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.target_trade_date IS 'Observation target trading date.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.maturity_trade_date IS 'Earliest trading date on which the observation may be evaluated.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.status IS 'MATURED when every entered episode exited, otherwise factual NO_ENTRY.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.entered_episode_count IS 'Episodes opened from this observation decision.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.exited_episode_count IS 'Fully exited episodes attributed to this observation.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.completed_episode_hit_rate IS 'Fraction of exited episodes with net_return_bps greater than zero; NULL for NO_ENTRY.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.mean_net_return_bps IS 'Mean completed episode net return in basis points; NULL for NO_ENTRY.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.outcome_payload_json IS 'advisory_forward_model_observation_outcome_v1 decision dates, status, and attributed episode facts.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.payload_sha256 IS 'Canonical immutable outcome payload SHA256 excluding created_at.';
COMMENT ON COLUMN app.advisory_forward_model_observation_outcome.created_at IS 'Database-independent UTC creation timestamp supplied by the evaluator.';

CREATE OR REPLACE FUNCTION app.reject_advisory_forward_model_evaluation_fact_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable advisory forward model evaluation facts cannot be updated or deleted'
        USING ERRCODE = '55000';
END;
$$;

COMMENT ON FUNCTION app.reject_advisory_forward_model_evaluation_fact_mutation() IS
    'Rejects UPDATE and DELETE against immutable forward model evaluation and outcome facts.';

DROP TRIGGER IF EXISTS trg_reject_advisory_forward_model_evaluation_mutation
    ON app.advisory_forward_model_evaluation;
CREATE TRIGGER trg_reject_advisory_forward_model_evaluation_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_forward_model_evaluation
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_forward_model_evaluation_fact_mutation();

DROP TRIGGER IF EXISTS trg_reject_advisory_forward_model_outcome_mutation
    ON app.advisory_forward_model_observation_outcome;
CREATE TRIGGER trg_reject_advisory_forward_model_outcome_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_forward_model_observation_outcome
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_forward_model_evaluation_fact_mutation();

DO $$
DECLARE
    table_count INTEGER;
    column_count INTEGER;
    index_count INTEGER;
    constraint_count INTEGER;
    trigger_count INTEGER;
    function_count INTEGER;
    column_comment_count INTEGER;
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

    SELECT COUNT(*) INTO index_count
    FROM pg_indexes
    WHERE schemaname='app' AND indexname IN (
      'ux_advisory_forward_model_evaluation_epoch_asof',
      'idx_advisory_forward_model_evaluation_program_latest',
      'idx_advisory_forward_model_outcome_program_maturity'
    );
    IF index_count <> 3 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 3 indexes, got %', index_count;
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
        'advisory_forward_model_observation_outcome_date_check',
        'advisory_forward_model_observation_outcome_count_check',
        'advisory_forward_model_observation_outcome_metric_check',
        'advisory_forward_model_observation_outcome_hash_check'
    );
    IF constraint_count <> 11 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 11 constraints, got %', constraint_count;
    END IF;

    SELECT COUNT(*) INTO trigger_count
    FROM pg_trigger
    WHERE NOT tgisinternal
      AND tgname IN (
        'trg_reject_advisory_forward_model_evaluation_mutation',
        'trg_reject_advisory_forward_model_outcome_mutation'
      );
    IF trigger_count <> 2 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 2 immutable triggers, got %', trigger_count;
    END IF;

    SELECT COUNT(*) INTO function_count
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='app'
      AND p.proname='reject_advisory_forward_model_evaluation_fact_mutation';
    IF function_count <> 1 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected immutable trigger function';
    END IF;

    SELECT COUNT(*) INTO column_comment_count
    FROM pg_attribute attribute
    JOIN pg_class relation ON relation.oid=attribute.attrelid
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    JOIN pg_description description
      ON description.objoid=relation.oid AND description.objsubid=attribute.attnum
    WHERE namespace.nspname='app' AND attribute.attnum > 0
      AND (
        relation.relname IN (
          'advisory_forward_model_evaluation',
          'advisory_forward_model_observation_outcome'
        )
        OR (
          relation.relname='advisory_forward_model_observation'
          AND attribute.attname IN (
            'evaluation_status','evaluation_reason_code',
            'evaluation_error_json','evaluated_at'
          )
        )
      );
    IF column_comment_count <> 43 THEN
        RAISE EXCEPTION 'forward model evaluation migration readback expected 43 column comments, got %', column_comment_count;
    END IF;
END $$;

COMMIT;
