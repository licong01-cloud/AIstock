CREATE SCHEMA IF NOT EXISTS app;

ALTER TABLE app.advisory_program
    DROP CONSTRAINT IF EXISTS advisory_program_package_mode_check;

ALTER TABLE app.advisory_program
    ADD CONSTRAINT advisory_program_package_mode_check
    CHECK (package_mode IN ('single_package', 'fusion_pool', 'weighted_rank_fusion', 'union', 'intersection', 'sleeve_mode_future'));

COMMENT ON COLUMN app.advisory_program.package_mode IS 'StrategyPackage binding mode: single_package, fusion_pool, weighted_rank_fusion, union, intersection, or design-reserved sleeve_mode_future.';

CREATE TABLE IF NOT EXISTS app.advisory_strategy_binding_version (
    binding_version_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    package_mode TEXT NOT NULL,
    package_ids JSONB NOT NULL,
    package_weights JSONB NOT NULL,
    fusion_method TEXT,
    package_set_hash TEXT NOT NULL,
    fusion_policy_sha256 TEXT,
    runtime_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    effective_from_trade_date DATE,
    effective_to_trade_date DATE,
    activation_status TEXT NOT NULL,
    activation_reason TEXT,
    source_replay_run_id TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    binding_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_binding_mode_check CHECK (
        package_mode IN ('single_package', 'fusion_pool', 'weighted_rank_fusion', 'union', 'intersection', 'sleeve_mode_future')
    ),
    CONSTRAINT advisory_binding_status_check CHECK (
        activation_status IN ('DRAFT', 'ACTIVE', 'RETIRED')
    )
);

CREATE INDEX IF NOT EXISTS idx_advisory_binding_program_created
    ON app.advisory_strategy_binding_version(program_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_binding_one_active
    ON app.advisory_strategy_binding_version(program_id)
    WHERE activation_status = 'ACTIVE';

COMMENT ON TABLE app.advisory_strategy_binding_version IS 'Versioned StrategyPackage binding for a long-running Advisory Program; package changes do not redefine the recommendation list identity.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.binding_version_id IS 'Stable binding version id for one package mode/package set/weight configuration.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.program_id IS 'Advisory Program that owns this StrategyPackage binding version.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.program_version IS 'Advisory Program config version active when this binding was created.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.package_mode IS 'Binding mode: single_package, fusion_pool, weighted_rank_fusion, union, intersection, or design-reserved sleeve_mode_future.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.package_ids IS 'JSON array of StrategyPackage ids used by this binding.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.package_weights IS 'JSON object of package weights; keys match package_ids.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.fusion_method IS 'Fusion method resolved by the advisory layer, such as weighted_rank_fusion, union, or intersection.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.package_set_hash IS 'SHA256 hash of binding package mode and package ids.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.fusion_policy_sha256 IS 'SHA256 hash of fusion method and package weights; NULL for pure single-package bindings.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.runtime_config_json IS 'Optional runtime config captured with this binding, including replay draft settings when applicable.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.effective_from_trade_date IS 'First trade date when the binding should be used, if operator supplied one.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.effective_to_trade_date IS 'Last trade date before this binding was retired, if known.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.activation_status IS 'Binding activation status: DRAFT, ACTIVE, or RETIRED.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.activation_reason IS 'Operator or system reason for creating or activating the binding.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.source_replay_run_id IS 'Optional replay run id reviewed by an operator before applying this binding.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.created_by IS 'Operator or system actor that created the binding.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.created_at IS 'Binding creation timestamp.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.activated_at IS 'Timestamp when the binding became ACTIVE; NULL for DRAFT bindings.';
COMMENT ON COLUMN app.advisory_strategy_binding_version.binding_payload_json IS 'Full binding payload for forward-compatible readback.';

INSERT INTO app.advisory_strategy_binding_version (
    binding_version_id, program_id, program_version, package_mode, package_ids,
    package_weights, fusion_method, package_set_hash, fusion_policy_sha256,
    runtime_config_json, effective_from_trade_date, effective_to_trade_date,
    activation_status, activation_reason, source_replay_run_id, created_by,
    created_at, activated_at, binding_payload_json
)
SELECT
    'advb_backfill_' || md5(p.program_id || ':' || p.version::text),
    p.program_id,
    p.version,
    p.package_mode,
    p.package_ids,
    p.package_weights,
    p.fusion_method,
    p.package_set_hash,
    p.fusion_policy_sha256,
    '{}'::jsonb,
    NULL::date,
    NULL::date,
    'ACTIVE',
    'backfilled active binding from advisory_program',
    NULL::text,
    p.created_by,
    COALESCE(p.created_at, NOW()),
    COALESCE(p.enabled_since, p.created_at, NOW()),
    jsonb_build_object(
        'binding_version_id', 'advb_backfill_' || md5(p.program_id || ':' || p.version::text),
        'program_id', p.program_id,
        'program_version', p.version,
        'package_mode', p.package_mode,
        'package_ids', p.package_ids,
        'package_weights', p.package_weights,
        'fusion_method', p.fusion_method,
        'package_set_hash', p.package_set_hash,
        'fusion_policy_sha256', p.fusion_policy_sha256,
        'runtime_config_json', '{}'::jsonb,
        'effective_from_trade_date', NULL,
        'effective_to_trade_date', NULL,
        'activation_status', 'ACTIVE',
        'activation_reason', 'backfilled active binding from advisory_program',
        'source_replay_run_id', NULL,
        'created_by', p.created_by,
        'created_at', COALESCE(p.created_at, NOW()),
        'activated_at', COALESCE(p.enabled_since, p.created_at, NOW())
    )
FROM app.advisory_program p
WHERE NOT EXISTS (
    SELECT 1
    FROM app.advisory_strategy_binding_version b
    WHERE b.program_id = p.program_id
      AND b.activation_status = 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS app.advisory_review_run (
    review_run_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    trade_date DATE NOT NULL,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    data_source TEXT NOT NULL,
    selection_run_id TEXT,
    selection_run_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    runtime_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_json JSONB,
    created_by TEXT,
    run_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_review_run_type_check CHECK (run_type IN ('PREVIEW', 'RUN', 'REPLAY')),
    CONSTRAINT advisory_review_run_status_check CHECK (status IN ('SUCCEEDED', 'WAITING_DATA', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_advisory_review_run_program_date
    ON app.advisory_review_run(program_id, trade_date DESC, started_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_review_run_one_run_per_program_date
    ON app.advisory_review_run(program_id, trade_date)
    WHERE run_type = 'RUN';

COMMENT ON TABLE app.advisory_review_run IS 'One execution record for an advisory preview, official daily review, or replay day.';
COMMENT ON COLUMN app.advisory_review_run.review_run_id IS 'Stable id for one advisory review execution.';
COMMENT ON COLUMN app.advisory_review_run.program_id IS 'Advisory Program reviewed.';
COMMENT ON COLUMN app.advisory_review_run.binding_version_id IS 'Strategy binding version used by this review execution.';
COMMENT ON COLUMN app.advisory_review_run.trade_date IS 'Trade date being reviewed.';
COMMENT ON COLUMN app.advisory_review_run.run_type IS 'Review run type: PREVIEW, RUN, or REPLAY.';
COMMENT ON COLUMN app.advisory_review_run.status IS 'Execution status: SUCCEEDED, WAITING_DATA, or FAILED.';
COMMENT ON COLUMN app.advisory_review_run.data_source IS 'Market and selection data source used by the review.';
COMMENT ON COLUMN app.advisory_review_run.selection_run_id IS 'Primary Selection Center run id if one run produced the candidates.';
COMMENT ON COLUMN app.advisory_review_run.selection_run_ids IS 'All Selection Center run ids used by this review.';
COMMENT ON COLUMN app.advisory_review_run.runtime_config_json IS 'Runtime config used for selection and advisory review.';
COMMENT ON COLUMN app.advisory_review_run.started_at IS 'Review execution start timestamp.';
COMMENT ON COLUMN app.advisory_review_run.finished_at IS 'Review execution finish timestamp.';
COMMENT ON COLUMN app.advisory_review_run.error_json IS 'Structured error context for failed review executions.';
COMMENT ON COLUMN app.advisory_review_run.created_by IS 'Operator or system actor that created the run.';
COMMENT ON COLUMN app.advisory_review_run.run_payload_json IS 'Full review run payload for forward-compatible readback.';

CREATE TABLE IF NOT EXISTS app.advisory_recommendation_list_version (
    list_version_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    review_run_id TEXT NOT NULL REFERENCES app.advisory_review_run(review_run_id),
    trade_date DATE NOT NULL,
    previous_list_version_id TEXT REFERENCES app.advisory_recommendation_list_version(list_version_id),
    version_status TEXT NOT NULL,
    target_count INTEGER NOT NULL,
    active_count INTEGER NOT NULL,
    entered_count INTEGER NOT NULL,
    held_count INTEGER NOT NULL,
    exited_count INTEGER NOT NULL,
    waiting_count INTEGER NOT NULL,
    changed_count INTEGER NOT NULL,
    turnover_rate DOUBLE PRECISION,
    overlap_rate DOUBLE PRECISION,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    list_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_list_version_status_check CHECK (version_status IN ('PREVIEW', 'PUBLISHED', 'REPLAY'))
);

CREATE INDEX IF NOT EXISTS idx_advisory_list_version_program_date
    ON app.advisory_recommendation_list_version(program_id, trade_date DESC, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_list_version_one_published_per_program_date
    ON app.advisory_recommendation_list_version(program_id, trade_date)
    WHERE version_status = 'PUBLISHED';

COMMENT ON TABLE app.advisory_recommendation_list_version IS 'Daily version of the advisory recommendation list; independent from the StrategyPackage identity.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.list_version_id IS 'Stable id for one recommendation list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.program_id IS 'Advisory Program that owns this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.binding_version_id IS 'Strategy binding version used to produce this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.review_run_id IS 'Review run that produced this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.trade_date IS 'Trade date of this recommendation list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.previous_list_version_id IS 'Previous list version used for diff and continuity display.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.version_status IS 'List version status: PREVIEW, PUBLISHED, or REPLAY.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.target_count IS 'Target active recommendation count for this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.active_count IS 'Number of ACTIVE items in this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.entered_count IS 'Number of ENTER decisions in this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.held_count IS 'Number of HOLD decisions in this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.exited_count IS 'Number of EXIT decisions in this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.waiting_count IS 'Number of WAITING decisions in this list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.changed_count IS 'Number of changed item decisions counted for UI diff.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.turnover_rate IS 'Display-only turnover rate; not a hard investment gate.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.overlap_rate IS 'Display-only active-symbol overlap rate versus the previous list.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.summary_json IS 'List diff and diagnostic summary for UI display.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.created_at IS 'List version creation timestamp.';
COMMENT ON COLUMN app.advisory_recommendation_list_version.list_payload_json IS 'Full list version payload for forward-compatible readback.';

CREATE TABLE IF NOT EXISTS app.advisory_recommendation_list_item (
    list_item_id TEXT PRIMARY KEY,
    list_version_id TEXT NOT NULL REFERENCES app.advisory_recommendation_list_version(list_version_id) ON DELETE CASCADE,
    program_id TEXT NOT NULL,
    binding_version_id TEXT NOT NULL,
    episode_id TEXT,
    symbol TEXT NOT NULL,
    item_state TEXT NOT NULL,
    action TEXT NOT NULL,
    previous_action TEXT,
    rank INTEGER,
    score DOUBLE PRECISION,
    previous_rank INTEGER,
    previous_score DOUBLE PRECISION,
    entry_price NUMERIC,
    exit_price NUMERIC,
    price_basis TEXT,
    effective_trade_date DATE,
    reason_code TEXT NOT NULL,
    operation_advice_json JSONB NOT NULL,
    component_scores_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    item_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_list_item_state_check CHECK (item_state IN ('ACTIVE', 'EXITED', 'WAITING', 'WATCH')),
    CONSTRAINT advisory_list_item_action_check CHECK (
        action IN ('ENTER', 'HOLD', 'EXIT', 'REDUCE', 'ADD', 'WAITING', 'SKIP', 'UNTRADABLE', 'WATCH')
    )
);

CREATE INDEX IF NOT EXISTS idx_advisory_list_item_list_rank
    ON app.advisory_recommendation_list_item(list_version_id, rank ASC NULLS LAST, symbol ASC);

CREATE INDEX IF NOT EXISTS idx_advisory_list_item_program_symbol
    ON app.advisory_recommendation_list_item(program_id, symbol, created_at DESC);

COMMENT ON TABLE app.advisory_recommendation_list_item IS 'Item-level advisory recommendation decision in one list version, including explicit operation advice.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.list_item_id IS 'Stable id for one item in one recommendation list version.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.list_version_id IS 'Recommendation list version that owns this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.program_id IS 'Advisory Program that owns this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.binding_version_id IS 'Strategy binding version used to produce this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.episode_id IS 'Advisory episode affected by this item, if one exists.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.symbol IS 'A-share symbol in the recommendation list.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.item_state IS 'Item state for display: ACTIVE, EXITED, WAITING, or WATCH.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.action IS 'Operation action such as ENTER, HOLD, EXIT, WAITING, UNTRADABLE, or WATCH.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.previous_action IS 'Previous list action for this symbol, if available.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.rank IS 'Current rank used for this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.score IS 'Current score used for this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.previous_rank IS 'Previous list rank for this symbol, if available.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.previous_score IS 'Previous list score for this symbol, if available.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.entry_price IS 'Entry price basis value for ENTER or active items, if available.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.exit_price IS 'Exit price basis value for EXIT items, if available.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.price_basis IS 'Price basis used by the operation advice.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.effective_trade_date IS 'Trade date when the advice becomes effective.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.reason_code IS 'Reason code explaining the operation action.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.operation_advice_json IS 'Human-readable operation advice payload for UI display; advisory only, not an order.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.component_scores_json IS 'Per-package or component scores used for this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.evidence_json IS 'Review-time evidence used to create this item.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.created_at IS 'Item creation timestamp.';
COMMENT ON COLUMN app.advisory_recommendation_list_item.item_payload_json IS 'Full item payload for forward-compatible readback.';

ALTER TABLE app.advisory_daily_review
    ADD COLUMN IF NOT EXISTS binding_version_id TEXT,
    ADD COLUMN IF NOT EXISTS review_run_id TEXT,
    ADD COLUMN IF NOT EXISTS list_version_id TEXT;

COMMENT ON COLUMN app.advisory_daily_review.binding_version_id IS 'Strategy binding version used by this advisory daily review decision, when produced by list-version-aware review.';
COMMENT ON COLUMN app.advisory_daily_review.review_run_id IS 'Review run id that produced this advisory daily review decision, when available.';
COMMENT ON COLUMN app.advisory_daily_review.list_version_id IS 'Recommendation list version id that contains this advisory daily review decision, when available.';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'advisory_daily_review_binding_version_fkey'
    ) THEN
        ALTER TABLE app.advisory_daily_review
            ADD CONSTRAINT advisory_daily_review_binding_version_fkey
            FOREIGN KEY (binding_version_id)
            REFERENCES app.advisory_strategy_binding_version(binding_version_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'advisory_daily_review_review_run_fkey'
    ) THEN
        ALTER TABLE app.advisory_daily_review
            ADD CONSTRAINT advisory_daily_review_review_run_fkey
            FOREIGN KEY (review_run_id)
            REFERENCES app.advisory_review_run(review_run_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'advisory_daily_review_list_version_fkey'
    ) THEN
        ALTER TABLE app.advisory_daily_review
            ADD CONSTRAINT advisory_daily_review_list_version_fkey
            FOREIGN KEY (list_version_id)
            REFERENCES app.advisory_recommendation_list_version(list_version_id);
    END IF;
END $$;

WITH grouped AS (
    SELECT
        r.program_id,
        r.trade_date,
        b.binding_version_id,
        CASE
            WHEN bool_or(COALESCE(r.review_status, 'SUCCEEDED') = 'WAITING_DATA') THEN 'WAITING_DATA'
            ELSE 'SUCCEEDED'
        END AS status
    FROM app.advisory_daily_review r
    JOIN app.advisory_strategy_binding_version b
      ON b.program_id = r.program_id
     AND b.activation_status = 'ACTIVE'
    WHERE r.program_id IS NOT NULL
    GROUP BY r.program_id, r.trade_date, b.binding_version_id
)
INSERT INTO app.advisory_review_run (
    review_run_id, program_id, binding_version_id, trade_date, run_type,
    status, data_source, selection_run_id, selection_run_ids, runtime_config_json,
    started_at, finished_at, error_json, created_by, run_payload_json
)
SELECT
    'advrun_backfill_' || md5(g.program_id || ':' || g.trade_date::text),
    g.program_id,
    g.binding_version_id,
    g.trade_date,
    'RUN',
    g.status,
    'BACKFILLED_DAILY_REVIEW',
    NULL::text,
    '[]'::jsonb,
    jsonb_build_object('backfill_source', 'advisory_daily_review'),
    NOW(),
    NOW(),
    NULL::jsonb,
    'migration_20260608',
    jsonb_build_object(
        'review_run_id', 'advrun_backfill_' || md5(g.program_id || ':' || g.trade_date::text),
        'program_id', g.program_id,
        'binding_version_id', g.binding_version_id,
        'trade_date', g.trade_date,
        'run_type', 'RUN',
        'status', g.status,
        'data_source', 'BACKFILLED_DAILY_REVIEW',
        'selection_run_id', NULL,
        'selection_run_ids', '[]'::jsonb,
        'runtime_config_json', jsonb_build_object('backfill_source', 'advisory_daily_review'),
        'created_by', 'migration_20260608'
    )
FROM grouped g
WHERE NOT EXISTS (
    SELECT 1
    FROM app.advisory_review_run rr
    WHERE rr.review_run_id = 'advrun_backfill_' || md5(g.program_id || ':' || g.trade_date::text)
);

WITH grouped AS (
    SELECT
        r.program_id,
        r.trade_date,
        b.binding_version_id,
        rr.review_run_id,
        p.target_count,
        count(*) FILTER (WHERE r.action NOT IN ('EXIT', 'WAITING')) AS active_count,
        count(*) FILTER (WHERE r.action = 'ENTER') AS entered_count,
        count(*) FILTER (WHERE r.action = 'HOLD') AS held_count,
        count(*) FILTER (WHERE r.action = 'EXIT') AS exited_count,
        count(*) FILTER (WHERE r.action = 'WAITING') AS waiting_count
    FROM app.advisory_daily_review r
    JOIN app.advisory_program p
      ON p.program_id = r.program_id
    JOIN app.advisory_strategy_binding_version b
      ON b.program_id = r.program_id
     AND b.activation_status = 'ACTIVE'
    JOIN app.advisory_review_run rr
      ON rr.review_run_id = 'advrun_backfill_' || md5(r.program_id || ':' || r.trade_date::text)
    WHERE r.program_id IS NOT NULL
    GROUP BY r.program_id, r.trade_date, b.binding_version_id, rr.review_run_id, p.target_count
)
INSERT INTO app.advisory_recommendation_list_version (
    list_version_id, program_id, binding_version_id, review_run_id, trade_date,
    previous_list_version_id, version_status, target_count, active_count,
    entered_count, held_count, exited_count, waiting_count, changed_count,
    turnover_rate, overlap_rate, summary_json, created_at, list_payload_json
)
SELECT
    'advlv_backfill_' || md5(g.program_id || ':' || g.trade_date::text),
    g.program_id,
    g.binding_version_id,
    g.review_run_id,
    g.trade_date,
    NULL::text,
    'PUBLISHED',
    g.target_count,
    g.active_count,
    g.entered_count,
    g.held_count,
    g.exited_count,
    g.waiting_count,
    g.entered_count + g.exited_count + g.waiting_count,
    ((g.entered_count + g.exited_count)::double precision / GREATEST(g.target_count, 1)),
    NULL::double precision,
    jsonb_build_object(
        'entered_count', g.entered_count,
        'held_count', g.held_count,
        'exited_count', g.exited_count,
        'waiting_count', g.waiting_count,
        'changed_count', g.entered_count + g.exited_count + g.waiting_count,
        'active_count', g.active_count,
        'overlap_rate', NULL,
        'turnover_rate', ((g.entered_count + g.exited_count)::double precision / GREATEST(g.target_count, 1)),
        'previous_list_version_id', NULL,
        'backfill_source', 'advisory_daily_review',
        'manual_gate', false
    ),
    NOW(),
    jsonb_build_object(
        'list_version_id', 'advlv_backfill_' || md5(g.program_id || ':' || g.trade_date::text),
        'program_id', g.program_id,
        'binding_version_id', g.binding_version_id,
        'review_run_id', g.review_run_id,
        'trade_date', g.trade_date,
        'previous_list_version_id', NULL,
        'version_status', 'PUBLISHED',
        'target_count', g.target_count,
        'active_count', g.active_count,
        'entered_count', g.entered_count,
        'held_count', g.held_count,
        'exited_count', g.exited_count,
        'waiting_count', g.waiting_count,
        'changed_count', g.entered_count + g.exited_count + g.waiting_count,
        'turnover_rate', ((g.entered_count + g.exited_count)::double precision / GREATEST(g.target_count, 1)),
        'overlap_rate', NULL,
        'summary_json', jsonb_build_object('backfill_source', 'advisory_daily_review')
    )
FROM grouped g
WHERE NOT EXISTS (
    SELECT 1
    FROM app.advisory_recommendation_list_version lv
    WHERE lv.program_id = g.program_id
      AND lv.trade_date = g.trade_date
      AND lv.version_status = 'PUBLISHED'
);

INSERT INTO app.advisory_recommendation_list_item (
    list_item_id, list_version_id, program_id, binding_version_id, episode_id,
    symbol, item_state, action, previous_action, rank, score, previous_rank,
    previous_score, entry_price, exit_price, price_basis, effective_trade_date,
    reason_code, operation_advice_json, component_scores_json, evidence_json,
    created_at, item_payload_json
)
SELECT
    'advli_backfill_' || md5(r.program_id || ':' || r.trade_date::text || ':' || r.code || ':' || r.review_id::text),
    lv.list_version_id,
    r.program_id,
    lv.binding_version_id,
    r.episode_id,
    r.code,
    CASE
        WHEN r.action = 'EXIT' THEN 'EXITED'
        WHEN r.action = 'WAITING' THEN 'WAITING'
        ELSE 'ACTIVE'
    END,
    r.action,
    NULL::text,
    r.rank,
    r.score,
    NULL::integer,
    NULL::double precision,
    CASE WHEN r.action <> 'EXIT' THEN r.current_price ELSE NULL END,
    CASE WHEN r.action = 'EXIT' THEN r.current_price ELSE NULL END,
    r.price_basis,
    r.trade_date,
    r.reason_code,
    COALESCE(
        r.decision_input_json -> 'operation_advice_json',
        jsonb_build_object(
            'advice_type', r.action,
            'human_label', r.action,
            'reason_code', r.reason_code,
            'reason_summary', r.reason_code,
            'trade_date', r.trade_date,
            'price_basis', r.price_basis,
            'suggested_price', r.current_price,
            'effective_trade_date', r.trade_date,
            'rank', r.rank,
            'score', r.score,
            'fallback_plan', 'Re-review on the next trading day if data or tradability is not ready.',
            'risk_note', 'Advisory-only recommendation; replay metrics are display diagnostics, not hard gates.'
        )
    ),
    COALESCE(r.fusion_evidence_json -> 'component_scores', '{}'::jsonb),
    COALESCE(r.fusion_evidence_json, '{}'::jsonb),
    COALESCE(r.created_at, NOW()),
    jsonb_build_object(
        'list_item_id', 'advli_backfill_' || md5(r.program_id || ':' || r.trade_date::text || ':' || r.code || ':' || r.review_id::text),
        'list_version_id', lv.list_version_id,
        'program_id', r.program_id,
        'binding_version_id', lv.binding_version_id,
        'episode_id', r.episode_id,
        'symbol', r.code,
        'item_state', CASE WHEN r.action = 'EXIT' THEN 'EXITED' WHEN r.action = 'WAITING' THEN 'WAITING' ELSE 'ACTIVE' END,
        'action', r.action,
        'previous_action', NULL,
        'rank', r.rank,
        'score', r.score,
        'previous_rank', NULL,
        'previous_score', NULL,
        'entry_price', CASE WHEN r.action <> 'EXIT' THEN r.current_price ELSE NULL END,
        'exit_price', CASE WHEN r.action = 'EXIT' THEN r.current_price ELSE NULL END,
        'price_basis', r.price_basis,
        'effective_trade_date', r.trade_date,
        'reason_code', r.reason_code,
        'operation_advice_json', COALESCE(
            r.decision_input_json -> 'operation_advice_json',
            jsonb_build_object(
                'advice_type', r.action,
                'human_label', r.action,
                'reason_code', r.reason_code,
                'reason_summary', r.reason_code,
                'trade_date', r.trade_date,
                'price_basis', r.price_basis,
                'suggested_price', r.current_price,
                'effective_trade_date', r.trade_date,
                'rank', r.rank,
                'score', r.score,
                'fallback_plan', 'Re-review on the next trading day if data or tradability is not ready.',
                'risk_note', 'Advisory-only recommendation; replay metrics are display diagnostics, not hard gates.'
            )
        ),
        'component_scores_json', COALESCE(r.fusion_evidence_json -> 'component_scores', '{}'::jsonb),
        'evidence_json', COALESCE(r.fusion_evidence_json, '{}'::jsonb),
        'created_at', COALESCE(r.created_at, NOW())
    )
FROM app.advisory_daily_review r
JOIN app.advisory_recommendation_list_version lv
  ON lv.program_id = r.program_id
 AND lv.trade_date = r.trade_date
 AND lv.version_status = 'PUBLISHED'
WHERE r.program_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM app.advisory_recommendation_list_item item
      WHERE item.list_item_id = 'advli_backfill_' || md5(r.program_id || ':' || r.trade_date::text || ':' || r.code || ':' || r.review_id::text)
  );
