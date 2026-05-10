CREATE SCHEMA IF NOT EXISTS model_registry;

CREATE TABLE IF NOT EXISTS model_registry.model_template (
    template_id TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    model_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    task_type TEXT NOT NULL DEFAULT 'rank',
    supported_freq TEXT[] NOT NULL DEFAULT ARRAY['day'],
    supported_input_shape TEXT NOT NULL DEFAULT 'tabular',
    train_backend TEXT NOT NULL DEFAULT 'qlib',
    default_search_space JSONB NOT NULL DEFAULT '{}'::jsonb,
    default_train_budget JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed_capability TEXT NOT NULL DEFAULT 'unset_legacy',
    deterministic_support TEXT NOT NULL DEFAULT 'partial',
    gpu_required BOOLEAN NOT NULL DEFAULT FALSE,
    lifecycle_status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT model_template_seed_capability_check CHECK (seed_capability IN ('fixed', 'multi_seed', 'random_logged', 'unsupported', 'unset_legacy')),
    CONSTRAINT model_template_deterministic_support_check CHECK (deterministic_support IN ('full', 'partial', 'none')),
    CONSTRAINT model_template_lifecycle_status_check CHECK (lifecycle_status IN ('active', 'experimental', 'deprecated', 'retired'))
);

CREATE TABLE IF NOT EXISTS model_registry.model_spec (
    spec_id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL REFERENCES model_registry.model_template(template_id) ON DELETE RESTRICT,
    spec_version TEXT NOT NULL DEFAULT 'v1',
    model_name TEXT NOT NULL,
    model_type TEXT NOT NULL,
    code_ref TEXT,
    code_text TEXT,
    code_sha256 TEXT,
    architecture_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    architecture_sha256 TEXT,
    hyperparam_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    default_hyperparams JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_space_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_contract_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_contract_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    feature_schema_requirements JSONB NOT NULL DEFAULT '{}'::jsonb,
    label_requirements JSONB NOT NULL DEFAULT '{}'::jsonb,
    dependency_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_type TEXT NOT NULL DEFAULT 'builtin',
    source_task_id TEXT,
    source_loop_id TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'research_candidate',
    qe_selectable BOOLEAN NOT NULL DEFAULT TRUE,
    qe_selectability_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT model_spec_source_type_check CHECK (source_type IN ('builtin', 'rdagent_sync', 'manual', 'imported')),
    CONSTRAINT model_spec_lifecycle_status_check CHECK (lifecycle_status IN ('template', 'research_candidate', 'rdagent_candidate', 'validated_spec', 'promoted_artifact', 'paper_candidate', 'paper_enabled', 'quarantined', 'training_failed', 'retired'))
);

CREATE TABLE IF NOT EXISTS model_registry.model_trial (
    trial_id TEXT PRIMARY KEY,
    spec_id TEXT NOT NULL REFERENCES model_registry.model_spec(spec_id) ON DELETE RESTRICT,
    qe_run_id TEXT,
    qe_experiment_id TEXT,
    qe_task_id TEXT,
    qe_loop_id TEXT,
    factor_set_hash TEXT,
    factor_list_ordered JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_schema_hash TEXT,
    data_context_id TEXT,
    dataset_version TEXT,
    label_config_hash TEXT,
    train_start DATE,
    train_end DATE,
    valid_start DATE,
    valid_end DATE,
    test_start DATE,
    test_end DATE,
    train_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    hyperparams_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed_policy TEXT NOT NULL DEFAULT 'unset_legacy',
    random_seed BIGINT,
    seed_sequence JSONB NOT NULL DEFAULT '{}'::jsonb,
    deterministic_flags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'succeeded',
    failure_reason TEXT,
    best_epoch INTEGER,
    total_epochs INTEGER,
    train_loss_final DOUBLE PRECISION,
    val_loss_final DOUBLE PRECISION,
    training_curves JSONB NOT NULL DEFAULT '{}'::jsonb,
    ic DOUBLE PRECISION,
    rank_ic DOUBLE PRECISION,
    icir DOUBLE PRECISION,
    annualized_return DOUBLE PRECISION,
    sharpe DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    cost_drag DOUBLE PRECISION,
    score_total DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT model_trial_seed_policy_check CHECK (seed_policy IN ('fixed', 'multi_seed', 'random_logged', 'unsupported', 'unset_legacy')),
    CONSTRAINT model_trial_status_check CHECK (status IN ('succeeded', 'failed', 'interrupted', 'invalid'))
);

CREATE TABLE IF NOT EXISTS model_registry.model_artifact (
    artifact_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES model_registry.model_trial(trial_id) ON DELETE RESTRICT,
    artifact_type TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    artifact_sha256 TEXT,
    artifact_size_bytes BIGINT,
    feature_schema_hash TEXT,
    feature_order_hash TEXT,
    preprocessor_hash TEXT,
    model_format TEXT,
    retention_class TEXT NOT NULL DEFAULT 'archived',
    protected_asset BOOLEAN NOT NULL DEFAULT FALSE,
    artifact_status TEXT NOT NULL DEFAULT 'present',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT model_artifact_type_check CHECK (artifact_type IN ('weights', 'preprocessor', 'feature_order', 'feature_schema', 'prediction_schema', 'checkpoint', 'params', 'qlib_recorder', 'other')),
    CONSTRAINT model_artifact_retention_class_check CHECK (retention_class IN ('temporary', 'archived', 'promoted', 'protected')),
    CONSTRAINT model_artifact_status_check CHECK (artifact_status IN ('present', 'missing', 'corrupted', 'expired')),
    CONSTRAINT model_artifact_protected_retention_check CHECK (protected_asset = FALSE OR retention_class IN ('promoted', 'protected'))
);

CREATE TABLE IF NOT EXISTS model_registry.model_lifecycle_event (
    event_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    operator TEXT NOT NULL,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT model_lifecycle_event_object_type_check CHECK (object_type IN ('template', 'spec', 'trial', 'artifact'))
);

CREATE INDEX IF NOT EXISTS idx_model_spec_template ON model_registry.model_spec(template_id);
CREATE INDEX IF NOT EXISTS idx_model_spec_qe_selectable ON model_registry.model_spec(qe_selectable, lifecycle_status);
CREATE INDEX IF NOT EXISTS idx_model_spec_source ON model_registry.model_spec(source_type, source_task_id, source_loop_id);
CREATE INDEX IF NOT EXISTS idx_model_trial_spec ON model_registry.model_trial(spec_id);
CREATE INDEX IF NOT EXISTS idx_model_trial_qe_lineage ON model_registry.model_trial(qe_task_id, qe_loop_id);
CREATE INDEX IF NOT EXISTS idx_model_artifact_trial ON model_registry.model_artifact(trial_id);
CREATE INDEX IF NOT EXISTS idx_model_artifact_status ON model_registry.model_artifact(artifact_status, retention_class, protected_asset);
CREATE INDEX IF NOT EXISTS idx_model_lifecycle_event_object ON model_registry.model_lifecycle_event(object_type, object_id, created_at DESC);

CREATE OR REPLACE VIEW model_registry.v_qe_selectable_model_spec AS
SELECT
    s.spec_id,
    s.template_id,
    t.family,
    s.model_name,
    s.model_type,
    s.spec_version,
    s.code_ref,
    s.code_text,
    s.code_sha256,
    s.architecture_config,
    s.architecture_sha256,
    s.hyperparam_schema,
    s.feature_schema_requirements,
    s.label_requirements,
    s.dependency_versions,
    s.source_type,
    s.source_task_id,
    s.source_loop_id,
    s.lifecycle_status,
    s.qe_selectable,
    s.qe_selectability_reason,
    t.seed_capability,
    t.deterministic_support,
    t.gpu_required,
    s.default_hyperparams,
    s.search_space_json,
    s.input_contract_json,
    s.output_contract_json,
    s.created_at,
    s.updated_at
FROM model_registry.model_spec s
JOIN model_registry.model_template t ON t.template_id = s.template_id
WHERE s.qe_selectable = TRUE
  AND s.lifecycle_status NOT IN ('quarantined', 'training_failed', 'retired')
  AND t.lifecycle_status NOT IN ('deprecated', 'retired');

CREATE OR REPLACE VIEW model_registry.v_model_catalog_compat AS
SELECT
    s.spec_id AS model_id,
    s.model_name,
    s.model_type,
    s.lifecycle_status,
    s.qe_selectable,
    FALSE::BOOLEAN AS paper_selectable,
    s.source_task_id AS task_run_id,
    s.source_loop_id AS loop_id,
    s.code_sha256,
    s.architecture_sha256,
    latest_trial.trial_id,
    latest_trial.status AS latest_trial_status,
    latest_trial.ic,
    latest_trial.rank_ic,
    latest_trial.sharpe,
    latest_trial.score_total,
    latest_artifact.artifact_id,
    latest_artifact.artifact_uri,
    latest_artifact.artifact_status,
    latest_artifact.protected_asset,
    s.created_at,
    s.updated_at
FROM model_registry.model_spec s
LEFT JOIN LATERAL (
    SELECT t.*
    FROM model_registry.model_trial t
    WHERE t.spec_id = s.spec_id
    ORDER BY COALESCE(t.completed_at, t.created_at) DESC
    LIMIT 1
) latest_trial ON TRUE
LEFT JOIN LATERAL (
    SELECT a.*
    FROM model_registry.model_artifact a
    WHERE latest_trial.trial_id IS NOT NULL AND a.trial_id = latest_trial.trial_id
    ORDER BY a.created_at DESC
    LIMIT 1
) latest_artifact ON TRUE;

CREATE OR REPLACE VIEW model_registry.v_legacy_aistock_model_catalog_bridge AS
SELECT
    mc.model_id AS legacy_model_id,
    mc.model_type,
    mc.task_run_id,
    mc.loop_id::TEXT AS loop_id,
    mc.workspace_id,
    mc.asset_bundle_id,
    mc.model_config,
    mc.feature_schema,
    mc.model_artifacts,
    'artifact_legacy'::TEXT AS model_role,
    CASE WHEN lower(COALESCE(mc.raw_payload ->> 'training_failed', 'false')) IN ('true', '1', 'yes') THEN 'training_failed' ELSE 'research_candidate' END AS lifecycle_status,
    CASE WHEN lower(COALESCE(mc.raw_payload ->> 'training_failed', 'false')) IN ('true', '1', 'yes') THEN FALSE ELSE TRUE END AS qe_selectable,
    FALSE::BOOLEAN AS paper_selectable,
    'legacy aistock_model_catalog compatibility bridge; Paper selects StrategyPackage, not this model row'::TEXT AS qe_selectability_reason,
    -- public.aistock_model_catalog has generated_at_utc text but no
    -- created_at/updated_at columns. Keep nullable synthetic timestamps to
    -- preserve the bridge contract without risking runtime text-cast failures.
    NULL::TIMESTAMPTZ AS created_at,
    NULL::TIMESTAMPTZ AS updated_at
FROM public.aistock_model_catalog mc;

COMMENT ON SCHEMA model_registry IS 'Authoritative model registry schema for QE model templates, specs, trials, artifacts, and lifecycle audit events.';

COMMENT ON TABLE model_registry.model_template IS 'Reusable model family/template capability records; no training result or weights are stored here.';
COMMENT ON COLUMN model_registry.model_template.template_id IS 'Stable model template identifier assigned by AIstock or migration tooling.';
COMMENT ON COLUMN model_registry.model_template.family IS 'Broad model family such as tree, boosting, neural_ts, tabular_deep, or transformer.';
COMMENT ON COLUMN model_registry.model_template.model_type IS 'Concrete model type such as LGBModel, CatBoost, LSTM, GRU, or Transformer.';
COMMENT ON COLUMN model_registry.model_template.display_name IS 'Operator-facing model template display name.';
COMMENT ON COLUMN model_registry.model_template.description IS 'Human-readable model template description and intended use.';
COMMENT ON COLUMN model_registry.model_template.task_type IS 'Learning task type such as rank, regression, or classification.';
COMMENT ON COLUMN model_registry.model_template.supported_freq IS 'Supported data frequencies, for example day, 1min, or mixed.';
COMMENT ON COLUMN model_registry.model_template.supported_input_shape IS 'Expected input shape family such as tabular or sequence_30d.';
COMMENT ON COLUMN model_registry.model_template.train_backend IS 'Training backend such as qlib, sklearn, torch, or tabpfn.';
COMMENT ON COLUMN model_registry.model_template.default_search_space IS 'Default hyperparameter search space JSON used when QE explores this template.';
COMMENT ON COLUMN model_registry.model_template.default_train_budget IS 'Default training budget JSON such as max trials, epochs, timeout, or GPU needs.';
COMMENT ON COLUMN model_registry.model_template.seed_capability IS 'Seed reproducibility capability: fixed, multi_seed, random_logged, unsupported, or unset_legacy.';
COMMENT ON COLUMN model_registry.model_template.deterministic_support IS 'Deterministic training support level: full, partial, or none.';
COMMENT ON COLUMN model_registry.model_template.gpu_required IS 'Whether this template requires GPU resources for normal training.';
COMMENT ON COLUMN model_registry.model_template.lifecycle_status IS 'Template lifecycle status: active, experimental, deprecated, or retired.';
COMMENT ON COLUMN model_registry.model_template.created_at IS 'Timestamp when the template row was created.';
COMMENT ON COLUMN model_registry.model_template.updated_at IS 'Timestamp when the template row was last updated.';

COMMENT ON TABLE model_registry.model_spec IS 'Trainable model specification records containing code, architecture, contracts, and QE selectability state.';
COMMENT ON COLUMN model_registry.model_spec.spec_id IS 'Stable model spec identifier.';
COMMENT ON COLUMN model_registry.model_spec.template_id IS 'Parent model template identifier.';
COMMENT ON COLUMN model_registry.model_spec.spec_version IS 'Version label for this model specification.';
COMMENT ON COLUMN model_registry.model_spec.model_name IS 'Operator-facing model spec name.';
COMMENT ON COLUMN model_registry.model_spec.model_type IS 'Concrete model type implemented by this spec.';
COMMENT ON COLUMN model_registry.model_spec.code_ref IS 'Repository path, artifact pointer, or external reference for the model code.';
COMMENT ON COLUMN model_registry.model_spec.code_text IS 'Optional inline source text snapshot for generated or imported specs.';
COMMENT ON COLUMN model_registry.model_spec.code_sha256 IS 'SHA256 digest of the model implementation code snapshot.';
COMMENT ON COLUMN model_registry.model_spec.architecture_config IS 'Model architecture configuration JSON.';
COMMENT ON COLUMN model_registry.model_spec.architecture_sha256 IS 'SHA256 digest of normalized architecture configuration.';
COMMENT ON COLUMN model_registry.model_spec.hyperparam_schema IS 'JSON schema for accepted hyperparameters.';
COMMENT ON COLUMN model_registry.model_spec.default_hyperparams IS 'Default hyperparameter values for this spec.';
COMMENT ON COLUMN model_registry.model_spec.search_space_json IS 'QE search space JSON for this spec.';
COMMENT ON COLUMN model_registry.model_spec.input_contract_json IS 'Input contract required by this spec, including features and shapes.';
COMMENT ON COLUMN model_registry.model_spec.output_contract_json IS 'Output contract produced by this spec, including prediction schema.';
COMMENT ON COLUMN model_registry.model_spec.feature_schema_requirements IS 'Feature schema requirements that must be frozen when promoted.';
COMMENT ON COLUMN model_registry.model_spec.label_requirements IS 'Label horizon and target requirements for training this spec.';
COMMENT ON COLUMN model_registry.model_spec.dependency_versions IS 'Library and runtime dependency versions relevant to this spec.';
COMMENT ON COLUMN model_registry.model_spec.source_type IS 'Source channel: builtin, rdagent_sync, manual, or imported.';
COMMENT ON COLUMN model_registry.model_spec.source_task_id IS 'Optional originating RD-Agent or QE task id.';
COMMENT ON COLUMN model_registry.model_spec.source_loop_id IS 'Optional originating RD-Agent or QE loop id.';
COMMENT ON COLUMN model_registry.model_spec.lifecycle_status IS 'Spec lifecycle status controlling QE default selection visibility.';
COMMENT ON COLUMN model_registry.model_spec.qe_selectable IS 'Whether QE default model selection may include this spec.';
COMMENT ON COLUMN model_registry.model_spec.qe_selectability_reason IS 'Human-readable explanation for QE selectability state.';
COMMENT ON COLUMN model_registry.model_spec.created_at IS 'Timestamp when the spec row was created.';
COMMENT ON COLUMN model_registry.model_spec.updated_at IS 'Timestamp when the spec row was last updated.';

COMMENT ON TABLE model_registry.model_trial IS 'One training attempt for a model spec, including data context, factors, seed contract, status, and metrics.';
COMMENT ON COLUMN model_registry.model_trial.trial_id IS 'Stable model trial identifier.';
COMMENT ON COLUMN model_registry.model_trial.spec_id IS 'Model spec trained by this trial.';
COMMENT ON COLUMN model_registry.model_trial.qe_run_id IS 'Optional QE run id associated with the trial.';
COMMENT ON COLUMN model_registry.model_trial.qe_experiment_id IS 'Optional QE experiment id associated with the trial.';
COMMENT ON COLUMN model_registry.model_trial.qe_task_id IS 'Optional QE task id associated with the trial.';
COMMENT ON COLUMN model_registry.model_trial.qe_loop_id IS 'Optional QE loop id associated with the trial.';
COMMENT ON COLUMN model_registry.model_trial.factor_set_hash IS 'Digest of the ordered factor set used for this trial.';
COMMENT ON COLUMN model_registry.model_trial.factor_list_ordered IS 'Ordered factor list JSON used by this trial.';
COMMENT ON COLUMN model_registry.model_trial.feature_schema_hash IS 'Digest of the feature schema used by this trial.';
COMMENT ON COLUMN model_registry.model_trial.data_context_id IS 'Dataset or warehouse context identifier used during training.';
COMMENT ON COLUMN model_registry.model_trial.dataset_version IS 'Dataset version label used during training.';
COMMENT ON COLUMN model_registry.model_trial.label_config_hash IS 'Digest of label horizon and target configuration.';
COMMENT ON COLUMN model_registry.model_trial.train_start IS 'Training window start date.';
COMMENT ON COLUMN model_registry.model_trial.train_end IS 'Training window end date.';
COMMENT ON COLUMN model_registry.model_trial.valid_start IS 'Validation window start date.';
COMMENT ON COLUMN model_registry.model_trial.valid_end IS 'Validation window end date.';
COMMENT ON COLUMN model_registry.model_trial.test_start IS 'Test window start date.';
COMMENT ON COLUMN model_registry.model_trial.test_end IS 'Test window end date.';
COMMENT ON COLUMN model_registry.model_trial.train_config_json IS 'Full normalized training config JSON.';
COMMENT ON COLUMN model_registry.model_trial.hyperparams_json IS 'Actual hyperparameters used by this trial.';
COMMENT ON COLUMN model_registry.model_trial.seed_policy IS 'Seed policy used by this trial.';
COMMENT ON COLUMN model_registry.model_trial.random_seed IS 'Primary random seed when present.';
COMMENT ON COLUMN model_registry.model_trial.seed_sequence IS 'Per-library or per-component child seed map.';
COMMENT ON COLUMN model_registry.model_trial.deterministic_flags_json IS 'Deterministic backend flags recorded for audit.';
COMMENT ON COLUMN model_registry.model_trial.status IS 'Trial execution status: succeeded, failed, interrupted, or invalid.';
COMMENT ON COLUMN model_registry.model_trial.failure_reason IS 'Fail-fast reason or training failure diagnostic.';
COMMENT ON COLUMN model_registry.model_trial.best_epoch IS 'Best epoch selected by validation diagnostics.';
COMMENT ON COLUMN model_registry.model_trial.total_epochs IS 'Total epochs attempted by training.';
COMMENT ON COLUMN model_registry.model_trial.train_loss_final IS 'Final training loss.';
COMMENT ON COLUMN model_registry.model_trial.val_loss_final IS 'Final validation loss.';
COMMENT ON COLUMN model_registry.model_trial.training_curves IS 'Training curve JSON including loss and metric histories.';
COMMENT ON COLUMN model_registry.model_trial.ic IS 'Information coefficient metric.';
COMMENT ON COLUMN model_registry.model_trial.rank_ic IS 'Rank information coefficient metric.';
COMMENT ON COLUMN model_registry.model_trial.icir IS 'Information coefficient information ratio metric.';
COMMENT ON COLUMN model_registry.model_trial.annualized_return IS 'Backtest annualized return for this trial.';
COMMENT ON COLUMN model_registry.model_trial.sharpe IS 'Backtest Sharpe ratio for this trial.';
COMMENT ON COLUMN model_registry.model_trial.max_drawdown IS 'Backtest maximum drawdown for this trial.';
COMMENT ON COLUMN model_registry.model_trial.turnover IS 'Backtest turnover for this trial.';
COMMENT ON COLUMN model_registry.model_trial.cost_drag IS 'Estimated cost drag for this trial.';
COMMENT ON COLUMN model_registry.model_trial.score_total IS 'Composite score used by QE ranking or governance.';
COMMENT ON COLUMN model_registry.model_trial.created_at IS 'Timestamp when the trial row was created.';
COMMENT ON COLUMN model_registry.model_trial.completed_at IS 'Timestamp when the trial completed or failed.';

COMMENT ON TABLE model_registry.model_artifact IS 'Reusable model artifact metadata; weight files stay in controlled storage and are referenced by URI and digest.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_id IS 'Stable model artifact identifier.';
COMMENT ON COLUMN model_registry.model_artifact.trial_id IS 'Trial that produced this artifact.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_type IS 'Artifact type such as weights, preprocessor, feature_order, feature_schema, prediction_schema, checkpoint, params, or qlib_recorder.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_uri IS 'Controlled storage URI or path for the artifact.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_sha256 IS 'SHA256 digest of artifact bytes when available.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_size_bytes IS 'Artifact size in bytes when known.';
COMMENT ON COLUMN model_registry.model_artifact.feature_schema_hash IS 'Feature schema digest associated with this artifact.';
COMMENT ON COLUMN model_registry.model_artifact.feature_order_hash IS 'Feature order digest associated with this artifact.';
COMMENT ON COLUMN model_registry.model_artifact.preprocessor_hash IS 'Preprocessor digest associated with this artifact.';
COMMENT ON COLUMN model_registry.model_artifact.model_format IS 'Serialized artifact format such as pkl, pt, json, txt, qlib_recorder, or tar.';
COMMENT ON COLUMN model_registry.model_artifact.retention_class IS 'Retention class: temporary, archived, promoted, or protected.';
COMMENT ON COLUMN model_registry.model_artifact.protected_asset IS 'Whether governance forbids cleanup or overwrite of this artifact.';
COMMENT ON COLUMN model_registry.model_artifact.artifact_status IS 'Artifact health status: present, missing, corrupted, or expired.';
COMMENT ON COLUMN model_registry.model_artifact.created_at IS 'Timestamp when the artifact row was created.';
COMMENT ON COLUMN model_registry.model_artifact.validated_at IS 'Timestamp when the artifact digest or loadability was last validated.';
COMMENT ON COLUMN model_registry.model_artifact.metadata_json IS 'Additional artifact metadata JSON.';

COMMENT ON TABLE model_registry.model_lifecycle_event IS 'Append-only lifecycle audit trail for templates, specs, trials, and artifacts; replaces silent delete semantics.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.event_id IS 'Stable lifecycle event identifier.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.object_type IS 'Changed object type: template, spec, trial, or artifact.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.object_id IS 'Identifier of the changed object.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.from_status IS 'Previous status before the transition.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.to_status IS 'New status after the transition.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.reason IS 'Operator or system reason for the transition.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.operator IS 'Operator, service, or automation that requested the transition.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.context_json IS 'Structured context for the lifecycle transition.';
COMMENT ON COLUMN model_registry.model_lifecycle_event.created_at IS 'Timestamp when the lifecycle event was recorded.';

COMMENT ON VIEW model_registry.v_qe_selectable_model_spec IS 'Default QE model search-space view; quarantined, training_failed, retired, deprecated, and non-selectable specs are hidden.';
COMMENT ON VIEW model_registry.v_model_catalog_compat IS 'Compatibility projection from model_registry four-layer records to model catalog style metadata; Paper selectability is always false.';
COMMENT ON VIEW model_registry.v_legacy_aistock_model_catalog_bridge IS 'Read-only bridge exposing legacy aistock_model_catalog rows with model_registry governance labels for migration planning.';
