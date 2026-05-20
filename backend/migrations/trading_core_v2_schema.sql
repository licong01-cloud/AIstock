-- Trading Core v2 / Strategy Package / Selection Center / Paper v2 schema.
-- Keep this migration explicit; business services must not run DDL implicitly.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;
CREATE SCHEMA IF NOT EXISTS selection;
CREATE SCHEMA IF NOT EXISTS paper_v2;
CREATE SCHEMA IF NOT EXISTS trading_core;

CREATE TABLE IF NOT EXISTS market.dataset_date_refresh_audit (
    dataset TEXT NOT NULL,
    trade_date DATE NOT NULL,
    data_source TEXT NOT NULL,
    job_id UUID,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_message TEXT,
    data_max_at TIMESTAMPTZ,
    written_rows BIGINT CHECK (written_rows IS NULL OR written_rows >= 0),
    expected_rows BIGINT CHECK (expected_rows IS NULL OR expected_rows >= 0),
    coverage_ratio NUMERIC(12, 8) CHECK (coverage_ratio IS NULL OR (coverage_ratio >= 0 AND coverage_ratio <= 1.5)),
    quality_status TEXT NOT NULL DEFAULT 'unknown',
    failure_category TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, trade_date, data_source)
);

COMMENT ON TABLE market.dataset_date_refresh_audit IS 'AIstock dataset/date readiness ledger used by local data management, Selection Center, and Paper v2 fail-fast data gates.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.dataset IS 'Logical dataset key, for example suspend_d, stk_limit, kline_daily_raw, or sector_data.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.trade_date IS 'Trading date or effective dataset date that this readiness row describes.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.data_source IS 'Provider or process that produced the readiness row, such as tushare, tdx_api, sector_builder, or seed_existing_rows.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.job_id IS 'Optional market.ingestion_jobs.job_id that produced the latest readiness state for this dataset/date/source.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.status IS 'Readiness status; success means the dataset/date/source is usable, failed means it must not pass Paper v2/local data gates.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.row_count IS 'Final usable row count present for this dataset/date after the refresh attempt completed.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.refreshed_at IS 'Timestamp when the readiness row was written or updated by the refresh/audit process.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.error_message IS 'Provider, validation, or persistence error message for failed refresh attempts.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.data_max_at IS 'Precise maximum source data timestamp covered by this row for intraday or timestamped datasets; NULL for date-only datasets.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.written_rows IS 'Rows written or touched by the latest refresh attempt for this dataset/date/source; NULL when unknown.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.expected_rows IS 'Optional expected usable row count for coverage checks on this dataset/date.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.coverage_ratio IS 'Optional row_count divided by expected_rows; values below dataset policy thresholds indicate low coverage.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.quality_status IS 'Quality classification such as ok, unknown, empty_valid, empty_invalid, low_coverage, upstream_not_published, provider_unavailable, or error.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.failure_category IS 'Machine-readable failure reason for retry/self-healing decisions, for example audit_stale, empty_invalid, or provider_unavailable.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.metadata IS 'Additional JSON context including API name, ingestion mode, table, source script, and validation notes.';

CREATE TABLE IF NOT EXISTS strategy_pkg.package (
    package_id TEXT PRIMARY KEY,
    package_name TEXT NOT NULL,
    package_version TEXT NOT NULL,
    source_type TEXT NOT NULL CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package')),
    source_id TEXT NOT NULL,
    loop_id TEXT,
    run_id TEXT,
    package_status TEXT NOT NULL,
    manifest_json JSONB NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    paper_portfolio_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package (
    candidate_id TEXT PRIMARY KEY,
    candidate_version INTEGER NOT NULL DEFAULT 1 CHECK (candidate_version > 0),
    source_type TEXT NOT NULL CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package')),
    source_id TEXT NOT NULL,
    source_task_id TEXT,
    source_loop_id TEXT,
    source_experiment_id TEXT,
    archive_run_id TEXT,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DELETED')),
    snapshot_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    factor_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    strategy_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    artifact_refs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    completeness_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    eligibility_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    audit_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_by TEXT,
    deleted_at TIMESTAMPTZ,
    delete_reason TEXT,
    UNIQUE (source_type, source_id, candidate_version)
);

CREATE INDEX IF NOT EXISTS idx_strategy_pkg_candidate_source
    ON strategy_pkg.candidate_strategy_package(source_type, source_id, status);

CREATE INDEX IF NOT EXISTS idx_strategy_pkg_candidate_status
    ON strategy_pkg.candidate_strategy_package(status, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES strategy_pkg.candidate_strategy_package(candidate_id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_pkg.package_status_event (
    event_id BIGSERIAL PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_pkg.promotion_review (
    review_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package')),
    source_id TEXT NOT NULL,
    task_id TEXT,
    loop_id TEXT,
    experiment_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('AUTO_CANDIDATE', 'REVIEW_PENDING', 'REVIEW_REJECTED', 'SOTA_APPROVED')),
    requested_by TEXT NOT NULL,
    reviewer TEXT,
    review_reason TEXT,
    decision_reason TEXT,
    source_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    audit_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    UNIQUE (source_type, source_id)
);

COMMENT ON TABLE strategy_pkg.promotion_review IS 'Manual SOTA promotion review ledger for QE experiments or evolution loops; REVIEW_PENDING is created by user action and does not imply approved SOTA or Paper eligibility.';
COMMENT ON COLUMN strategy_pkg.promotion_review.review_id IS 'Stable review identifier generated by AIstock promotion workflow, prefixed pr_.';
COMMENT ON COLUMN strategy_pkg.promotion_review.source_type IS 'Source object type for the candidate, currently qe_experiment or qe_evolution_loop.';
COMMENT ON COLUMN strategy_pkg.promotion_review.source_id IS 'Stable source object identifier; unique with source_type to make repeated manual requests idempotent.';
COMMENT ON COLUMN strategy_pkg.promotion_review.task_id IS 'Optional QE task id for evolution-loop sources, retained for audit and UI filtering.';
COMMENT ON COLUMN strategy_pkg.promotion_review.loop_id IS 'Optional QE evolution loop id in database form, usually {task_id}_Loop{N}.';
COMMENT ON COLUMN strategy_pkg.promotion_review.experiment_id IS 'Optional QE experiment id associated with the promoted source.';
COMMENT ON COLUMN strategy_pkg.promotion_review.status IS 'Promotion lifecycle status; AUTO_CANDIDATE is an unapproved automatic discovery, REVIEW_PENDING awaits human review, and SOTA_APPROVED requires explicit future approval.';
COMMENT ON COLUMN strategy_pkg.promotion_review.requested_by IS 'User, UI actor, or service identity that requested manual SOTA review.';
COMMENT ON COLUMN strategy_pkg.promotion_review.reviewer IS 'Human reviewer identity that made a future approval or rejection decision; NULL while pending.';
COMMENT ON COLUMN strategy_pkg.promotion_review.review_reason IS 'Requester-supplied reason or note captured when creating the review.';
COMMENT ON COLUMN strategy_pkg.promotion_review.decision_reason IS 'Reviewer-supplied approval or rejection rationale; NULL while pending.';
COMMENT ON COLUMN strategy_pkg.promotion_review.source_metrics_json IS 'Snapshot of source QE metrics at request time; display/audit evidence only, not recomputed by this table.';
COMMENT ON COLUMN strategy_pkg.promotion_review.audit_json IS 'Machine-readable audit context including workflow source, candidate state, non-approval flags, and Paper eligibility false by default.';
COMMENT ON COLUMN strategy_pkg.promotion_review.created_at IS 'Timestamp when the manual promotion review row was created.';
COMMENT ON COLUMN strategy_pkg.promotion_review.updated_at IS 'Timestamp when the promotion review row was last updated.';
COMMENT ON COLUMN strategy_pkg.promotion_review.decided_at IS 'Timestamp of a future human approval or rejection decision; NULL for REVIEW_PENDING.';

CREATE TABLE IF NOT EXISTS strategy_pkg.package_asset (
    asset_id BIGSERIAL PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    asset_type TEXT NOT NULL,
    asset_ref TEXT NOT NULL,
    asset_sha256 TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_pkg.selection_score_artifact (
    artifact_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    trade_date DATE NOT NULL,
    data_source TEXT NOT NULL,
    runtime_config_hash TEXT NOT NULL,
    scores_json JSONB NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    score_count INTEGER NOT NULL CHECK (score_count >= 0),
    universe_count INTEGER NOT NULL CHECK (universe_count >= 0),
    top_score_symbol TEXT,
    status TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    error_json JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (package_id, manifest_sha256, trade_date, data_source, runtime_config_hash)
);

CREATE TABLE IF NOT EXISTS strategy_pkg.validated_execution_policy (
    policy_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_json JSONB NOT NULL,
    policy_sha256 TEXT NOT NULL,
    algo_code TEXT NOT NULL,
    algo_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    unfilled_handler TEXT,
    unfilled_handler_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_backtest_id TEXT NOT NULL,
    source_backtest_status TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    paper_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (package_id, policy_sha256)
);

CREATE TABLE IF NOT EXISTS strategy_pkg.live_approval (
    approval_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    alpha_core_sha256 TEXT NOT NULL,
    portfolio_id TEXT,
    runtime_release_id TEXT NOT NULL,
    runtime_release_sha256 TEXT NOT NULL,
    runtime_profile_id TEXT NOT NULL,
    runtime_profile_version_id TEXT NOT NULL,
    runtime_profile_sha256 TEXT NOT NULL,
    execution_policy_id TEXT NOT NULL,
    execution_policy_sha256 TEXT NOT NULL,
    tail_policy_id TEXT NOT NULL,
    tail_policy_sha256 TEXT NOT NULL,
    target_broker_backend TEXT NOT NULL,
    broker_account_id TEXT,
    approval_status TEXT NOT NULL CHECK (
        approval_status IN (
            'LIVE_CANDIDATE',
            'LIVE_APPROVAL_PENDING',
            'LIVE_APPROVED',
            'LIVE_REJECTED',
            'LIVE_RETIRED'
        )
    ),
    sim_validation_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    broker_compatibility JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_note TEXT,
    rollback_plan TEXT,
    requested_by TEXT,
    requested_at TIMESTAMPTZ,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    rejected_by TEXT,
    rejected_at TIMESTAMPTZ,
    rejection_reason TEXT,
    retired_by TEXT,
    retired_at TIMESTAMPTZ,
    retirement_reason TEXT,
    audit_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE strategy_pkg.live_approval IS 'Auditable live-admission lifecycle for future MiniQMT live promotion; Paper status alone never grants live eligibility.';
COMMENT ON COLUMN strategy_pkg.live_approval.approval_id IS 'Stable live approval identifier generated by AIstock, prefixed liveappr_.';
COMMENT ON COLUMN strategy_pkg.live_approval.package_id IS 'StrategyPackage alpha-core package_id under review for live admission.';
COMMENT ON COLUMN strategy_pkg.live_approval.manifest_sha256 IS 'Immutable StrategyPackage frozen manifest hash that the approval is bound to.';
COMMENT ON COLUMN strategy_pkg.live_approval.alpha_core_sha256 IS 'Hash of the package factor/model alpha core; must match the immutable StrategyPackage core.';
COMMENT ON COLUMN strategy_pkg.live_approval.portfolio_id IS 'Optional Paper v2 portfolio or MiniQMT strategy binding that produced the simulation evidence.';
COMMENT ON COLUMN strategy_pkg.live_approval.runtime_release_id IS 'Platform runtime release or portfolio-binding version identifier for the approved live configuration.';
COMMENT ON COLUMN strategy_pkg.live_approval.runtime_release_sha256 IS 'Canonical hash of package, runtime profile, execution policy, tail policy, broker target, and trade-date release references.';
COMMENT ON COLUMN strategy_pkg.live_approval.runtime_profile_id IS 'Platform runtime profile id; HMM, stock pool, ST PIT, risk, and daily strategy settings stay outside StrategyPackage manifest.';
COMMENT ON COLUMN strategy_pkg.live_approval.runtime_profile_version_id IS 'Specific runtime profile version id approved for live admission.';
COMMENT ON COLUMN strategy_pkg.live_approval.runtime_profile_sha256 IS 'Canonical hash of the runtime profile version approved for live admission.';
COMMENT ON COLUMN strategy_pkg.live_approval.execution_policy_id IS 'Backtest/simulation validated execution policy id approved for live admission.';
COMMENT ON COLUMN strategy_pkg.live_approval.execution_policy_sha256 IS 'Canonical hash of the validated execution policy JSON approved for live admission.';
COMMENT ON COLUMN strategy_pkg.live_approval.tail_policy_id IS 'Tail/unfilled handling policy reference approved for live admission; explicit even when the policy is fail-fast/default.';
COMMENT ON COLUMN strategy_pkg.live_approval.tail_policy_sha256 IS 'Canonical hash of the tail/unfilled handling policy payload.';
COMMENT ON COLUMN strategy_pkg.live_approval.target_broker_backend IS 'Broker backend targeted by approval, for example minqmt_live; adapter compatibility must be verified.';
COMMENT ON COLUMN strategy_pkg.live_approval.broker_account_id IS 'Optional live broker account id or alias covered by the approval; NULL means account binding is recorded elsewhere.';
COMMENT ON COLUMN strategy_pkg.live_approval.approval_status IS 'Lifecycle status: LIVE_CANDIDATE, LIVE_APPROVAL_PENDING, LIVE_APPROVED, LIVE_REJECTED, or LIVE_RETIRED.';
COMMENT ON COLUMN strategy_pkg.live_approval.sim_validation_evidence IS 'JSON evidence requiring successful Paper v2 and MiniQMT SIM validation runs, run ids, periods, metrics, and quality status.';
COMMENT ON COLUMN strategy_pkg.live_approval.broker_compatibility IS 'JSON evidence proving target broker compatibility; must include target_broker_backend/broker_backend and verified status.';
COMMENT ON COLUMN strategy_pkg.live_approval.risk_note IS 'Human risk note required before approval pending/approved states; NULL for raw candidate rows.';
COMMENT ON COLUMN strategy_pkg.live_approval.rollback_plan IS 'Human rollback plan required before approval pending/approved states; NULL for raw candidate rows.';
COMMENT ON COLUMN strategy_pkg.live_approval.requested_by IS 'Actor that submitted the candidate for human live approval; NULL while only a candidate.';
COMMENT ON COLUMN strategy_pkg.live_approval.requested_at IS 'Timestamp when human live approval was requested; NULL while only a candidate.';
COMMENT ON COLUMN strategy_pkg.live_approval.approved_by IS 'Human approver identity; required only when approval_status is LIVE_APPROVED.';
COMMENT ON COLUMN strategy_pkg.live_approval.approved_at IS 'Timestamp when human approval was granted; required only when approval_status is LIVE_APPROVED.';
COMMENT ON COLUMN strategy_pkg.live_approval.rejected_by IS 'Human reviewer identity that rejected the approval; required only when LIVE_REJECTED.';
COMMENT ON COLUMN strategy_pkg.live_approval.rejected_at IS 'Timestamp when approval was rejected; required only when LIVE_REJECTED.';
COMMENT ON COLUMN strategy_pkg.live_approval.rejection_reason IS 'Human rejection reason; required only when LIVE_REJECTED.';
COMMENT ON COLUMN strategy_pkg.live_approval.retired_by IS 'Actor that retired or rolled back this approval record; required only when LIVE_RETIRED.';
COMMENT ON COLUMN strategy_pkg.live_approval.retired_at IS 'Timestamp when approval was retired or rolled back; required only when LIVE_RETIRED.';
COMMENT ON COLUMN strategy_pkg.live_approval.retirement_reason IS 'Reason this approval was retired, superseded, or rolled back; required only when LIVE_RETIRED.';
COMMENT ON COLUMN strategy_pkg.live_approval.audit_json IS 'Append-only JSON audit context containing lifecycle events, release payload, validation source, and operator notes.';
COMMENT ON COLUMN strategy_pkg.live_approval.created_at IS 'Timestamp when the live approval candidate was created.';
COMMENT ON COLUMN strategy_pkg.live_approval.updated_at IS 'Timestamp when the live approval record was last changed.';

CREATE TABLE IF NOT EXISTS strategy_pkg.model_state (
    package_id TEXT PRIMARY KEY REFERENCES strategy_pkg.package(package_id),
    active_model_version_id TEXT,
    train_start_date DATE,
    train_end_date DATE,
    trained_at TIMESTAMPTZ,
    last_retrain_job_id TEXT,
    last_retrained_at TIMESTAMPTZ,
    stale_after_days INTEGER NOT NULL DEFAULT 30,
    staleness_status TEXT NOT NULL,
    warning TEXT,
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_pkg.model_retrain_job (
    job_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    job_type TEXT NOT NULL,
    requested_train_start_date DATE,
    requested_train_end_date DATE NOT NULL,
    stale_after_days INTEGER NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    requires_manual_confirmation BOOLEAN NOT NULL DEFAULT TRUE,
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    status_reason TEXT,
    error_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS selection.run (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    trade_date DATE NOT NULL,
    data_source TEXT NOT NULL,
    package_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    valid_no_candidate BOOLEAN NOT NULL DEFAULT FALSE,
    no_candidate_reason TEXT,
    error_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS selection.package_result (
    result_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES selection.run(run_id),
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    symbol TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    rank INTEGER NOT NULL,
    target_weight DOUBLE PRECISION,
    target_quantity INTEGER,
    reference_price DOUBLE PRECISION,
    component_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, package_id, symbol)
);

CREATE TABLE IF NOT EXISTS selection.aggregate_result (
    aggregate_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES selection.run(run_id),
    symbol TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    rank INTEGER NOT NULL,
    target_weight DOUBLE PRECISION,
    target_quantity INTEGER,
    reference_price DOUBLE PRECISION,
    source_package_ids JSONB NOT NULL,
    explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, symbol)
);

ALTER TABLE selection.run
    ADD COLUMN IF NOT EXISTS package_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS selection.excluded_result (
    exclusion_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES selection.run(run_id),
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    symbol TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    raw_rank INTEGER NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, package_id, symbol, reason)
);

CREATE TABLE IF NOT EXISTS selection.paper_portfolio_link (
    link_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES selection.run(run_id),
    portfolio_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    trade_date DATE NOT NULL,
    data_source TEXT NOT NULL,
    start_date DATE NOT NULL,
    initial_cash NUMERIC(20, 6) NOT NULL CHECK (initial_cash > 0),
    runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, portfolio_id)
);

CREATE TABLE IF NOT EXISTS paper_v2.portfolio (
    portfolio_id TEXT PRIMARY KEY,
    portfolio_name TEXT NOT NULL,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    frozen_manifest_json JSONB NOT NULL,
    initial_cash NUMERIC(20, 6) NOT NULL CHECK (initial_cash > 0),
    start_date DATE NOT NULL,
    data_source TEXT NOT NULL CHECK (data_source IN ('TDX_REALTIME', 'DB_HISTORICAL')),
    fee_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.execution_policy_activation (
    activation_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    trade_date DATE NOT NULL,
    policy_id TEXT NOT NULL,
    policy_sha256 TEXT NOT NULL,
    policy_name TEXT,
    policy_json JSONB NOT NULL,
    status TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by TEXT,
    reason TEXT,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    superseded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS paper_v2.runtime_profile (
    profile_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    package_id TEXT NOT NULL,
    profile_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'ACTIVE', 'RETIRED')),
    current_version_id TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.runtime_profile_version (
    profile_version_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES paper_v2.runtime_profile(profile_id),
    version_no INTEGER NOT NULL CHECK (version_no >= 1),
    config_json JSONB NOT NULL,
    config_sha256 TEXT NOT NULL,
    validation_status TEXT NOT NULL CHECK (validation_status IN ('VALIDATED', 'INVALID')),
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by TEXT,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    supersedes_version_id TEXT,
    UNIQUE(profile_id, version_no),
    UNIQUE(profile_id, config_sha256)
);

CREATE TABLE IF NOT EXISTS paper_v2.runtime_config_activation (
    activation_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    trade_date DATE NOT NULL,
    profile_version_id TEXT NOT NULL REFERENCES paper_v2.runtime_profile_version(profile_version_id),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'CANCELLED')),
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by TEXT,
    reason TEXT,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    superseded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS paper_v2.config_change_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    portfolio_id TEXT,
    package_id TEXT,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    change_type TEXT NOT NULL,
    before_json JSONB,
    after_json JSONB,
    before_sha256 TEXT,
    after_sha256 TEXT,
    reason TEXT,
    created_by TEXT,
    request_id TEXT,
    code_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.run (
    run_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    trade_date DATE NOT NULL,
    status TEXT NOT NULL,
    data_source TEXT NOT NULL,
    runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_json JSONB,
    UNIQUE(portfolio_id, trade_date)
);

CREATE TABLE IF NOT EXISTS paper_v2.trade_session (
    session_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    mode TEXT NOT NULL CHECK (mode IN ('REPLAY_ONLY', 'LIVE_ONLY', 'CATCHUP_THEN_LIVE')),
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    historical_data_source TEXT CHECK (historical_data_source IN ('TDX_REALTIME', 'DB_HISTORICAL')),
    live_data_source TEXT CHECK (live_data_source IN ('TDX_REALTIME', 'DB_HISTORICAL')),
    runtime_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    validated_execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error_json JSONB
);

CREATE TABLE IF NOT EXISTS paper_v2.session_day (
    session_day_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES paper_v2.trade_session(session_id),
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    trade_date DATE NOT NULL,
    run_id TEXT REFERENCES paper_v2.run(run_id),
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    data_source TEXT NOT NULL CHECK (data_source IN ('TDX_REALTIME', 'DB_HISTORICAL')),
    expected_bar_count INTEGER,
    latest_available_bar_time TIMESTAMPTZ,
    last_processed_bar_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(session_id, trade_date)
);

CREATE TABLE IF NOT EXISTS paper_v2.order_execution_state (
    execution_state_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES paper_v2.trade_session(session_id),
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    algo_code TEXT NOT NULL,
    algo_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    plan_json JSONB,
    plan_sha256 TEXT,
    last_processed_bar_time TIMESTAMPTZ,
    filled_quantity INTEGER NOT NULL CHECK (filled_quantity >= 0),
    remaining_quantity INTEGER NOT NULL CHECK (remaining_quantity >= 0),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(order_id)
);

CREATE TABLE IF NOT EXISTS paper_v2.intraday_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES paper_v2.trade_session(session_id),
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    trade_date DATE NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    cash DOUBLE PRECISION NOT NULL,
    market_value DOUBLE PRECISION NOT NULL,
    nav DOUBLE PRECISION NOT NULL,
    positions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, snapshot_time)
);

CREATE TABLE IF NOT EXISTS paper_v2.session_events (
    event_id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES paper_v2.trade_session(session_id),
    run_id TEXT,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.orders (
    order_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    portfolio_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stock_name TEXT,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    order_type TEXT NOT NULL,
    limit_price DOUBLE PRECISION,
    status TEXT NOT NULL,
    filled_quantity INTEGER NOT NULL,
    avg_fill_price DOUBLE PRECISION,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_v2.order_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    fill_json JSONB
);

CREATE TABLE IF NOT EXISTS paper_v2.fills (
    fill_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stock_name TEXT,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    bar_time TIMESTAMPTZ,
    reason TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS paper_v2.cash_ledger (
    cash_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    portfolio_id TEXT NOT NULL,
    fill_id TEXT,
    trade_date DATE NOT NULL,
    symbol TEXT,
    stock_name TEXT,
    side TEXT,
    notional NUMERIC(20, 6) NOT NULL,
    fee NUMERIC(20, 6) NOT NULL,
    cash_delta NUMERIC(20, 6) NOT NULL,
    cash_after NUMERIC(20, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.positions (
    position_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    portfolio_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    stock_name TEXT,
    quantity INTEGER NOT NULL,
    available_quantity INTEGER NOT NULL,
    avg_cost DOUBLE PRECISION NOT NULL,
    market_price DOUBLE PRECISION NOT NULL,
    market_value DOUBLE PRECISION NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(run_id, symbol)
);

CREATE TABLE IF NOT EXISTS paper_v2.daily_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    portfolio_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    cash DOUBLE PRECISION NOT NULL,
    market_value DOUBLE PRECISION NOT NULL,
    nav DOUBLE PRECISION NOT NULL,
    position_count INTEGER NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(portfolio_id, trade_date)
);

CREATE TABLE IF NOT EXISTS paper_v2.run_events (
    event_seq BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.errors (
    error_id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    portfolio_id TEXT,
    error_code TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.reset_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES paper_v2.portfolio(portfolio_id),
    rerun_policy TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    confirm_text TEXT NOT NULL,
    deleted_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_pkg_source ON strategy_pkg.package(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_promotion_review_status ON strategy_pkg.promotion_review(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_selection_artifact_package ON strategy_pkg.selection_score_artifact(package_id, manifest_sha256, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_exec_policy_package ON strategy_pkg.validated_execution_policy(package_id, paper_enabled);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_live_approval_package_status ON strategy_pkg.live_approval(package_id, approval_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_live_approval_portfolio_status ON strategy_pkg.live_approval(portfolio_id, approval_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_model_state_status ON strategy_pkg.model_state(staleness_status, train_end_date);
CREATE INDEX IF NOT EXISTS idx_strategy_pkg_model_retrain_job_package ON strategy_pkg.model_retrain_job(package_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_selection_run_date ON selection.run(trade_date, status);
CREATE INDEX IF NOT EXISTS idx_selection_pkg_result ON selection.package_result(package_id, manifest_sha256, symbol);
CREATE INDEX IF NOT EXISTS idx_selection_excluded_run ON selection.excluded_result(run_id, package_id, raw_rank);
CREATE INDEX IF NOT EXISTS idx_selection_paper_link_run ON selection.paper_portfolio_link(run_id, portfolio_id);
CREATE INDEX IF NOT EXISTS idx_paper_v2_portfolio_package ON paper_v2.portfolio(package_id, manifest_sha256);
CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_exec_policy_activation_active ON paper_v2.execution_policy_activation(portfolio_id, trade_date) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_paper_v2_exec_policy_activation_portfolio ON paper_v2.execution_policy_activation(portfolio_id, trade_date DESC, activated_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_profile_portfolio ON paper_v2.runtime_profile(portfolio_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_profile_version_profile ON paper_v2.runtime_profile_version(profile_id, version_no DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_runtime_config_activation_active ON paper_v2.runtime_config_activation(portfolio_id, trade_date) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_config_activation_portfolio ON paper_v2.runtime_config_activation(portfolio_id, trade_date DESC, activated_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_config_change_audit_portfolio ON paper_v2.config_change_audit(portfolio_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_run_portfolio_date ON paper_v2.run(portfolio_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_paper_v2_trade_session_portfolio ON paper_v2.trade_session(portfolio_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_session_day_session ON paper_v2.session_day(session_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_paper_v2_order_execution_state_session ON paper_v2.order_execution_state(session_id, run_id);
CREATE INDEX IF NOT EXISTS idx_paper_v2_intraday_snapshots_session ON paper_v2.intraday_snapshots(session_id, trade_date, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_paper_v2_session_events_session ON paper_v2.session_events(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_v2_fills_run ON paper_v2.fills(run_id);
CREATE INDEX IF NOT EXISTS idx_paper_v2_positions_portfolio_date ON paper_v2.positions(portfolio_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_paper_v2_reset_audit_portfolio ON paper_v2.reset_audit(portfolio_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_pkg_source_version ON strategy_pkg.package(source_type, source_id, COALESCE(loop_id, ''), package_version);
CREATE INDEX IF NOT EXISTS idx_dataset_refresh_audit_date ON market.dataset_date_refresh_audit(dataset, trade_date, status);
CREATE INDEX IF NOT EXISTS idx_dataset_refresh_audit_latest_success ON market.dataset_date_refresh_audit(dataset, status, trade_date DESC);
