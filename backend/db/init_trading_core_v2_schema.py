"""DB schema bootstrap for Strategy Package, Selection Center, and Paper v2.

This module is intentionally separate from business code. Runtime services should
not create or alter tables implicitly; operators can run this bootstrap or the
matching SQL migration explicitly.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    from .pg_pool import get_conn
except ImportError:  # pragma: no cover - direct script execution convenience.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from backend.db.pg_pool import get_conn

DDL: list[str] = [
    "CREATE SCHEMA IF NOT EXISTS strategy_pkg",
    "CREATE SCHEMA IF NOT EXISTS selection",
    "CREATE SCHEMA IF NOT EXISTS paper_v2",
    "CREATE SCHEMA IF NOT EXISTS trading_core",
    """
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
    )
    """,
    "COMMENT ON TABLE market.dataset_date_refresh_audit IS 'AIstock dataset/date readiness ledger used by local data management, Selection Center, and Paper v2 fail-fast data gates.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.dataset IS 'Logical dataset key, for example suspend_d, stk_limit, kline_daily_raw, or sector_data.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.trade_date IS 'Trading date or effective dataset date that this readiness row describes.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.data_source IS 'Provider or process that produced the readiness row, such as tushare, tdx_api, sector_builder, or seed_existing_rows.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.job_id IS 'Optional market.ingestion_jobs.job_id that produced the latest readiness state for this dataset/date/source.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.status IS 'Readiness status; success means the dataset/date/source is usable, failed means it must not pass Paper v2/local data gates.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.row_count IS 'Final usable row count present for this dataset/date after the refresh attempt completed.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.refreshed_at IS 'Timestamp when the readiness row was written or updated by the refresh/audit process.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.error_message IS 'Provider, validation, or persistence error message for failed refresh attempts.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.data_max_at IS 'Precise maximum source data timestamp covered by this row for intraday or timestamped datasets; NULL for date-only datasets.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.written_rows IS 'Rows written or touched by the latest refresh attempt for this dataset/date/source; NULL when unknown.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.expected_rows IS 'Optional expected usable row count for coverage checks on this dataset/date.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.coverage_ratio IS 'Optional row_count divided by expected_rows; values below dataset policy thresholds indicate low coverage.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.quality_status IS 'Quality classification such as ok, unknown, empty_valid, empty_invalid, low_coverage, upstream_not_published, provider_unavailable, or error.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.failure_category IS 'Machine-readable failure reason for retry/self-healing decisions, for example audit_stale, empty_invalid, or provider_unavailable.'",
    "COMMENT ON COLUMN market.dataset_date_refresh_audit.metadata IS 'Additional JSON context including API name, ingestion mode, table, source script, and validation notes.'",
    """
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
    )
    """,
    "ALTER TABLE strategy_pkg.package DROP CONSTRAINT IF EXISTS package_source_type_check",
    """
    ALTER TABLE strategy_pkg.package
        ADD CONSTRAINT package_source_type_check
        CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package'))
    """,
    """
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
    )
    """,
    "ALTER TABLE strategy_pkg.candidate_strategy_package DROP CONSTRAINT IF EXISTS candidate_strategy_package_source_type_check",
    """
    ALTER TABLE strategy_pkg.candidate_strategy_package
        ADD CONSTRAINT candidate_strategy_package_source_type_check
        CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package'))
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_pkg_candidate_source
        ON strategy_pkg.candidate_strategy_package(source_type, source_id, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_pkg_candidate_status
        ON strategy_pkg.candidate_strategy_package(status, created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_pkg.candidate_strategy_package_audit (
        audit_id BIGSERIAL PRIMARY KEY,
        candidate_id TEXT NOT NULL REFERENCES strategy_pkg.candidate_strategy_package(candidate_id) ON DELETE RESTRICT,
        action TEXT NOT NULL,
        actor TEXT NOT NULL,
        context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_pkg.package_status_event (
        event_id BIGSERIAL PRIMARY KEY,
        package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
        from_status TEXT,
        to_status TEXT NOT NULL,
        reason TEXT,
        context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_pkg.package_asset (
        asset_id BIGSERIAL PRIMARY KEY,
        package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
        asset_type TEXT NOT NULL,
        asset_ref TEXT NOT NULL,
        asset_sha256 TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    "ALTER TABLE selection.run ADD COLUMN IF NOT EXISTS package_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
    """
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_v2.portfolio (
        portfolio_id TEXT PRIMARY KEY,
        portfolio_name TEXT NOT NULL,
        package_id TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        frozen_manifest_json JSONB NOT NULL,
        initial_cash NUMERIC(20, 6) NOT NULL CHECK (initial_cash > 0),
        start_date DATE NOT NULL,
        data_source TEXT NOT NULL CHECK (data_source IN ('TDX_REALTIME', 'DB_HISTORICAL', 'MINIQMT_REALTIME')),
        broker_backend VARCHAR(32) NOT NULL DEFAULT 'local_sim'
            CHECK (broker_backend IN ('local_sim', 'minqmt_sim')),
        fee_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
        risk_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
        execution_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT portfolio_broker_market_source_check CHECK (
            (broker_backend = 'local_sim' AND data_source IN ('TDX_REALTIME', 'DB_HISTORICAL'))
            OR (broker_backend = 'minqmt_sim' AND data_source = 'MINIQMT_REALTIME')
        )
    )
    """,
    "ALTER TABLE paper_v2.portfolio ADD COLUMN IF NOT EXISTS broker_backend VARCHAR(32) NOT NULL DEFAULT 'local_sim'",
    "ALTER TABLE paper_v2.portfolio DROP CONSTRAINT IF EXISTS portfolio_broker_backend_check",
    """
    ALTER TABLE paper_v2.portfolio
        ADD CONSTRAINT portfolio_broker_backend_check
        CHECK (broker_backend IN ('local_sim', 'minqmt_sim'))
    """,
    "ALTER TABLE paper_v2.portfolio DROP CONSTRAINT IF EXISTS portfolio_broker_market_source_check",
    """
    ALTER TABLE paper_v2.portfolio
        ADD CONSTRAINT portfolio_broker_market_source_check CHECK (
            (broker_backend = 'local_sim' AND data_source IN ('TDX_REALTIME', 'DB_HISTORICAL'))
            OR (broker_backend = 'minqmt_sim' AND data_source = 'MINIQMT_REALTIME')
        )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
        model_params_origin VARCHAR(16) NOT NULL DEFAULT 'node'
            CHECK (model_params_origin IN ('node', 'cache', 'unavailable')),
        UNIQUE(portfolio_id, trade_date)
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_v2.session_events (
        event_id BIGSERIAL PRIMARY KEY,
        session_id TEXT NOT NULL REFERENCES paper_v2.trade_session(session_id),
        run_id TEXT,
        event_type TEXT NOT NULL,
        message TEXT NOT NULL,
        context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_v2.order_events (
        event_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
        order_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        reason TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        fill_json JSONB
    )
    """,
    """
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
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        intended_price NUMERIC(18, 4),
        fill_market_context JSONB
    )
    """,
    """
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
    )
    """,
    """
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
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(run_id, symbol)
    )
    """,
    """
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
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(portfolio_id, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_v2.run_events (
        event_seq BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES paper_v2.run(run_id),
        event_type TEXT NOT NULL,
        message TEXT NOT NULL,
        context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_v2.errors (
        error_id BIGSERIAL PRIMARY KEY,
        run_id TEXT,
        portfolio_id TEXT,
        error_code TEXT NOT NULL,
        message TEXT NOT NULL,
        context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
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
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_strategy_pkg_source ON strategy_pkg.package(source_type, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_pkg_selection_artifact_package ON strategy_pkg.selection_score_artifact(package_id, manifest_sha256, trade_date DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_pkg_source_version ON strategy_pkg.package(source_type, source_id, COALESCE(loop_id, ''), package_version)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_pkg_exec_policy_package ON strategy_pkg.validated_execution_policy(package_id, paper_enabled)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_pkg_model_state_status ON strategy_pkg.model_state(staleness_status, train_end_date)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_pkg_model_retrain_job_package ON strategy_pkg.model_retrain_job(package_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_selection_run_date ON selection.run(trade_date, status)",
    "CREATE INDEX IF NOT EXISTS idx_selection_pkg_result ON selection.package_result(package_id, manifest_sha256, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_selection_excluded_run ON selection.excluded_result(run_id, package_id, raw_rank)",
    "CREATE INDEX IF NOT EXISTS idx_selection_paper_link_run ON selection.paper_portfolio_link(run_id, portfolio_id)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_portfolio_package ON paper_v2.portfolio(package_id, manifest_sha256)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_exec_policy_activation_active ON paper_v2.execution_policy_activation(portfolio_id, trade_date) WHERE status = 'ACTIVE'",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_exec_policy_activation_portfolio ON paper_v2.execution_policy_activation(portfolio_id, trade_date DESC, activated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_profile_portfolio ON paper_v2.runtime_profile(portfolio_id, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_profile_version_profile ON paper_v2.runtime_profile_version(profile_id, version_no DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_runtime_config_activation_active ON paper_v2.runtime_config_activation(portfolio_id, trade_date) WHERE status = 'ACTIVE'",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_runtime_config_activation_portfolio ON paper_v2.runtime_config_activation(portfolio_id, trade_date DESC, activated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_config_change_audit_portfolio ON paper_v2.config_change_audit(portfolio_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_run_portfolio_date ON paper_v2.run(portfolio_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_trade_session_portfolio ON paper_v2.trade_session(portfolio_id, status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_session_day_session ON paper_v2.session_day(session_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_order_execution_state_session ON paper_v2.order_execution_state(session_id, run_id)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_intraday_snapshots_session ON paper_v2.intraday_snapshots(session_id, trade_date, snapshot_time)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_session_events_session ON paper_v2.session_events(session_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_fills_run ON paper_v2.fills(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_positions_portfolio_date ON paper_v2.positions(portfolio_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_paper_v2_reset_audit_portfolio ON paper_v2.reset_audit(portfolio_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_dataset_refresh_audit_date ON market.dataset_date_refresh_audit(dataset, trade_date, status)",
    "CREATE INDEX IF NOT EXISTS idx_dataset_refresh_audit_latest_success ON market.dataset_date_refresh_audit(dataset, status, trade_date DESC)",
]


def iter_ddl() -> Iterable[str]:
    return tuple(DDL)


def init_trading_core_v2_schema() -> None:
    """Create the Trading Core v2 schemas and tables explicitly."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    from pathlib import Path

    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    init_trading_core_v2_schema()
