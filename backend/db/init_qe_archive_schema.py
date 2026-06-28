"""Explicit schema bootstrap for the QE realtime experiment warehouse.

Business services must not create these tables implicitly. Run this module as an
operator/bootstrap step, or copy the DDL into a reviewed migration.
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


QE_ARCHIVE_SCHEMA_VERSION = "qe_archive_v3_20260628"


BASE_DDL: list[str] = [
    "CREATE SCHEMA IF NOT EXISTS qe_archive",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.schema_version (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        description TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run (
        run_id TEXT PRIMARY KEY,
        logical_experiment_id TEXT NOT NULL,
        attempt_no INTEGER NOT NULL DEFAULT 1,
        is_latest_attempt BOOLEAN NOT NULL DEFAULT TRUE,
        source_system TEXT NOT NULL,
        run_type TEXT NOT NULL,
        task_id TEXT,
        loop_id TEXT,
        loop_index INTEGER,
        experiment_id TEXT,
        node_id TEXT,
        model_catalog_id BIGINT,
        model_family TEXT,
        model_type TEXT,
        factor_set_hash TEXT,
        factor_count INTEGER,
        freq TEXT,
        label_horizon INTEGER,
        status TEXT NOT NULL,
        research_valid BOOLEAN NOT NULL DEFAULT TRUE,
        invalid_reason TEXT,
        exclusion_tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        score_total DOUBLE PRECISION,
        score_version TEXT,
        priority_rank INTEGER,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        archived_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source_created_at TIMESTAMPTZ,
        source_updated_at TIMESTAMPTZ,
        CONSTRAINT uq_qear_run_logical_attempt UNIQUE (logical_experiment_id, attempt_no),
        CONSTRAINT ck_qear_run_status CHECK (
            status IN ('pending','running','completed','failed','interrupted','partial_archived','archived','succeeded','partial_failed')
        )
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_latest
        ON qe_archive.run(logical_experiment_id)
        WHERE is_latest_attempt = TRUE
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_run_valid_score
        ON qe_archive.run(research_valid, score_total DESC NULLS LAST, completed_at DESC)
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_run_task_loop ON qe_archive.run(task_id, loop_index, loop_id)",
    """
    CREATE INDEX IF NOT EXISTS idx_qear_run_model
        ON qe_archive.run(model_family, model_type, label_horizon, freq, completed_at DESC)
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_run_factor_hash ON qe_archive.run(factor_set_hash, completed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_qear_run_type_status ON qe_archive.run(run_type, source_system, status)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_run (
        run_id TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        roster_hash TEXT NOT NULL,
        oos_start DATE NOT NULL,
        oos_end DATE NOT NULL,
        normalize_method TEXT NOT NULL,
        walk_forward_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        baseline_leg_id TEXT,
        leg_count INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        logical_status TEXT,
        reason_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_created_at TIMESTAMPTZ,
        archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_macb_run_window CHECK (oos_end >= oos_start),
        CONSTRAINT ck_qear_macb_run_status CHECK (status IN ('succeeded','partial_failed','failed')),
        CONSTRAINT ck_qear_macb_run_logical_status CHECK (logical_status IS NULL OR logical_status IN ('succeeded','partial_failed','failed')),
        CONSTRAINT ck_qear_macb_run_leg_count CHECK (leg_count >= 0),
        CONSTRAINT ck_qear_macb_run_walk_forward_json CHECK (jsonb_typeof(walk_forward_json) = 'object'),
        CONSTRAINT ck_qear_macb_run_reason_json CHECK (jsonb_typeof(reason_json) = 'object')
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_run_roster ON qe_archive.multi_alpha_run(roster_hash, oos_start, oos_end)",
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_run_status ON qe_archive.multi_alpha_run(status, archived_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg (
        run_id TEXT NOT NULL REFERENCES qe_archive.multi_alpha_run(run_id) ON DELETE CASCADE,
        leg_id TEXT NOT NULL,
        leg_order INTEGER NOT NULL,
        seed_run_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
        factor_set_hash TEXT,
        factor_names JSONB NOT NULL DEFAULT '[]'::jsonb,
        factor_count INTEGER NOT NULL DEFAULT 0,
        model_type TEXT,
        model_family TEXT,
        freq TEXT,
        label_horizon INTEGER,
        seed_count INTEGER NOT NULL DEFAULT 0,
        source_run_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        provenance_complete BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (run_id, leg_id),
        CONSTRAINT ck_qear_macb_leg_order CHECK (leg_order >= 0),
        CONSTRAINT ck_qear_macb_leg_seed_run_ids CHECK (jsonb_typeof(seed_run_ids) = 'array'),
        CONSTRAINT ck_qear_macb_leg_factor_names CHECK (jsonb_typeof(factor_names) = 'array'),
        CONSTRAINT ck_qear_macb_leg_factor_count CHECK (factor_count >= 0),
        CONSTRAINT ck_qear_macb_leg_seed_count CHECK (seed_count >= 0),
        CONSTRAINT ck_qear_macb_leg_source_run_meta CHECK (jsonb_typeof(source_run_meta) = 'object')
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_factor_hash ON qe_archive.multi_alpha_leg(factor_set_hash)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg_source (
        run_id TEXT NOT NULL,
        leg_id TEXT NOT NULL,
        source_seq INTEGER NOT NULL,
        seed_ref TEXT NOT NULL,
        seed_ref_kind TEXT NOT NULL,
        source_experiment_id TEXT,
        source_task_id TEXT,
        source_loop_id TEXT,
        source_loop_index INTEGER,
        source_run_type TEXT,
        source_model_type TEXT,
        source_factor_set_hash TEXT,
        resolved BOOLEAN NOT NULL DEFAULT FALSE,
        resolve_method TEXT NOT NULL,
        resolve_note TEXT,
        PRIMARY KEY (run_id, leg_id, source_seq),
        FOREIGN KEY (run_id, leg_id) REFERENCES qe_archive.multi_alpha_leg(run_id, leg_id) ON DELETE CASCADE,
        CONSTRAINT ck_qear_macb_leg_source_seq CHECK (source_seq >= 1),
        CONSTRAINT ck_qear_macb_leg_source_kind CHECK (seed_ref_kind IN ('archive_run_id','evolution_loop_id','unknown')),
        CONSTRAINT ck_qear_macb_leg_source_resolved CHECK (
            (resolved = TRUE AND source_experiment_id IS NOT NULL AND source_loop_id IS NOT NULL AND source_loop_index IS NOT NULL AND source_run_type IS NOT NULL)
            OR (resolved = FALSE AND resolve_note IS NOT NULL)
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_source_exp_loop ON qe_archive.multi_alpha_leg_source(source_experiment_id, source_loop_id, source_loop_index)",
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_source_seed ON qe_archive.multi_alpha_leg_source(seed_ref)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_scheme (
        run_id TEXT NOT NULL REFERENCES qe_archive.multi_alpha_run(run_id) ON DELETE CASCADE,
        weighting_scheme TEXT NOT NULL,
        scheme_algorithm TEXT NOT NULL,
        weights_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        per_window_weights_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        cagr DOUBLE PRECISION,
        max_drawdown DOUBLE PRECISION,
        sharpe DOUBLE PRECISION,
        calmar DOUBLE PRECISION,
        topk_return_20 DOUBLE PRECISION,
        topk_hit_rate_20 DOUBLE PRECISION,
        turnover DOUBLE PRECISION,
        vs_baseline_sharpe_delta DOUBLE PRECISION,
        vs_baseline_calmar_delta DOUBLE PRECISION,
        pred_persisted BOOLEAN NOT NULL DEFAULT FALSE,
        skipped BOOLEAN NOT NULL DEFAULT FALSE,
        skipped_reason TEXT,
        is_best BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (run_id, weighting_scheme),
        CONSTRAINT ck_qear_macb_scheme_weights_json CHECK (jsonb_typeof(weights_json) = 'object'),
        CONSTRAINT ck_qear_macb_scheme_window_weights_json CHECK (jsonb_typeof(per_window_weights_json) = 'array'),
        CONSTRAINT ck_qear_macb_scheme_skip_reason CHECK ((skipped = FALSE) OR skipped_reason IS NOT NULL)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_scheme_best ON qe_archive.multi_alpha_scheme(is_best, sharpe DESC NULLS LAST)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_loo (
        run_id TEXT NOT NULL,
        weighting_scheme TEXT NOT NULL,
        dropped_leg_id TEXT NOT NULL,
        marginal_cagr DOUBLE PRECISION,
        marginal_sharpe DOUBLE PRECISION,
        marginal_calmar DOUBLE PRECISION,
        PRIMARY KEY (run_id, weighting_scheme, dropped_leg_id),
        FOREIGN KEY (run_id, weighting_scheme) REFERENCES qe_archive.multi_alpha_scheme(run_id, weighting_scheme) ON DELETE CASCADE,
        FOREIGN KEY (run_id, dropped_leg_id) REFERENCES qe_archive.multi_alpha_leg(run_id, leg_id) ON DELETE CASCADE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_macb_loo_leg ON qe_archive.multi_alpha_loo(run_id, dropped_leg_id)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_source (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        source_system TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_sub_id TEXT,
        source_status TEXT,
        source_uri TEXT,
        recorder_experiment_id TEXT,
        recorder_id TEXT,
        mlflow_tracking_uri TEXT,
        mlflow_artifact_uri TEXT,
        qlib_recorder_name TEXT,
        node_api_base_url TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_source_source
        ON qe_archive.run_source(source_system, source_type, source_id, COALESCE(source_sub_id, ''))
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_run_source_run ON qe_archive.run_source(run_id)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_config (
        run_id TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        config_schema_version TEXT NOT NULL,
        config_sha256 TEXT NOT NULL,
        canonical_config JSONB NOT NULL,
        raw_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        factor_list JSONB NOT NULL DEFAULT '[]'::jsonb,
        factor_set_hash TEXT,
        model_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        model_params JSONB NOT NULL DEFAULT '{}'::jsonb,
        strategy_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        backtest_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        data_split JSONB NOT NULL DEFAULT '{}'::jsonb,
        execution_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        runtime_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
        agent_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        config_capture_complete BOOLEAN NOT NULL DEFAULT FALSE,
        config_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
        missing_config_items JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "UPDATE qe_archive.run_config SET raw_config = '{}'::jsonb WHERE raw_config IS NULL",
    "ALTER TABLE qe_archive.run_config ALTER COLUMN raw_config SET DEFAULT '{}'::jsonb",
    "ALTER TABLE qe_archive.run_config ALTER COLUMN raw_config SET NOT NULL",
    "ALTER TABLE qe_archive.run_config ADD COLUMN IF NOT EXISTS config_capture_complete BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE qe_archive.run_config ADD COLUMN IF NOT EXISTS config_provenance JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE qe_archive.run_config ADD COLUMN IF NOT EXISTS missing_config_items JSONB NOT NULL DEFAULT '[]'::jsonb",
    "CREATE INDEX IF NOT EXISTS idx_qear_run_config_hash ON qe_archive.run_config(config_sha256)",
    "CREATE INDEX IF NOT EXISTS gin_qear_run_config_canonical ON qe_archive.run_config USING GIN(canonical_config)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_reproducibility_manifest (
        run_id TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        manifest_schema_version TEXT NOT NULL,
        reproducibility_level TEXT NOT NULL,
        verification_status TEXT NOT NULL DEFAULT 'not_verified',
        config_sha256 TEXT,
        canonical_config_sha256 TEXT,
        raw_config_sha256 TEXT,
        factor_set_hash TEXT,
        qlib_config_sha256 TEXT,
        model_params_sha256 TEXT,
        strategy_config_sha256 TEXT,
        data_context_sha256 TEXT,
        metrics_payload_sha256 TEXT,
        enhanced_metrics_sha256 TEXT,
        artifact_manifest_sha256 TEXT,
        git_commit TEXT,
        git_dirty BOOLEAN,
        runner_script TEXT,
        runner_script_sha256 TEXT,
        python_version TEXT,
        qlib_version TEXT,
        mlflow_version TEXT,
        torch_version TEXT,
        package_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
        random_seed BIGINT,
        deterministic_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_config_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
        required_artifact_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        missing_items JSONB NOT NULL DEFAULT '[]'::jsonb,
        manifest_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_repro_level CHECK (reproducibility_level IN ('full','partial','audit_only')),
        CONSTRAINT ck_qear_repro_status CHECK (
            verification_status IN ('not_verified','verified','failed','not_reproducible')
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_repro_level
        ON qe_archive.run_reproducibility_manifest(reproducibility_level, verification_status)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_data_context (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        context_type TEXT NOT NULL DEFAULT 'primary',
        freq TEXT,
        market TEXT,
        universe TEXT,
        benchmark TEXT,
        train_start DATE,
        train_end DATE,
        valid_start DATE,
        valid_end DATE,
        test_start DATE,
        test_end DATE,
        backtest_start DATE,
        backtest_end DATE,
        label_horizon INTEGER,
        qlib_provider_uri TEXT,
        qlib_dataset_version TEXT,
        dataset_snapshot_id TEXT,
        feature_snapshot_id TEXT,
        factor_cache_snapshot_id TEXT,
        data_version_hash TEXT,
        pit_cutoff_date DATE,
        limit_handling TEXT,
        suspend_handling TEXT,
        limit_suspend_authoritative BOOLEAN NOT NULL DEFAULT FALSE,
        cost_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        stock_pool_config JSONB NOT NULL DEFAULT '{}'::jsonb,
        data_quality_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_data_context_run ON qe_archive.run_data_context(run_id)",
    """
    CREATE INDEX IF NOT EXISTS idx_qear_data_context_dates
        ON qe_archive.run_data_context(freq, backtest_start, backtest_end)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_account_summary (
        run_id TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        initial_capital DOUBLE PRECISION,
        final_total_value DOUBLE PRECISION,
        final_account_value DOUBLE PRECISION,
        final_nav_value DOUBLE PRECISION,
        total_return DOUBLE PRECISION,
        cagr DOUBLE PRECISION,
        max_drawdown DOUBLE PRECISION,
        max_drawdown_date DATE,
        sharpe DOUBLE PRECISION,
        annualized_volatility DOUBLE PRECISION,
        avg_cash_ratio DOUBLE PRECISION,
        final_cash DOUBLE PRECISION,
        final_stock_value DOUBLE PRECISION,
        final_stock_count INTEGER,
        final_cash_ratio DOUBLE PRECISION,
        n_trading_days INTEGER,
        position_count_min DOUBLE PRECISION,
        position_count_avg DOUBLE PRECISION,
        position_count_max DOUBLE PRECISION,
        position_count_p95 DOUBLE PRECISION,
        source_payload_path TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_account_summary_return
        ON qe_archive.run_account_summary(total_return DESC NULLS LAST, max_drawdown ASC NULLS LAST)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.metric_taxonomy (
        metric_key TEXT PRIMARY KEY,
        metric_group TEXT NOT NULL,
        display_name TEXT NOT NULL,
        unit TEXT,
        direction TEXT NOT NULL DEFAULT 'higher_better',
        canonical_description TEXT,
        source_aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_metric (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        metric_key TEXT NOT NULL,
        metric_scope TEXT NOT NULL DEFAULT 'run',
        period_start DATE,
        period_end DATE,
        horizon INTEGER,
        freq TEXT,
        value_num DOUBLE PRECISION,
        value_text TEXT,
        value_json JSONB,
        unit TEXT,
        direction TEXT,
        source_key TEXT,
        source_payload_path TEXT,
        quality_flag TEXT NOT NULL DEFAULT 'ok',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_metric_key_value ON qe_archive.run_metric(metric_key, value_num DESC NULLS LAST)",
    "CREATE INDEX IF NOT EXISTS idx_qear_metric_run ON qe_archive.run_metric(run_id, metric_key)",
    "CREATE INDEX IF NOT EXISTS idx_qear_metric_scope_period ON qe_archive.run_metric(metric_scope, period_start, period_end)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_curve (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        curve_key TEXT NOT NULL,
        ts TIMESTAMPTZ,
        trade_date DATE,
        step INTEGER,
        epoch INTEGER,
        split_name TEXT,
        value_num DOUBLE PRECISION,
        value_json JSONB,
        source_key TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_curve_run_key_ts ON qe_archive.run_curve(run_id, curve_key, ts, step)",
    "CREATE INDEX IF NOT EXISTS idx_qear_curve_key_date ON qe_archive.run_curve(curve_key, trade_date)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_factor (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        factor_catalog_id BIGINT,
        factor_name TEXT NOT NULL,
        factor_source TEXT,
        factor_version TEXT,
        factor_order INTEGER,
        factor_group TEXT,
        factor_classification JSONB NOT NULL DEFAULT '{}'::jsonb,
        factor_expression_hash TEXT,
        factor_asset_hash TEXT,
        inclusion_reason TEXT,
        inclusion_source TEXT,
        is_alpha158 BOOLEAN NOT NULL DEFAULT FALSE,
        independent_metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        official_rating_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        correlation_cluster TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_factor
        ON qe_archive.run_factor(run_id, factor_name, COALESCE(factor_source, ''))
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_factor_name ON qe_archive.run_factor(factor_name, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_qear_factor_catalog ON qe_archive.run_factor(factor_catalog_id)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_factor_importance (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        factor_catalog_id BIGINT,
        factor_name TEXT NOT NULL,
        feature_name TEXT,
        feature_index INTEGER,
        model_family TEXT,
        model_type TEXT,
        method TEXT NOT NULL,
        method_version TEXT,
        split_name TEXT,
        time_bucket TEXT,
        epoch INTEGER,
        step INTEGER,
        importance_value DOUBLE PRECISION NOT NULL,
        normalized_value DOUBLE PRECISION,
        weight_pct DOUBLE PRECISION,
        signed_value DOUBLE PRECISION,
        rank_in_run INTEGER,
        sample_count INTEGER,
        reliability TEXT NOT NULL DEFAULT 'unknown',
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_importance_run ON qe_archive.run_factor_importance(run_id, method, split_name)",
    "CREATE INDEX IF NOT EXISTS idx_qear_importance_factor ON qe_archive.run_factor_importance(factor_name, method, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_factor_pair (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        factor_a_catalog_id BIGINT,
        factor_b_catalog_id BIGINT,
        factor_a_name TEXT NOT NULL,
        factor_b_name TEXT NOT NULL,
        corr_method TEXT NOT NULL DEFAULT 'spearman',
        corr_value DOUBLE PRECISION,
        corr_as_of_date DATE,
        corr_window TEXT,
        same_cluster BOOLEAN,
        synergy_score DOUBLE PRECISION,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_factor_pair_order CHECK (factor_a_name < factor_b_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_factor_pair_run ON qe_archive.run_factor_pair(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_qear_factor_pair_names ON qe_archive.run_factor_pair(factor_a_name, factor_b_name)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_symbol_summary (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        symbol TEXT NOT NULL,
        source_list TEXT NOT NULL DEFAULT 'all_stocks',
        profit DOUBLE PRECISION,
        profit_pct DOUBLE PRECISION,
        avg_cost DOUBLE PRECISION,
        last_price DOUBLE PRECISION,
        holding_days INTEGER,
        first_date DATE,
        last_date DATE,
        rank_in_list INTEGER,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_symbol_summary
        ON qe_archive.run_symbol_summary(run_id, source_list, symbol)
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_symbol_summary_symbol ON qe_archive.run_symbol_summary(symbol, first_date, last_date)",
    "CREATE INDEX IF NOT EXISTS idx_qear_symbol_summary_profit ON qe_archive.run_symbol_summary(run_id, profit DESC NULLS LAST)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_model_trial (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        model_catalog_id BIGINT,
        model_family TEXT NOT NULL,
        model_type TEXT NOT NULL,
        trial_source TEXT NOT NULL DEFAULT 'qe',
        optimizer_name TEXT,
        optimizer_study_name TEXT,
        optimizer_trial_number INTEGER,
        search_space JSONB NOT NULL DEFAULT '{}'::jsonb,
        params JSONB NOT NULL DEFAULT '{}'::jsonb,
        fixed_params JSONB NOT NULL DEFAULT '{}'::jsonb,
        objective_name TEXT,
        objective_value DOUBLE PRECISION,
        objective_values JSONB,
        score_total DOUBLE PRECISION,
        score_version TEXT,
        trial_state TEXT NOT NULL DEFAULT 'complete',
        pruned_reason TEXT,
        train_wall_seconds DOUBLE PRECISION,
        gpu_info JSONB NOT NULL DEFAULT '{}'::jsonb,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_model_trial_model_score
        ON qe_archive.run_model_trial(model_family, model_type, score_total DESC NULLS LAST, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_model_trial_optimizer
        ON qe_archive.run_model_trial(optimizer_name, optimizer_study_name, optimizer_trial_number)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_model_training_metric (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        trial_id BIGINT REFERENCES qe_archive.run_model_trial(id) ON DELETE CASCADE,
        metric_key TEXT NOT NULL,
        split_name TEXT,
        epoch INTEGER,
        step INTEGER,
        value_num DOUBLE PRECISION,
        value_json JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_training_metric_run
        ON qe_archive.run_model_training_metric(run_id, metric_key, split_name, epoch, step)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_position (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        trade_date DATE NOT NULL,
        symbol TEXT NOT NULL,
        weight DOUBLE PRECISION,
        shares DOUBLE PRECISION,
        price DOUBLE PRECISION,
        score DOUBLE PRECISION,
        rank_in_portfolio INTEGER,
        return_contribution DOUBLE PRECISION,
        industry_code TEXT,
        industry_name TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_position_run_date ON qe_archive.run_position(run_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_qear_position_symbol_date ON qe_archive.run_position(symbol, trade_date)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_order (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        order_uid TEXT,
        trade_date DATE,
        ts TIMESTAMPTZ,
        symbol TEXT NOT NULL,
        side TEXT,
        target_weight DOUBLE PRECISION,
        target_qty DOUBLE PRECISION,
        limit_price DOUBLE PRECISION,
        order_price DOUBLE PRECISION,
        order_qty DOUBLE PRECISION,
        status TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_order_run_date ON qe_archive.run_order(run_id, trade_date, ts)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_trade (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        trade_uid TEXT,
        order_uid TEXT,
        trade_date DATE,
        ts TIMESTAMPTZ,
        symbol TEXT NOT NULL,
        side TEXT,
        price DOUBLE PRECISION,
        quantity DOUBLE PRECISION,
        amount DOUBLE PRECISION,
        commission DOUBLE PRECISION,
        tax DOUBLE PRECISION,
        slippage DOUBLE PRECISION,
        pnl DOUBLE PRECISION,
        source_payload_path TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_trade_run_date ON qe_archive.run_trade(run_id, trade_date, ts)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_execution_event (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        trade_date DATE,
        symbol TEXT,
        event_type TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'info',
        message TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_exec_event_run ON qe_archive.run_execution_event(run_id, event_type, event_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_qear_exec_event_symbol ON qe_archive.run_execution_event(symbol, trade_date, event_type)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_artifact (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        artifact_type TEXT NOT NULL,
        artifact_name TEXT NOT NULL,
        storage_tier TEXT NOT NULL DEFAULT 'local_hot',
        artifact_uri TEXT NOT NULL,
        local_rel_path TEXT,
        source_system TEXT,
        source_uri TEXT,
        source_node_id TEXT,
        sha256 TEXT,
        size_bytes BIGINT,
        content_type TEXT,
        compression TEXT,
        collected_status TEXT NOT NULL DEFAULT 'pending',
        collected_at TIMESTAMPTZ,
        parser_status TEXT NOT NULL DEFAULT 'not_required',
        parser_error TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_artifact_run_type ON qe_archive.run_artifact(run_id, artifact_type, collected_status)",
    "CREATE INDEX IF NOT EXISTS idx_qear_artifact_sha ON qe_archive.run_artifact(sha256)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.raw_payload (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        payload_type TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_id TEXT,
        payload_sha256 TEXT,
        payload_json JSONB,
        payload_text TEXT,
        provenance_level TEXT NOT NULL DEFAULT 'direct',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_raw_payload_run ON qe_archive.raw_payload(run_id, payload_type, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS gin_qear_raw_payload_json ON qe_archive.raw_payload USING GIN(payload_json)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.run_priority_score (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
        score_version TEXT NOT NULL,
        score_total DOUBLE PRECISION,
        alpha_score DOUBLE PRECISION,
        return_score DOUBLE PRECISION,
        risk_score DOUBLE PRECISION,
        stability_score DOUBLE PRECISION,
        execution_score DOUBLE PRECISION,
        novelty_score DOUBLE PRECISION,
        data_quality_score DOUBLE PRECISION,
        penalty_score DOUBLE PRECISION,
        score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
        exclusion_reason TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_priority_run_version ON qe_archive.run_priority_score(run_id, score_version)",
    "CREATE INDEX IF NOT EXISTS idx_qear_priority_score ON qe_archive.run_priority_score(score_version, score_total DESC NULLS LAST)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.optimization_candidate (
        candidate_id TEXT PRIMARY KEY,
        candidate_type TEXT NOT NULL,
        generated_by TEXT NOT NULL,
        generator_version TEXT,
        status TEXT NOT NULL DEFAULT 'proposed',
        priority_score DOUBLE PRECISION,
        model_family TEXT,
        model_type TEXT,
        factor_set_hash TEXT,
        label_horizon INTEGER,
        freq TEXT,
        candidate_config JSONB NOT NULL,
        evidence_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_run_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        submitted_task_id TEXT,
        submitted_loop_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        result_run_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_qear_candidate_status_priority
        ON qe_archive.optimization_candidate(status, priority_score DESC NULLS LAST, created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.agent_query_audit (
        audit_id BIGSERIAL PRIMARY KEY,
        agent_name TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        request_id TEXT,
        user_intent TEXT,
        query_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
        row_count INTEGER,
        token_budget INTEGER,
        allowed BOOLEAN NOT NULL DEFAULT TRUE,
        denial_reason TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_agent_audit_time ON qe_archive.agent_query_audit(agent_name, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.outbox_event (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_sub_id TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'pending',
        retry_count INTEGER NOT NULL DEFAULT 0,
        next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        locked_by TEXT,
        locked_at TIMESTAMPTZ,
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_outbox_pending ON qe_archive.outbox_event(status, next_retry_at, created_at)",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_outbox_source_terminal
        ON qe_archive.outbox_event(event_type, source_system, source_id, COALESCE(source_sub_id, ''))
    """,
    """
    CREATE TABLE IF NOT EXISTS qe_archive.archive_job (
        job_id TEXT PRIMARY KEY,
        event_id TEXT REFERENCES qe_archive.outbox_event(event_id) ON DELETE SET NULL,
        run_id TEXT REFERENCES qe_archive.run(run_id) ON DELETE SET NULL,
        job_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        level TEXT NOT NULL DEFAULT 'A',
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        retry_count INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_archive_job_status ON qe_archive.archive_job(status, level, created_at)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.skip_registry (
        skip_id TEXT PRIMARY KEY,
        source_system TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_sub_id TEXT,
        event_type TEXT,
        archive_policy TEXT NOT NULL,
        archive_policy_source TEXT NOT NULL,
        skip_reason TEXT NOT NULL,
        allow_override BOOLEAN NOT NULL DEFAULT FALSE,
        override_required_token TEXT,
        trigger_reason TEXT NOT NULL,
        payload_sha256 TEXT,
        runtime_config_sha256 TEXT,
        created_by TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_skip_policy CHECK (archive_policy IN ('SKIP','MANUAL_ONLY'))
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_skip_source
        ON qe_archive.skip_registry(source_system, source_type, source_id, COALESCE(source_sub_id, ''))
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_skip_policy ON qe_archive.skip_registry(archive_policy, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.ingest_history (
        history_id TEXT PRIMARY KEY,
        run_id TEXT REFERENCES qe_archive.run(run_id) ON DELETE SET NULL,
        logical_experiment_id TEXT,
        event_id TEXT REFERENCES qe_archive.outbox_event(event_id) ON DELETE SET NULL,
        job_id TEXT REFERENCES qe_archive.archive_job(job_id) ON DELETE SET NULL,
        backfill_run_id TEXT,
        source_system TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_sub_id TEXT,
        trigger_reason TEXT NOT NULL,
        archive_policy TEXT,
        ingest_status TEXT NOT NULL,
        attempt_no INTEGER NOT NULL DEFAULT 1,
        payload_sha256 TEXT,
        runtime_config_sha256 TEXT,
        result_fingerprint TEXT,
        anomaly BOOLEAN NOT NULL DEFAULT FALSE,
        anomaly_reason TEXT,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_message TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        CONSTRAINT ck_qear_ingest_trigger CHECK (trigger_reason IN ('realtime','backfill','retry','manual','rebootstrap')),
        CONSTRAINT ck_qear_ingest_policy CHECK (archive_policy IS NULL OR archive_policy IN ('AUTO','SKIP','MANUAL_ONLY')),
        CONSTRAINT ck_qear_ingest_status CHECK (ingest_status IN ('queued','started','completed','failed','skipped','manual_only','noop'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_ingest_run ON qe_archive.ingest_history(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_qear_ingest_backfill ON qe_archive.ingest_history(backfill_run_id)",
    """
    CREATE INDEX IF NOT EXISTS idx_qear_ingest_source
        ON qe_archive.ingest_history(source_system, source_type, source_id, COALESCE(source_sub_id, ''))
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_ingest_status ON qe_archive.ingest_history(ingest_status, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.backfill_run (
        backfill_run_id TEXT PRIMARY KEY,
        source_mode TEXT NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        force_rebackfill BOOLEAN NOT NULL DEFAULT FALSE,
        confirm_token_used BOOLEAN NOT NULL DEFAULT FALSE,
        requested_by TEXT,
        candidate_count INTEGER NOT NULL DEFAULT 0,
        processed_count INTEGER NOT NULL DEFAULT 0,
        ingested_count INTEGER NOT NULL DEFAULT 0,
        skipped_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        last_cursor JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_message TEXT,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_backfill_source_mode CHECK (
            source_mode IN ('completed_single_experiments','completed_custom_evo_loops','all_completed_qe_sources','specific_ids')
        ),
        CONSTRAINT ck_qear_backfill_mode CHECK (mode IN ('preview','execute','resume','rebootstrap')),
        CONSTRAINT ck_qear_backfill_status CHECK (status IN ('pending','running','completed','failed','partial'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_backfill_run_status ON qe_archive.backfill_run(status, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.backfill_run_item (
        item_id TEXT PRIMARY KEY,
        backfill_run_id TEXT NOT NULL REFERENCES qe_archive.backfill_run(backfill_run_id) ON DELETE CASCADE,
        source_system TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_sub_id TEXT,
        archive_policy TEXT,
        status TEXT NOT NULL DEFAULT 'candidate',
        run_id TEXT,
        skip_id TEXT,
        error_message TEXT,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_backfill_item_policy CHECK (archive_policy IS NULL OR archive_policy IN ('AUTO','SKIP','MANUAL_ONLY')),
        CONSTRAINT ck_qear_backfill_item_status CHECK (status IN ('candidate','ingested','skipped','failed'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qear_backfill_item_run ON qe_archive.backfill_run_item(backfill_run_id, status)",
    """
    CREATE TABLE IF NOT EXISTS qe_archive.bootstrap_marker (
        source_type TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        mode TEXT NOT NULL,
        backfill_run_id TEXT NOT NULL,
        operator TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        ingested_count INTEGER NOT NULL DEFAULT 0,
        skipped_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qear_bootstrap_status CHECK (status IN ('running','completed','failed')),
        CONSTRAINT ck_qear_bootstrap_mode CHECK (mode IN ('execute','rebootstrap'))
    )
    """,
    """
    INSERT INTO qe_archive.schema_version(version, description)
    VALUES (%s, 'QE realtime experiment warehouse schema bootstrap')
    ON CONFLICT (version) DO NOTHING
    """,
]


TABLE_COMMENTS: dict[str, str] = {
    "qe_archive.schema_version": "Applied QE archive schema versions and bootstrap metadata.",
    "qe_archive.run": "Canonical archive row for one QE experiment attempt or evolution loop.",
    "qe_archive.run_source": "External source identifiers and recorder links for an archived QE run.",
    "qe_archive.multi_alpha_run": "Archived multi-alpha combine-backtest run header and roster-level configuration snapshot.",
    "qe_archive.multi_alpha_leg": "Materialized per-leg snapshot for multi-alpha combine-backtest provenance and replay.",
    "qe_archive.multi_alpha_leg_source": "Per-seed precise provenance from a multi-alpha leg to QE experiment, loop, and run coordinates.",
    "qe_archive.multi_alpha_scheme": "Per-weighting-scheme weights, rolling weights, metrics, and baseline deltas for a multi-alpha run.",
    "qe_archive.multi_alpha_loo": "Leave-one-out marginal contribution metrics for multi-alpha weighting schemes.",
    "qe_archive.run_config": "Reproducibility critical canonical and raw configuration for a QE run.",
    "qe_archive.run_reproducibility_manifest": "Full reproducibility manifest, hashes, environment versions, artifacts, and gaps.",
    "qe_archive.run_data_context": "Data universe, date windows, benchmark, PIT, cost, and tradability context for a run.",
    "qe_archive.run_account_summary": "Account level absolute return, capital, cash, drawdown, and holding summary.",
    "qe_archive.metric_taxonomy": "Canonical metric dictionary used to normalize QE and Qlib metric names.",
    "qe_archive.run_metric": "Scalar metrics captured from QE, Qlib, enhanced metrics, and derived archive scores.",
    "qe_archive.run_curve": "Time series or step series curves such as return, drawdown, IC, RankIC, and training loss.",
    "qe_archive.run_factor": "Factor membership, metadata snapshots, ratings, and correlation cluster context for a run.",
    "qe_archive.run_factor_importance": "Per factor or feature importance and attribution records for all model families.",
    "qe_archive.run_factor_pair": "Pairwise factor correlation, cluster, and synergy diagnostics for a run.",
    "qe_archive.run_symbol_summary": "Per stock profit and holding summaries from all/top/bottom stock result lists.",
    "qe_archive.run_model_trial": "Model training or optimizer trial parameters, objectives, score, and resource metadata.",
    "qe_archive.run_model_training_metric": "Per epoch or step model training metrics linked to a run or model trial.",
    "qe_archive.run_position": "Daily or intraday position snapshots reconstructed from archived backtest artifacts.",
    "qe_archive.run_order": "Order intent and execution order records from QE backtests or artifact parsers.",
    "qe_archive.run_trade": "Executed trade records including price, quantity, amount, cost, slippage, and pnl.",
    "qe_archive.run_execution_event": "Execution and parser events that explain backtest behavior and data quality decisions.",
    "qe_archive.run_artifact": "Artifact manifest rows for files stored outside PostgreSQL with hash and storage metadata.",
    "qe_archive.raw_payload": "Raw JSON or text payload snapshots used for audit and field mapping verification.",
    "qe_archive.run_priority_score": "Default score_total and component scores used for ranking and optimization priority.",
    "qe_archive.optimization_candidate": "Future optimization candidates generated from warehouse evidence.",
    "qe_archive.agent_query_audit": "Audit log for LLM agent or tool read access to QE archive data.",
    "qe_archive.outbox_event": "Durable archive ingestion events written by QE completion paths or backfill jobs.",
    "qe_archive.archive_job": "Archive worker job state, retry, level, and processing statistics.",
    "qe_archive.skip_registry": "Auditable records for QE runs intentionally excluded from automatic archive ingestion.",
    "qe_archive.ingest_history": "Attempt-level archive ingestion history for realtime, retry, manual, and backfill flows.",
    "qe_archive.backfill_run": "Durable lifecycle record for historical QE archive backfill preview, execute, resume, and rebootstrap operations.",
    "qe_archive.backfill_run_item": "Per-source item state captured for one historical QE archive backfill run.",
    "qe_archive.bootstrap_marker": "One-time broad historical backfill marker by QE source type.",
}


COMMON_COLUMN_COMMENTS: dict[str, str] = {
    "id": "Surrogate numeric row identifier.",
    "run_id": "Stable QE archive run identifier used as the primary cross table join key.",
    "logical_experiment_id": "Stable logical experiment identifier shared by retries or attempts.",
    "attempt_no": "Attempt number for the same logical experiment.",
    "is_latest_attempt": "Whether this run is the latest attempt for the logical experiment.",
    "source_system": "Originating system name such as qe, qlib, mlflow, backfill, or agent.",
    "run_type": "Archived run kind, for example single experiment, evolution loop, or backfill.",
    "task_id": "QE evolution task identifier when the run comes from an evolution task.",
    "loop_id": "QE loop identifier when the run comes from an evolution loop.",
    "loop_index": "Numeric QE loop index within the task.",
    "experiment_id": "QE single experiment identifier when applicable.",
    "node_id": "Compute node identifier that produced or served the run payload.",
    "model_catalog_id": "Optional model catalog row identifier linked to this run.",
    "model_family": "High level model family such as tree, linear, LSTM, or deep model.",
    "model_type": "Concrete model type or implementation name used by the run.",
    "factor_set_hash": "Deterministic hash of the exact ordered factor list or feature set.",
    "factor_count": "Number of factors or features included in the archived run.",
    "freq": "Data frequency used by the run, for example day or minute.",
    "label_horizon": "Prediction label horizon used by the model or dataset.",
    "status": "Archive or source run status.",
    "research_valid": "Whether this run is valid for default research rankings and optimizer samples.",
    "invalid_reason": "Reason why the run is excluded from default research ranking.",
    "exclusion_tags": "Machine readable exclusion tags for filtering invalid or low trust runs.",
    "score_total": "Default aggregate ranking score for optimization priority.",
    "score_version": "Version of the score formula used to compute score_total and components.",
    "priority_rank": "Optional precomputed priority rank among comparable runs.",
    "started_at": "Source run or archive job start timestamp.",
    "completed_at": "Source run or archive job completion timestamp.",
    "archived_at": "Timestamp when archive processing marked the run archived.",
    "created_at": "Timestamp when this database row was created.",
    "updated_at": "Timestamp when this database row was last updated.",
    "source_created_at": "Creation timestamp reported by the upstream source system.",
    "source_updated_at": "Update timestamp reported by the upstream source system.",
    "source_type": "Source object type such as task, loop, experiment, recorder, or artifact.",
    "source_id": "Source object identifier from the upstream system.",
    "source_sub_id": "Optional source sub identifier such as loop id or recorder id.",
    "source_status": "Status reported by the upstream source object.",
    "source_uri": "URI or logical reference to the upstream source object.",
    "recorder_experiment_id": "MLflow or Qlib recorder experiment identifier.",
    "recorder_id": "MLflow or Qlib recorder run identifier.",
    "mlflow_tracking_uri": "MLflow tracking URI recorded for traceability only.",
    "mlflow_artifact_uri": "MLflow artifact URI recorded for manifest traceability.",
    "qlib_recorder_name": "Qlib recorder name or experiment label.",
    "node_api_base_url": "Node API base URL used for controlled artifact access.",
    "metadata": "Additional structured metadata that does not yet have a dedicated column.",
    "config_schema_version": "Version of the archived canonical configuration schema.",
    "config_sha256": "SHA256 hash of the canonical configuration.",
    "canonical_config": "Normalized complete configuration used for query, comparison, and replay.",
    "raw_config": "Raw source configuration payloads retained without semantic loss.",
    "factor_list": "Ordered factor or feature list used by the model.",
    "model_config": "Model class, module, and high level model configuration.",
    "model_params": "Model initialization, fit, optimizer, seed, and training parameters.",
    "strategy_config": "Portfolio strategy configuration such as topk, drop, rebalance, and thresholds.",
    "backtest_config": "Backtest window, benchmark, cost, and evaluation configuration.",
    "data_split": "Train, validation, test, and backtest date split configuration.",
    "execution_config": "Execution algorithm, unfilled handling, minute policy, and trade execution settings.",
    "runtime_flags": "Runtime switches such as HMM, Alpha158, multi alpha, or debug flags.",
    "agent_context": "LLM or automation context that influenced the submitted configuration.",
    "config_capture_complete": "Whether all required config sources were captured for this run.",
    "config_provenance": "Map of config fragments to their source tables, APIs, or artifact manifests.",
    "missing_config_items": "Required config items that were unavailable during archive capture.",
    "manifest_schema_version": "Version of the reproducibility manifest schema.",
    "reproducibility_level": "Replay confidence level: full, partial, or audit_only.",
    "verification_status": "Status of any replay or hash verification performed for the run.",
    "canonical_config_sha256": "SHA256 of the canonical config payload recorded in the manifest.",
    "raw_config_sha256": "SHA256 of the raw config payload collection.",
    "qlib_config_sha256": "SHA256 of the rendered Qlib config when available.",
    "model_params_sha256": "SHA256 of model parameter payloads used for replay verification.",
    "strategy_config_sha256": "SHA256 of strategy config payloads used for replay verification.",
    "data_context_sha256": "SHA256 of data context and split payloads used for replay verification.",
    "metrics_payload_sha256": "SHA256 of the primary metrics payload captured from source.",
    "enhanced_metrics_sha256": "SHA256 of the enhanced metrics payload captured from source.",
    "artifact_manifest_sha256": "SHA256 of the artifact manifest linked to this run.",
    "git_commit": "Git commit reported for the runner code if available.",
    "git_dirty": "Whether the runner repository reported uncommitted changes.",
    "runner_script": "Runner script or entrypoint used to produce the source experiment.",
    "runner_script_sha256": "SHA256 of the runner script when captured.",
    "python_version": "Python version reported by the runner environment.",
    "qlib_version": "Qlib version reported by the runner environment.",
    "mlflow_version": "MLflow version reported by the runner environment.",
    "torch_version": "PyTorch version reported by the runner environment.",
    "package_versions": "Package version snapshot needed for replay and audit.",
    "random_seed": "Random seed used by the runner when available.",
    "deterministic_flags": "Determinism and backend flags that affect replay repeatability.",
    "source_config_paths": "Logical source config paths and artifact identifiers, not direct worker paths.",
    "required_artifact_types": "Artifact types expected for the declared reproducibility level.",
    "missing_items": "Missing reproducibility inputs or artifacts found during archiving.",
    "manifest_json": "Complete reproducibility manifest payload.",
    "context_type": "Role of the data context row, usually primary.",
    "market": "Market or exchange scope used by the run.",
    "universe": "Trading universe or stock pool used by the run.",
    "benchmark": "Benchmark symbol or index used for evaluation.",
    "train_start": "Training window start date.",
    "train_end": "Training window end date.",
    "valid_start": "Validation window start date.",
    "valid_end": "Validation window end date.",
    "test_start": "Test window start date.",
    "test_end": "Test window end date.",
    "backtest_start": "Backtest evaluation start date.",
    "backtest_end": "Backtest evaluation end date.",
    "qlib_provider_uri": "Qlib provider URI recorded for data lineage only.",
    "qlib_dataset_version": "Qlib dataset version or logical snapshot label.",
    "dataset_snapshot_id": "Dataset snapshot identifier used by the run.",
    "feature_snapshot_id": "Feature snapshot identifier used by the run.",
    "factor_cache_snapshot_id": "Factor cache snapshot identifier used by the run.",
    "data_version_hash": "Hash representing the data version and context.",
    "pit_cutoff_date": "Point in time cutoff date that prevents future data leakage.",
    "limit_handling": "Limit up or limit down handling policy used by the backtest.",
    "suspend_handling": "Suspension handling policy used by the backtest.",
    "limit_suspend_authoritative": "Whether limit and suspension handling came from authoritative data.",
    "cost_config": "Trading cost, fee, tax, slippage, and minimum commission settings.",
    "stock_pool_config": "Stock pool, blacklist, and eligibility configuration.",
    "data_quality_flags": "Data quality warnings or trust flags for the run.",
    "initial_capital": "Initial capital or cash used by the backtest.",
    "final_total_value": "Final total account value at the end of the backtest.",
    "final_account_value": "Final account value as reported by the source payload.",
    "final_nav_value": "Final net asset value as reported by the source payload.",
    "total_return": "Total absolute return over the backtest period.",
    "cagr": "Compound annual growth rate over the evaluated period.",
    "max_drawdown": "Maximum drawdown over the evaluated period.",
    "max_drawdown_date": "Date when maximum drawdown occurred if available.",
    "sharpe": "Sharpe ratio from the source or canonical metric calculation.",
    "annualized_volatility": "Annualized return volatility.",
    "avg_cash_ratio": "Average cash ratio over the evaluated period.",
    "final_cash": "Cash balance at the end of the backtest.",
    "final_stock_value": "Market value of stock holdings at the end of the backtest.",
    "final_stock_count": "Number of held stocks at the end of the backtest.",
    "final_cash_ratio": "Cash ratio at the end of the backtest.",
    "n_trading_days": "Number of trading days in the evaluated period.",
    "position_count_min": "Minimum number of positions held during the backtest.",
    "position_count_avg": "Average number of positions held during the backtest.",
    "position_count_max": "Maximum number of positions held during the backtest.",
    "position_count_p95": "95th percentile of held position count.",
    "source_payload_path": "Path inside the archived payload or manifest where the value came from.",
    "metric_key": "Canonical metric key used for cross run analysis.",
    "metric_group": "Metric family or display group.",
    "display_name": "Human readable metric name.",
    "unit": "Metric unit such as percent, ratio, currency, count, or days.",
    "direction": "Optimization direction such as higher_better or lower_better.",
    "canonical_description": "Canonical metric definition.",
    "source_aliases": "Source field aliases mapped to this canonical metric.",
    "metric_scope": "Metric scope such as run, train, validation, test, symbol, or period.",
    "period_start": "Metric period start date when applicable.",
    "period_end": "Metric period end date when applicable.",
    "horizon": "Prediction or evaluation horizon for this metric.",
    "value_num": "Numeric metric value.",
    "value_text": "Text metric value when the metric is not numeric.",
    "value_json": "Structured metric value when scalar columns are insufficient.",
    "source_key": "Original source key or metric name before canonical mapping.",
    "quality_flag": "Quality flag for this metric value.",
    "curve_key": "Canonical curve key such as return, drawdown, IC, RankIC, or loss.",
    "ts": "Timestamp for a time series curve point.",
    "trade_date": "Trading date associated with this row.",
    "step": "Training or curve step number.",
    "epoch": "Training epoch number.",
    "split_name": "Dataset split name such as train, valid, test, or backtest.",
    "factor_catalog_id": "Factor catalog row identifier when the factor is registered.",
    "factor_name": "Factor or feature name.",
    "factor_source": "Originating factor source or library.",
    "factor_version": "Factor version label.",
    "factor_order": "Position of the factor in the ordered feature list.",
    "factor_group": "Factor category, family, or group label.",
    "factor_classification": "Factor classification snapshot captured at archive time.",
    "factor_expression_hash": "Hash of the factor expression or formula.",
    "factor_asset_hash": "Hash of the factor code or asset file.",
    "inclusion_reason": "Reason this factor was included in the run.",
    "inclusion_source": "Source that selected the factor, such as user, LLM, Optuna, or rule.",
    "is_alpha158": "Whether this factor belongs to the Alpha158 baseline family.",
    "independent_metrics_snapshot": "Snapshot of independent factor metrics at archive time.",
    "official_rating_snapshot": "Snapshot of official factor rating at archive time.",
    "correlation_cluster": "Correlation cluster label assigned before or during the run.",
    "feature_name": "Model feature name when different from the factor name.",
    "feature_index": "Feature index used by the model input schema.",
    "method": "Importance or correlation method name.",
    "method_version": "Version of the importance or attribution method.",
    "time_bucket": "Time bucket used for grouped importance or attribution.",
    "importance_value": "Raw importance or attribution value.",
    "normalized_value": "Normalized importance value comparable within a run.",
    "weight_pct": "Normalized factor importance percentage within a run, in percentage points from 0 to 100 when available.",
    "signed_value": "Signed contribution value when available.",
    "rank_in_run": "Rank of this item within the run.",
    "sample_count": "Sample count used to compute the statistic.",
    "reliability": "Reliability flag for the importance or attribution value.",
    "factor_a_catalog_id": "Catalog id for the first factor in the pair.",
    "factor_b_catalog_id": "Catalog id for the second factor in the pair.",
    "factor_a_name": "Name of the first factor in the pair.",
    "factor_b_name": "Name of the second factor in the pair.",
    "corr_method": "Correlation method used for the factor pair.",
    "corr_value": "Correlation value for the factor pair.",
    "corr_as_of_date": "As of date for the pairwise correlation value.",
    "corr_window": "Lookback window used for the pairwise correlation value.",
    "same_cluster": "Whether both factors were assigned to the same correlation cluster.",
    "synergy_score": "Estimated combined value of using the two factors together.",
    "symbol": "Security symbol.",
    "source_list": "Source list name such as all_stocks, top_stocks, or bottom_stocks.",
    "profit": "Absolute profit associated with the symbol.",
    "profit_pct": "Percentage profit associated with the symbol.",
    "avg_cost": "Average holding cost for the symbol.",
    "last_price": "Last observed price for the symbol in the payload.",
    "holding_days": "Number of holding days for the symbol.",
    "first_date": "First date when the symbol appeared in the payload.",
    "last_date": "Last date when the symbol appeared in the payload.",
    "rank_in_list": "Rank of the symbol within the source list.",
    "trial_source": "Source of the model trial such as qe, optuna, mlflow, or backfill.",
    "optimizer_name": "Optimizer name such as Optuna or custom search.",
    "optimizer_study_name": "Optimizer study name when available.",
    "optimizer_trial_number": "Optimizer trial number when available.",
    "search_space": "Hyperparameter search space for this model trial.",
    "params": "Hyperparameters used by this model trial.",
    "fixed_params": "Fixed parameters not searched by the optimizer.",
    "objective_name": "Objective metric name optimized by the trial.",
    "objective_value": "Primary objective value for this trial.",
    "objective_values": "Multiple objective values for multi objective trials.",
    "trial_state": "Trial state such as complete, failed, or pruned.",
    "pruned_reason": "Reason the optimizer pruned or stopped the trial.",
    "train_wall_seconds": "Training wall clock seconds for the trial.",
    "gpu_info": "GPU and compute resource metadata captured during training.",
    "trial_id": "Model trial row identifier linked to the training metric.",
    "weight": "Position weight or target weight.",
    "shares": "Share quantity held or targeted.",
    "price": "Price used for the position or trade.",
    "score": "Model or ranking score associated with the symbol.",
    "rank_in_portfolio": "Rank of the symbol within the portfolio.",
    "return_contribution": "Estimated return contribution from the position.",
    "industry_code": "Industry code associated with the symbol.",
    "industry_name": "Industry name associated with the symbol.",
    "order_uid": "Unique order identifier from the source or archive parser.",
    "side": "Trade side such as buy or sell.",
    "target_weight": "Target portfolio weight.",
    "target_qty": "Target order quantity.",
    "limit_price": "Limit price used by the order if any.",
    "order_price": "Order price submitted by the strategy or executor.",
    "order_qty": "Order quantity submitted by the strategy or executor.",
    "trade_uid": "Unique trade identifier from the source or archive parser.",
    "quantity": "Executed or reported trade quantity.",
    "amount": "Trade amount before or after fees according to source semantics.",
    "commission": "Commission cost for the trade.",
    "tax": "Tax cost for the trade.",
    "slippage": "Estimated slippage cost for the trade.",
    "pnl": "Profit and loss associated with the trade.",
    "event_ts": "Timestamp of the execution or archive event.",
    "event_type": "Machine readable event type.",
    "severity": "Event severity such as info, warning, or error.",
    "message": "Human readable event message.",
    "artifact_type": "Artifact type such as config, metrics, model, curve, log, pred, or trade.",
    "artifact_name": "Artifact display name or logical file name.",
    "storage_tier": "Storage tier for the artifact such as local_hot or cold.",
    "artifact_uri": "Archive owned URI or logical pointer for the artifact.",
    "local_rel_path": "Path relative to the AIstock owned artifact root when locally stored.",
    "source_node_id": "Node identifier from which the artifact was collected.",
    "sha256": "SHA256 hash of the artifact content.",
    "size_bytes": "Artifact size in bytes.",
    "content_type": "Artifact content type or MIME type.",
    "compression": "Compression format if the artifact is compressed.",
    "collected_status": "Artifact collection status.",
    "collected_at": "Timestamp when the artifact was collected.",
    "parser_status": "Parser status for structured extraction from the artifact.",
    "parser_error": "Parser error message when artifact parsing failed.",
    "payload_type": "Raw payload type such as metrics_json, enhanced_metrics, config, or webhook.",
    "payload_sha256": "SHA256 hash of the raw payload body.",
    "payload_json": "Raw payload JSON snapshot.",
    "payload_text": "Raw payload text snapshot when JSON is unavailable.",
    "provenance_level": "Payload provenance level such as direct, derived, or backfilled.",
    "alpha_score": "Alpha quality component of score_total.",
    "return_score": "Return quality component of score_total.",
    "risk_score": "Risk control component of score_total.",
    "stability_score": "Metric stability component of score_total.",
    "execution_score": "Execution realism component of score_total.",
    "novelty_score": "Novelty or diversity component of score_total.",
    "data_quality_score": "Data quality and reproducibility component of score_total.",
    "penalty_score": "Penalty component deducted from score_total.",
    "score_components": "Structured score component details.",
    "exclusion_reason": "Reason the run or score is excluded from a default ranking.",
    "candidate_id": "Optimization candidate identifier.",
    "candidate_type": "Candidate type such as model_param, factor_combo, or strategy_param.",
    "generated_by": "Generator that created the candidate.",
    "generator_version": "Generator version that created the candidate.",
    "priority_score": "Priority score assigned to the candidate.",
    "candidate_config": "Candidate configuration proposed for future execution.",
    "evidence_summary": "Warehouse evidence that supports the candidate.",
    "source_run_ids": "Archive runs used as evidence for the candidate.",
    "submitted_task_id": "QE task id created from this candidate if submitted.",
    "submitted_loop_ids": "QE loop ids created from this candidate if submitted.",
    "result_run_ids": "Archive run ids produced by this candidate.",
    "created_by": "User, service, or agent that created the row.",
    "audit_id": "Agent audit row identifier.",
    "agent_name": "Agent name that accessed the warehouse.",
    "tool_name": "Tool or query interface used by the agent.",
    "request_id": "Request identifier for tracing agent activity.",
    "user_intent": "User intent attached to the agent query.",
    "query_scope": "Allowed query scope and filters used by the agent.",
    "row_count": "Number of rows returned or affected by the operation.",
    "token_budget": "Token budget granted to the agent query.",
    "allowed": "Whether the agent query was allowed.",
    "denial_reason": "Reason why the agent query was denied.",
    "event_id": "Outbox event identifier.",
    "retry_count": "Number of processing retries.",
    "next_retry_at": "Earliest timestamp when the event or job can be retried.",
    "locked_by": "Worker identifier that currently holds the lock.",
    "locked_at": "Timestamp when the row was locked by a worker.",
    "error_message": "Last processing error message.",
    "job_id": "Archive worker job identifier.",
    "job_type": "Archive job type.",
    "level": "Archive depth level such as A, B, or C.",
    "stats": "Structured job processing statistics.",
    "skip_id": "Stable skip registry identifier for one source object.",
    "archive_policy": "Archive policy selected by the experiment or template: AUTO, SKIP, or MANUAL_ONLY.",
    "archive_policy_source": "Configuration layer that supplied the archive policy decision.",
    "skip_reason": "Human or agent supplied reason for not automatically archiving the source.",
    "allow_override": "Whether a later explicit backfill may override this skip decision.",
    "override_required_token": "Confirmation token required to override a skip decision.",
    "trigger_reason": "Reason the ingestion attempt was triggered, such as realtime or backfill.",
    "runtime_config_sha256": "SHA256 hash of the runtime configuration used for anomaly detection.",
    "history_id": "Stable ingestion history row identifier.",
    "backfill_run_id": "Backfill run identifier for preview, execute, resume, or rebootstrap lifecycle.",
    "ingest_status": "Status of one ingestion attempt.",
    "result_fingerprint": "Stable fingerprint of the archived result used to detect changed repeats.",
    "anomaly": "Whether this attempt differs from previous payload or runtime fingerprints.",
    "anomaly_reason": "Explanation of the detected ingestion anomaly.",
    "source_mode": "Source selection mode for a historical backfill run.",
    "mode": "Backfill lifecycle mode such as preview, execute, resume, or rebootstrap.",
    "request_payload": "Original API request payload for the backfill run.",
    "force_rebackfill": "Whether the backfill run explicitly requested reprocessing already bootstrapped sources.",
    "confirm_token_used": "Whether the caller supplied the required write confirmation token.",
    "requested_by": "User, service, or agent identity that requested the backfill.",
    "candidate_count": "Number of source candidates discovered for the backfill.",
    "processed_count": "Number of source candidates processed by the backfill.",
    "ingested_count": "Number of source candidates successfully archived.",
    "skipped_count": "Number of source candidates skipped by policy or idempotency.",
    "failed_count": "Number of source candidates that failed archive processing.",
    "last_cursor": "Resume cursor or processing checkpoint for a backfill run.",
    "item_id": "Backfill item identifier for a source candidate.",
    "roster_hash": "Deterministic hash of the multi-alpha roster used to build the combine-backtest run.",
    "oos_start": "First out-of-sample trade date covered by the combine-backtest run.",
    "oos_end": "Last out-of-sample trade date covered by the combine-backtest run.",
    "normalize_method": "Score normalization method used before multi-alpha combination.",
    "walk_forward_json": "Walk-forward weighting configuration such as window, min_periods, and expanding mode.",
    "baseline_leg_id": "Optional leg id used as the baseline for scheme delta metrics.",
    "leg_count": "Number of legs in the archived multi-alpha roster.",
    "logical_status": "Business logical status preserved separately from legacy storage mappings.",
    "reason_json": "Terminal reason and failure details copied from the source combine-backtest run.",
    "leg_id": "Stable leg identifier within one multi-alpha combine-backtest roster.",
    "leg_order": "Deterministic zero-based order of the leg within the source roster.",
    "seed_run_ids": "Original seed run identifiers used to build this leg.",
    "factor_names": "Ordered factor names materialized for this leg when available.",
    "seed_count": "Number of seed run identifiers listed for this leg.",
    "source_run_meta": "JSON snapshot of resolved seed metadata, unresolved reasons, and leg metadata.",
    "provenance_complete": "Whether every seed source was resolved and required leg metadata was materialized.",
    "source_seq": "One-based source sequence within a leg's seed list.",
    "seed_ref": "Original seed reference string from the source roster.",
    "seed_ref_kind": "Parsed seed reference kind: archive_run_id, evolution_loop_id, or unknown.",
    "source_experiment_id": "Resolved QE experiment id for this seed source.",
    "source_task_id": "Resolved QE evolution task id for this seed source when applicable.",
    "source_loop_id": "Resolved QE loop id for this seed source when applicable.",
    "source_loop_index": "Resolved QE loop index for this seed source when applicable.",
    "source_run_type": "Resolved QE archive run type, such as evolution_loop or single_experiment.",
    "source_model_type": "Resolved model type copied from the source QE run.",
    "source_factor_set_hash": "Resolved factor set hash copied from the source QE run.",
    "resolved": "Whether the seed reference was resolved to precise QE coordinates.",
    "resolve_method": "Resolution method used for the seed reference.",
    "resolve_note": "Resolution note or explicit unresolved reason.",
    "weighting_scheme": "Source weighting scheme identifier for multi-alpha combination.",
    "scheme_algorithm": "Human-meaningful algorithm family for the weighting scheme.",
    "weights_json": "Static leg weight payload for the weighting scheme.",
    "per_window_weights_json": "Rolling or per-window weight trajectory payload for the weighting scheme.",
    "vs_baseline_sharpe_delta": "Sharpe difference versus the configured baseline leg.",
    "vs_baseline_calmar_delta": "Calmar difference versus the configured baseline leg.",
    "pred_persisted": "Whether the combined prediction artifact was persisted by the source run.",
    "skipped": "Whether this scheme was explicitly skipped or failed before metrics were available.",
    "skipped_reason": "Explicit reason for a skipped multi-alpha scheme.",
    "is_best": "Whether this scheme is the best non-skipped scheme selected for the run.",
    "dropped_leg_id": "Leg id dropped for leave-one-out marginal contribution measurement.",
    "marginal_cagr": "CAGR marginal contribution versus the full scheme.",
    "marginal_sharpe": "Sharpe marginal contribution versus the full scheme.",
    "marginal_calmar": "Calmar marginal contribution versus the full scheme.",
    "operator": "Operator or service identity for a bootstrap marker.",
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _extract_create_table_columns(ddl_statements: Iterable[str]) -> dict[str, tuple[str, ...]]:
    tables: dict[str, tuple[str, ...]] = {}
    for sql in ddl_statements:
        lines = [line.strip() for line in sql.strip().splitlines() if line.strip()]
        if not lines:
            continue
        first_line = lines[0]
        if not first_line.startswith("CREATE TABLE IF NOT EXISTS qe_archive."):
            continue
        parts = first_line.split()
        if len(parts) < 6:
            continue
        table_name = parts[5]
        columns: list[str] = []
        constraint_balance = 0
        for line in lines[1:]:
            line = line.rstrip(",")
            if not line or line == ")":
                continue
            if constraint_balance > 0:
                constraint_balance += line.count("(") - line.count(")")
                continue
            keyword = line.split()[0].upper()
            if keyword in {"CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"}:
                constraint_balance = max(0, line.count("(") - line.count(")"))
                continue
            columns.append(line.split()[0].strip('"'))
        tables[table_name] = tuple(columns)
    return tables


def _column_comment(table_name: str, column_name: str) -> str:
    return COMMON_COLUMN_COMMENTS.get(
        column_name,
        f"{table_name.rsplit('.', 1)[-1]} column {column_name} used by the QE archive.",
    )


def _build_comment_ddl() -> list[str]:
    ddl: list[str] = [
        "COMMENT ON SCHEMA qe_archive IS 'QE realtime experiment warehouse schema for reproducible research archive.'"
    ]
    for table_name, columns in _extract_create_table_columns(BASE_DDL).items():
        table_comment = TABLE_COMMENTS.get(
            table_name,
            f"QE archive table {table_name.rsplit('.', 1)[-1]}.",
        )
        ddl.append(f"COMMENT ON TABLE {table_name} IS '{_sql_literal(table_comment)}'")
        for column_name in columns:
            ddl.append(
                "COMMENT ON COLUMN "
                f"{table_name}.{column_name} IS '{_sql_literal(_column_comment(table_name, column_name))}'"
            )
    return ddl


def iter_qe_archive_tables() -> Iterable[str]:
    """Return qe_archive table names managed by this bootstrap."""

    return tuple(_extract_create_table_columns(BASE_DDL).keys())


def iter_qe_archive_columns() -> Iterable[tuple[str, str]]:
    """Return all qe_archive table columns that must have DB comments."""

    for table_name, columns in _extract_create_table_columns(BASE_DDL).items():
        for column_name in columns:
            yield table_name, column_name


COMMENT_DDL: list[str] = _build_comment_ddl()
DDL: list[str] = BASE_DDL + COMMENT_DDL


def iter_ddl() -> Iterable[str]:
    """Return immutable DDL statements for tests and explicit bootstrap."""

    return tuple(DDL)


def init_qe_archive_schema() -> None:
    """Create the QE archive schema explicitly."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                if "%s" in sql:
                    cur.execute(sql, (QE_ARCHIVE_SCHEMA_VERSION,))
                else:
                    cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    init_qe_archive_schema()
    print(f"QE archive schema initialized: {QE_ARCHIVE_SCHEMA_VERSION}")
