import sys
import re
from pathlib import Path
from dotenv import load_dotenv

# Add backend to sys.path
backend_path = Path(__file__).resolve().parent
if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))

def init_database():
    env_path = backend_path.parent / ".env"
    load_dotenv(env_path, override=True)

    from db.pg_pool import get_conn
    from services.quantevolver.experiment_config import ALLOWED_LABEL_HORIZONS

    label_horizons_sql = ", ".join(str(value) for value in ALLOWED_LABEL_HORIZONS)
    
    print("Starting database initialization for RD-Agent Catalogs...")
    
    tables_sql = {
        "aistock_factor_catalog": """
            CREATE TABLE IF NOT EXISTS aistock_factor_catalog (
                factor_name TEXT,
                source TEXT,
                catalog_version TEXT,
                generated_at_utc TEXT,
                catalog_source TEXT,
                expression TEXT,
                region TEXT,
                tags JSONB,
                description_cn TEXT,
                formula_hint TEXT,
                variables JSONB,
                freq TEXT,
                align TEXT,
                nan_policy TEXT,
                created_at_utc TEXT,
                experiment_id TEXT,
                impl_module TEXT,
                impl_func TEXT,
                impl_version TEXT,
                performance_metrics JSONB,
                first_sota_task_id TEXT,
                is_sota_factor BOOLEAN,
                best_performance TEXT,
                best_performance_sharpe DOUBLE PRECISION,
                best_performance_ann_ret DOUBLE PRECISION,
                interface_info JSONB,
                factor_type TEXT,  -- 因子类型: CrossSection(截面) / TimeSeries(时序)
                data_source TEXT,  -- 数据来源: daily_pv/daily_basic/moneyflow/cyq_perf/bak_basic/multi
                asset_bundle_id TEXT,
                raw_payload JSONB,
                PRIMARY KEY (factor_name, source)
            );
        """,
        "aistock_strategy_catalog": """
            CREATE TABLE IF NOT EXISTS aistock_strategy_catalog (
                strategy_id TEXT PRIMARY KEY,
                catalog_version TEXT,
                generated_at_utc TEXT,
                catalog_source TEXT,
                scenario TEXT,
                step_name TEXT,
                action TEXT,
                example_task_run_id TEXT,
                example_loop_id INTEGER,
                example_workspace_id TEXT,
                example_workspace_path TEXT,
                template_files JSONB,
                data_config JSONB,
                dataset_config JSONB,
                portfolio_config JSONB,
                backtest_config JSONB,
                model_config JSONB,
                feature_list JSONB,
                market TEXT,
                instruments JSONB,
                freq TEXT,
                python_implementation JSONB,
                in_selection_center BOOLEAN DEFAULT FALSE,
                asset_bundle_id TEXT,
                raw_payload JSONB
            );
        """,
        "aistock_loop_catalog": """
            CREATE TABLE IF NOT EXISTS aistock_loop_catalog (
                task_run_id TEXT,
                loop_id INTEGER,
                catalog_version TEXT,
                generated_at_utc TEXT,
                catalog_source TEXT,
                asset_bundle_id TEXT,
                is_solidified BOOLEAN,
                sync_status TEXT,
                workspace_id TEXT,
                workspace_path TEXT,
                scenario TEXT,
                step_name TEXT,
                action TEXT,
                status TEXT,
                has_result BOOLEAN,
                strategy_id TEXT,
                factor_names JSONB,
                annualized_return DOUBLE PRECISION,
                max_drawdown DOUBLE PRECISION,
                sharpe DOUBLE PRECISION,
                ic DOUBLE PRECISION,
                ic_ir DOUBLE PRECISION,
                win_rate DOUBLE PRECISION,
                decision TEXT,
                summary_execution TEXT,
                raw_payload JSONB,
                PRIMARY KEY (task_run_id, loop_id)
            );
        """,
        "aistock_model_catalog": """
            CREATE TABLE IF NOT EXISTS aistock_model_catalog (
                model_id TEXT PRIMARY KEY,
                catalog_version TEXT,
                generated_at_utc TEXT,
                catalog_source TEXT,
                task_run_id TEXT,
                loop_id INTEGER,
                workspace_id TEXT,
                workspace_path TEXT,
                log_dir TEXT,
                model_type TEXT,
                model_config JSONB,
                dataset_config JSONB,
                feature_schema JSONB,
                flattened_feature_list JSONB,
                model_artifacts JSONB,
                asset_bundle_id TEXT,
                raw_payload JSONB
            );
        """,
        "aistock_alpha158_meta": """
            CREATE TABLE IF NOT EXISTS aistock_alpha158_meta (
                factor_name TEXT PRIMARY KEY,
                lib_version TEXT,
                generated_at_utc TEXT,
                expression TEXT,
                description_cn TEXT,
                variables JSONB,
                freq TEXT,
                align TEXT,
                nan_policy TEXT,
                impl_module TEXT,
                impl_func TEXT,
                impl_version TEXT,
                best_performance TEXT,
                best_performance_sharpe DOUBLE PRECISION,
                best_performance_ann_ret DOUBLE PRECISION,
                interface_info JSONB,
                asset_bundle_id TEXT,
                raw_payload JSONB
            );
        """,
        "aistock_alpha360_meta": """
            CREATE TABLE IF NOT EXISTS aistock_alpha360_meta (
                factor_name TEXT PRIMARY KEY,
                lib_version TEXT,
                generated_at_utc TEXT,
                expression TEXT,
                description_cn TEXT,
                variables JSONB,
                freq TEXT,
                align TEXT,
                nan_policy TEXT,
                impl_module TEXT,
                impl_func TEXT,
                impl_version TEXT,
                best_performance TEXT,
                best_performance_sharpe DOUBLE PRECISION,
                best_performance_ann_ret DOUBLE PRECISION,
                interface_info JSONB,
                asset_bundle_id TEXT,
                raw_payload JSONB
            );
        """
    }
    
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            for table_name, sql in tables_sql.items():
                print(f"Creating/Checking table: {table_name}...")
                cur.execute(sql)
            
            # Ensure required columns exist on all tables
            for table_name in tables_sql.keys():
                # Check raw_payload
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='raw_payload';")
                if not cur.fetchone():
                    print(f"Adding missing column 'raw_payload' to {table_name}...")
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN raw_payload JSONB;")

                # Check asset_bundle_id (Phase 3)
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='asset_bundle_id';")
                if not cur.fetchone():
                    print(f"Adding missing column 'asset_bundle_id' to {table_name}...")
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN asset_bundle_id TEXT;")
                
                # Special check for aistock_strategy_catalog
                if table_name == "aistock_strategy_catalog":
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_strategy_catalog' AND column_name='in_selection_center';")
                    if not cur.fetchone():
                        print("Adding missing column 'in_selection_center' to aistock_strategy_catalog...")
                        cur.execute("ALTER TABLE aistock_strategy_catalog ADD COLUMN in_selection_center BOOLEAN DEFAULT FALSE;")

                # Special check for aistock_model_catalog
                if table_name == "aistock_model_catalog":
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_model_catalog' AND column_name='workspace_path';")
                    if not cur.fetchone():
                        print("Adding missing column 'workspace_path' to aistock_model_catalog...")
                        cur.execute("ALTER TABLE aistock_model_catalog ADD COLUMN workspace_path TEXT;")

                # Special check for aistock_loop_catalog
                if table_name == "aistock_loop_catalog":
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_loop_catalog' AND column_name='workspace_path';")
                    if not cur.fetchone():
                        print("Adding missing column 'workspace_path' to aistock_loop_catalog...")
                        cur.execute("ALTER TABLE aistock_loop_catalog ADD COLUMN workspace_path TEXT;")

                # Special check for aistock_factor_catalog
                if table_name == "aistock_factor_catalog":
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='first_sota_task_id';")
                    if not cur.fetchone():
                        print("Adding missing column 'first_sota_task_id' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN first_sota_task_id TEXT;")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='is_sota_factor';")
                    if not cur.fetchone():
                        print("Adding missing column 'is_sota_factor' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN is_sota_factor BOOLEAN;")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='source_task_id';")
                    if not cur.fetchone():
                        print("Adding missing column 'source_task_id' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN source_task_id TEXT;")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='source_code_origin';")
                    if not cur.fetchone():
                        print("Adding missing column 'source_code_origin' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN source_code_origin TEXT;")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='source_loop_tag';")
                    if not cur.fetchone():
                        print("Adding missing column 'source_loop_tag' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN source_loop_tag TEXT;")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='source_index';")
                    if not cur.fetchone():
                        print("Adding missing column 'source_index' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN source_index INTEGER;")

            # ============================================================
            # QuantEvolver Phase 1: 扩展 aistock_model_catalog 字段
            # ============================================================
            qe_model_columns = {
                "model_name": "TEXT",
                "model_description": "TEXT",
                "model_architecture": "TEXT",
                "model_formulation": "TEXT",
                "model_hyperparameters": "JSONB",
                "model_training_hyperparameters": "JSONB",
                "model_variables": "JSONB",
                "ic": "DOUBLE PRECISION",
                "annualized_return": "DOUBLE PRECISION",
                "max_drawdown": "DOUBLE PRECISION",
                "sharpe": "DOUBLE PRECISION",
                "information_ratio": "DOUBLE PRECISION",
                "all_metrics": "JSONB",
                "code_text": "TEXT",
                "hypothesis_text": "TEXT",
                "feedback_observations": "TEXT",
                "feedback_evaluation": "TEXT",
                "feedback_reason": "TEXT",
                "feedback_decision": "BOOLEAN",
                "is_sota": "BOOLEAN DEFAULT FALSE",
                "source_task_id": "TEXT",
                "display_name": "TEXT",
            }
            for col_name, col_type in qe_model_columns.items():
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='aistock_model_catalog' AND column_name='{col_name}';")
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to aistock_model_catalog...")
                    cur.execute(f"ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS {col_name} {col_type};")

            # ============================================================
            # QuantEvolver Phase 1: 扩展 aistock_strategy_catalog 字段
            # ============================================================
            qe_strategy_columns = {
                "source_code_relpath": "TEXT",
                "strategy_type": "TEXT",
                "default_kwargs": "JSONB",
                "display_name": "TEXT",
                "description": "TEXT",
                "source_code": "TEXT",
                "param_schema": "JSONB",
                "parent_strategy_id": "TEXT",
                "llm_analysis": "TEXT",
                "created_at": "TIMESTAMPTZ DEFAULT NOW()",
                "updated_at": "TIMESTAMPTZ DEFAULT NOW()",
            }
            for col_name, col_type in qe_strategy_columns.items():
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='aistock_strategy_catalog' AND column_name='{col_name}';")
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to aistock_strategy_catalog...")
                    cur.execute(f"ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS {col_name} {col_type};")

            # ============================================================
            # QuantEvolver Phase 2: QE 专用表
            # ============================================================
            qe_tables_sql = {
                "qe_factor_classification": """
                    CREATE TABLE IF NOT EXISTS qe_factor_classification (
                        id SERIAL PRIMARY KEY,
                        factor_name TEXT NOT NULL,
                        factor_source TEXT NOT NULL,
                        category TEXT,
                        grade TEXT,
                        grade_reason TEXT,
                        classification_reason TEXT,
                        ic_value DOUBLE PRECISION,
                        sharpe_value DOUBLE PRECISION,
                        ann_ret_value DOUBLE PRECISION,
                        llm_analysis TEXT,
                        description TEXT,
                        factor_dimension TEXT,
                        factor_profile JSONB,
                        analyzed_at TIMESTAMPTZ DEFAULT NOW(),
                        analyzed_by TEXT DEFAULT 'factor_analyst',
                        factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT,
                        UNIQUE(factor_name, factor_source)
                    );
                """,
                "qe_factor_correlations": """
                    CREATE TABLE IF NOT EXISTS qe_factor_correlations (
                        id                SERIAL PRIMARY KEY,
                        factor_a_id       BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                        factor_b_id       BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                        correlation       DOUBLE PRECISION NOT NULL,
                        method            TEXT NOT NULL DEFAULT 'spearman_ewma',
                        as_of_date        DATE NOT NULL DEFAULT CURRENT_DATE,
                        data_window_days  INTEGER NOT NULL DEFAULT 252,
                        computed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        universe          TEXT,
                        universe_rule_version TEXT,
                        universe_fingerprint_sha256 TEXT,
                        index_policy      TEXT,
                        CONSTRAINT chk_factor_order CHECK (factor_a_id < factor_b_id),
                        CONSTRAINT uq_factor_pair   UNIQUE (factor_a_id, factor_b_id)
                    );
                """,
                "qe_correlation_metadata": """
                    CREATE TABLE IF NOT EXISTS qe_correlation_metadata (
                        id SERIAL PRIMARY KEY,
                        as_of_date DATE NOT NULL,
                        num_factors INT,
                        num_high_corr_pairs INT,
                        avg_correlation DOUBLE PRECISION,
                        computation_time_sec DOUBLE PRECISION,
                        hdf5_path TEXT,
                        universe TEXT,
                        universe_rule_version TEXT,
                        universe_fingerprint_sha256 TEXT,
                        index_policy TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(as_of_date)
                    );
                """,
                "qe_tasks": """
                    CREATE TABLE IF NOT EXISTS qe_tasks (
                        task_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
                        task_name TEXT NOT NULL,
                        status TEXT DEFAULT 'draft',
                        max_rounds INTEGER DEFAULT 10,
                        current_round INTEGER DEFAULT 0,
                        target_profiles JSONB,
                        target_metrics JSONB,
                        initial_config JSONB,
                        current_best_config JSONB,
                        agent_model_map JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        completed_at TIMESTAMPTZ
                    );
                """,
                "qe_rounds": """
                    CREATE TABLE IF NOT EXISTS qe_rounds (
                        round_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
                        task_id TEXT REFERENCES qe_tasks(task_id),
                        round_number INTEGER NOT NULL,
                        status TEXT DEFAULT 'pending',
                        config JSONB,
                        hypothesis TEXT,
                        metrics JSONB,
                        analysis TEXT,
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ,
                        UNIQUE(task_id, round_number)
                    );
                """,
                "qe_experiments": """
                    CREATE TABLE IF NOT EXISTS qe_experiments (
                        experiment_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
                        task_id TEXT REFERENCES qe_tasks(task_id),
                        round_id TEXT REFERENCES qe_rounds(round_id),
                        experiment_name TEXT,
                        status TEXT DEFAULT 'pending',
                        factor_names JSONB,
                        model_id TEXT,
                        strategy_id TEXT,
                        data_split JSONB,
                        custom_params JSONB,
                        conf_yaml_path TEXT,
                        workspace_path TEXT,
                        wsl_command TEXT,
                        result_metrics JSONB,
                        result_files JSONB,
                        ai_evaluation TEXT,
                        qe_task_id TEXT,
                        qe_loop_id TEXT,
                        loop_index INTEGER DEFAULT 1,
                        parent_experiment_id TEXT,
                        is_evolution_loop BOOLEAN DEFAULT FALSE,
                        -- 核心指标独立列（从 result_metrics 提取，便于查询/排序/演进诊断）
                        ic DOUBLE PRECISION,
                        icir DOUBLE PRECISION,
                        rank_ic DOUBLE PRECISION,
                        rank_icir DOUBLE PRECISION,
                        annualized_return DOUBLE PRECISION,
                        max_drawdown DOUBLE PRECISION,
                        information_ratio DOUBLE PRECISION,
                        excess_return_with_cost_mean DOUBLE PRECISION,
                        excess_return_without_cost_mean DOUBLE PRECISION,
                        annualized_return_no_cost DOUBLE PRECISION,
                        max_drawdown_no_cost DOUBLE PRECISION,
                        information_ratio_no_cost DOUBLE PRECISION,
                        model_catalog_id BIGINT REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ
                    );
                """,
                "qe_factor_experiment_metrics": """
                    CREATE TABLE IF NOT EXISTS qe_factor_experiment_metrics (
                        id SERIAL PRIMARY KEY,
                        factor_name TEXT NOT NULL,
                        factor_source TEXT NOT NULL,
                        experiment_id TEXT NOT NULL,
                        experiment_name TEXT,

                        ic DOUBLE PRECISION,
                        icir DOUBLE PRECISION,
                        rank_ic DOUBLE PRECISION,
                        rank_icir DOUBLE PRECISION,

                        ann_return_no_cost DOUBLE PRECISION,
                        info_ratio_no_cost DOUBLE PRECISION,
                        max_drawdown_no_cost DOUBLE PRECISION,

                        ann_return_with_cost DOUBLE PRECISION,
                        info_ratio_with_cost DOUBLE PRECISION,
                        max_drawdown_with_cost DOUBLE PRECISION,

                        daily_win_rate DOUBLE PRECISION,
                        weekly_win_rate DOUBLE PRECISION,
                        max_consecutive_win INTEGER,
                        max_consecutive_loss INTEGER,

                        total_trades INTEGER,
                        winning_trades INTEGER,
                        losing_trades INTEGER,
                        stock_win_rate DOUBLE PRECISION,
                        avg_profit_pct DOUBLE PRECISION,
                        avg_loss_pct DOUBLE PRECISION,
                        profit_loss_ratio DOUBLE PRECISION,
                        max_single_profit_pct DOUBLE PRECISION,
                        max_single_loss_pct DOUBLE PRECISION,

                        sharpe_ratio DOUBLE PRECISION,
                        calmar_ratio DOUBLE PRECISION,
                        avg_turnover DOUBLE PRECISION,
                        total_trading_days INTEGER,

                        model_id TEXT,
                        other_factors JSONB,
                        data_split JSONB,

                        collected_at TIMESTAMPTZ DEFAULT NOW(),
                        raw_metrics JSONB,

                        factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT,
                        model_catalog_id BIGINT REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT,

                        UNIQUE(factor_name, factor_source, experiment_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_fexp_factor
                        ON qe_factor_experiment_metrics(factor_name, factor_source);
                    CREATE INDEX IF NOT EXISTS idx_fexp_experiment
                        ON qe_factor_experiment_metrics(experiment_id);
                    CREATE INDEX IF NOT EXISTS idx_fexp_ic
                        ON qe_factor_experiment_metrics(ic DESC NULLS LAST);
                """,
                "qe_factor_official_ratings": """
                    CREATE TABLE IF NOT EXISTS qe_factor_official_ratings (
                        id BIGSERIAL PRIMARY KEY,
                        factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                        rule_version TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        snapshot_date DATE,
                        official_score DOUBLE PRECISION NOT NULL,
                        official_grade TEXT NOT NULL,
                        dimension_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
                        hard_gate_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
                        grade_reason_structured JSONB NOT NULL DEFAULT '{}'::jsonb,
                        metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                        llm_audit_summary TEXT,
                        llm_risk_notes JSONB,
                        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (factor_catalog_id, rule_version, snapshot_date)
                    );
                    CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_factor_version
                        ON qe_factor_official_ratings(factor_catalog_id, rule_version);
                    CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_grade
                        ON qe_factor_official_ratings(rule_version, official_grade);
                """,
                "qe_rating_rule_versions": """
                    CREATE TABLE IF NOT EXISTS qe_rating_rule_versions (
                        rule_version TEXT PRIMARY KEY,
                        version_name TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'draft',
                        rule_file_path TEXT NOT NULL,
                        description_md TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        activated_at TIMESTAMPTZ
                    );
                """,
                "qe_factor_rating_runs": """
                    CREATE TABLE IF NOT EXISTS qe_factor_rating_runs (
                        run_id TEXT PRIMARY KEY,
                        rule_version TEXT NOT NULL REFERENCES qe_rating_rule_versions(rule_version) ON DELETE RESTRICT,
                        scope_type TEXT NOT NULL,
                        scope_payload JSONB,
                        snapshot_date DATE,
                        triggered_from TEXT NOT NULL DEFAULT 'ui_toolbar',
                        status TEXT NOT NULL DEFAULT 'pending',
                        summary JSONB,
                        error_message TEXT,
                        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        finished_at TIMESTAMPTZ
                    );
                    CREATE INDEX IF NOT EXISTS idx_qe_factor_rating_runs_started_at
                        ON qe_factor_rating_runs(started_at DESC);
                """,
                "qe_agent_prompts": """
                    CREATE TABLE IF NOT EXISTS qe_agent_prompts (
                        id SERIAL PRIMARY KEY,
                        agent_type TEXT NOT NULL,
                        prompt_key TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        description TEXT,
                        system_prompt TEXT NOT NULL DEFAULT '',
                        user_prompt_template TEXT NOT NULL DEFAULT '',
                        is_active BOOLEAN DEFAULT TRUE,
                        version INTEGER DEFAULT 1,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(agent_type, prompt_key)
                    );
                """,
                "factor_live_track": """
                    CREATE TABLE IF NOT EXISTS factor_live_track (
                        id SERIAL PRIMARY KEY,
                        factor_name TEXT NOT NULL,
                        strategy_id TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        daily_ic DOUBLE PRECISION,
                        daily_rank_ic DOUBLE PRECISION,
                        rolling_20d_ic DOUBLE PRECISION,
                        rolling_20d_icir DOUBLE PRECISION,
                        rolling_60d_ic DOUBLE PRECISION,
                        rolling_60d_icir DOUBLE PRECISION,
                        signal_hit_rate DOUBLE PRECISION,
                        avg_slippage DOUBLE PRECISION,
                        turnover_actual DOUBLE PRECISION,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT,
                        UNIQUE(factor_name, strategy_id, trade_date)
                    );
                    CREATE INDEX IF NOT EXISTS idx_flt_factor_date
                        ON factor_live_track(factor_name, trade_date);
                    CREATE INDEX IF NOT EXISTS idx_flt_factor_catalog_id
                        ON factor_live_track(factor_catalog_id);
                """,
                "qe_evolution_tasks": f"""
                    CREATE TABLE IF NOT EXISTS qe_evolution_tasks (
                        task_id TEXT PRIMARY KEY,
                        task_name TEXT NOT NULL,
                        target_desc TEXT,
                        max_loops INTEGER DEFAULT 10,
                        current_loop INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',
                        base_experiment_id TEXT REFERENCES qe_experiments(experiment_id),
                        node_id TEXT,
                        label_horizon INTEGER NOT NULL DEFAULT 1,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        CONSTRAINT ck_qe_evolution_tasks_label_horizon CHECK (label_horizon IN ({label_horizons_sql}))
                    );
                """,
                "qe_evolution_loops": """
                    CREATE TABLE IF NOT EXISTS qe_evolution_loops (
                        loop_id TEXT PRIMARY KEY,
                        task_id TEXT REFERENCES qe_evolution_tasks(task_id) ON DELETE CASCADE,
                        loop_index INTEGER NOT NULL,
                        action_type TEXT,
                        config_json JSONB,
                        metrics_json JSONB,
                        agent_analysis JSONB,
                        is_sota BOOLEAN DEFAULT FALSE,
                        status TEXT DEFAULT 'pending',
                        node_id TEXT,
                        experiment_id TEXT REFERENCES qe_experiments(experiment_id),
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(task_id, loop_index)
                    );
                """,
                "qe_sota_registry": """
                    CREATE TABLE IF NOT EXISTS qe_sota_registry (
                        sota_id SERIAL PRIMARY KEY,
                        loop_id TEXT REFERENCES qe_evolution_loops(loop_id) ON DELETE CASCADE,
                        evaluation_reason TEXT,
                        model_assets_synced BOOLEAN DEFAULT FALSE,
                        local_asset_path TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """,
                "qe_agent_model_config": """
                    CREATE TABLE IF NOT EXISTS qe_agent_model_config (
                        id SERIAL PRIMARY KEY,
                        agent_type TEXT NOT NULL,
                        model_provider TEXT DEFAULT 'openai',
                        model_name TEXT DEFAULT 'gpt-4o',
                        temperature DOUBLE PRECISION DEFAULT 0.7,
                        max_tokens INTEGER DEFAULT 4096,
                        extra_config JSONB,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(agent_type)
                    );
                """,
                "qe_loop_factor_records": """
                    CREATE TABLE IF NOT EXISTS qe_loop_factor_records (
                        id BIGSERIAL PRIMARY KEY,
                        task_id TEXT NOT NULL REFERENCES qe_evolution_tasks(task_id) ON DELETE CASCADE,
                        loop_id TEXT NOT NULL REFERENCES qe_evolution_loops(loop_id) ON DELETE CASCADE,
                        loop_index INTEGER NOT NULL,
                        factor_name TEXT NOT NULL,
                        action_role TEXT,
                        combo_ic DOUBLE PRECISION,
                        combo_icir DOUBLE PRECISION,
                        combo_sharpe DOUBLE PRECISION,
                        combo_ann_return DOUBLE PRECISION,
                        combo_max_drawdown DOUBLE PRECISION,
                        factor_ic DOUBLE PRECISION,
                        factor_rank_ic DOUBLE PRECISION,
                        model_id TEXT,
                        action_type TEXT,
                        is_sota BOOLEAN DEFAULT FALSE,
                        other_factors JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT,
                        factor_source TEXT NOT NULL,
                        model_catalog_id BIGINT REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT,
                        UNIQUE(loop_id, factor_name)
                    );
                    CREATE INDEX IF NOT EXISTS idx_qlfr_factor ON qe_loop_factor_records(factor_name);
                    CREATE INDEX IF NOT EXISTS idx_qlfr_task   ON qe_loop_factor_records(task_id);
                """,
                "qe_loop_model_records": """
                    CREATE TABLE IF NOT EXISTS qe_loop_model_records (
                        id BIGSERIAL PRIMARY KEY,
                        task_id TEXT NOT NULL REFERENCES qe_evolution_tasks(task_id) ON DELETE CASCADE,
                        loop_id TEXT NOT NULL REFERENCES qe_evolution_loops(loop_id) ON DELETE CASCADE,
                        loop_index INTEGER NOT NULL,
                        model_id TEXT NOT NULL,
                        model_type TEXT,
                        combo_ic DOUBLE PRECISION,
                        combo_icir DOUBLE PRECISION,
                        combo_sharpe DOUBLE PRECISION,
                        combo_ann_return DOUBLE PRECISION,
                        combo_max_drawdown DOUBLE PRECISION,
                        model_params JSONB,
                        best_epoch INTEGER,
                        total_epochs INTEGER,
                        convergence_ratio DOUBLE PRECISION,
                        overfit_ratio DOUBLE PRECISION,
                        training_failed BOOLEAN DEFAULT FALSE,
                        train_loss_final DOUBLE PRECISION,
                        val_loss_final DOUBLE PRECISION,
                        train_loss_curve JSONB,
                        val_loss_curve JSONB,
                        action_type TEXT,
                        is_sota BOOLEAN DEFAULT FALSE,
                        factor_count INTEGER,
                        factor_list JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        model_catalog_id BIGINT NOT NULL REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT,
                        UNIQUE(loop_id, model_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_qlmr_model ON qe_loop_model_records(model_id);
                    CREATE INDEX IF NOT EXISTS idx_qlmr_task  ON qe_loop_model_records(task_id);
                """,
                "aistock_factor_metrics": """
                    CREATE TABLE IF NOT EXISTS aistock_factor_metrics (
                        id SERIAL PRIMARY KEY,
                        factor_name TEXT NOT NULL,
                        ic_mean DOUBLE PRECISION,
                        ic_std DOUBLE PRECISION,
                        icir DOUBLE PRECISION,
                        rank_ic_1d DOUBLE PRECISION,
                        rank_ic_5d DOUBLE PRECISION,
                        rank_ic_10d DOUBLE PRECISION,
                        rank_ic_20d DOUBLE PRECISION,
                        h20_return_horizon TEXT,
                        h20_ic_mean DOUBLE PRECISION,
                        h20_ic_std DOUBLE PRECISION,
                        h20_rank_ic_mean DOUBLE PRECISION,
                        h20_rank_ic_std DOUBLE PRECISION,
                        h20_icir DOUBLE PRECISION,
                        h20_rank_icir DOUBLE PRECISION,
                        h20_icir_hac DOUBLE PRECISION,
                        h20_rank_icir_hac DOUBLE PRECISION,
                        h20_ic_positive_ratio DOUBLE PRECISION,
                        h20_n_obs INTEGER,
                        h20_hac_lag INTEGER,
                        sharpe DOUBLE PRECISION,
                        annual_return DOUBLE PRECISION,
                        max_drawdown DOUBLE PRECISION,
                        top_annual_return DOUBLE PRECISION,
                        top_max_drawdown DOUBLE PRECISION,
                        top_excess_annual_return DOUBLE PRECISION,
                        benchmark_annual_return DOUBLE PRECISION,
                        turnover DOUBLE PRECISION,
                        coverage DOUBLE PRECISION,
                        coverage_numerator BIGINT,
                        coverage_denominator BIGINT,
                        coverage_semantics TEXT,
                        universe_rule_version TEXT,
                        universe_fingerprint_sha256 TEXT,
                        index_policy TEXT,
                        eligible_sample_count BIGINT,
                        suspended_excluded_count BIGINT,
                        st_pit_excluded_count BIGINT,
                        ic_decay_half_life INTEGER,
                        group_return_monotonicity DOUBLE PRECISION,
                        best_holding_period TEXT,
                        calculated_at TIMESTAMPTZ DEFAULT NOW(),
                        raw_metrics JSONB,
                        UNIQUE(factor_name, calculated_at)
                    );
                    CREATE INDEX IF NOT EXISTS idx_afm_factor_name
                        ON aistock_factor_metrics(factor_name);
                    CREATE INDEX IF NOT EXISTS idx_afm_calc_at
                        ON aistock_factor_metrics(calculated_at DESC);
                """,
            }
            for tbl_name, tbl_sql in qe_tables_sql.items():
                print(f"Creating/Checking QE table: {tbl_name}...")
                cur.execute(tbl_sql)

            # ============================================================
            # QE 单次实验执行：qe_experiments 新增列迁移
            # ============================================================
            qe_exp_migrations = [
                ("qe_task_id", "TEXT"),
                ("qe_loop_id", "TEXT"),
                ("loop_index", "INTEGER DEFAULT 1"),
                ("parent_experiment_id", "TEXT"),
                ("is_evolution_loop", "BOOLEAN DEFAULT FALSE"),
                ("updated_at", "TIMESTAMPTZ DEFAULT NOW()"),
                ("evolution_goal", "TEXT"),
                ("llm_hypothesis", "JSONB"),
                ("llm_feedback", "JSONB"),
                ("is_sota", "BOOLEAN DEFAULT FALSE"),
            ]
            for col_name, col_type in qe_exp_migrations:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='qe_experiments' AND column_name=%s;",
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to qe_experiments...")
                    cur.execute(f"ALTER TABLE qe_experiments ADD COLUMN {col_name} {col_type};")

            # ============================================================
            # 正式评级表迁移
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qe_rating_rule_versions (
                    rule_version TEXT PRIMARY KEY,
                    version_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    rule_file_path TEXT NOT NULL,
                    description_md TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    activated_at TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qe_factor_rating_runs (
                    run_id TEXT PRIMARY KEY,
                    rule_version TEXT NOT NULL REFERENCES qe_rating_rule_versions(rule_version) ON DELETE RESTRICT,
                    scope_type TEXT NOT NULL,
                    scope_payload JSONB,
                    snapshot_date DATE,
                    triggered_from TEXT NOT NULL DEFAULT 'ui_toolbar',
                    status TEXT NOT NULL DEFAULT 'pending',
                    summary JSONB,
                    error_message TEXT,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_qe_factor_rating_runs_started_at ON qe_factor_rating_runs(started_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qe_factor_official_ratings (
                    id BIGSERIAL PRIMARY KEY,
                    factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                    rule_version TEXT NOT NULL REFERENCES qe_rating_rule_versions(rule_version) ON DELETE RESTRICT,
                    run_id TEXT NOT NULL REFERENCES qe_factor_rating_runs(run_id) ON DELETE CASCADE,
                    snapshot_date DATE,
                    official_score DOUBLE PRECISION NOT NULL,
                    official_grade TEXT NOT NULL,
                    dimension_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
                    hard_gate_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
                    grade_reason_structured JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                    llm_audit_summary TEXT,
                    llm_risk_notes JSONB,
                    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (factor_catalog_id, rule_version, snapshot_date)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_factor_version ON qe_factor_official_ratings(factor_catalog_id, rule_version)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_grade ON qe_factor_official_ratings(rule_version, official_grade)")

            # ============================================================
            # qe_factor_classification 新增列迁移
            # ============================================================
            fc_migrations = [
                ("description", "TEXT"),
                ("factor_dimension", "TEXT"),
                ("factor_profile", "JSONB"),
                ("holding_period_class", "TEXT"),  # short(<8d) / medium(8-25d) / long(>25d) / unknown
            ]
            for col_name, col_type in fc_migrations:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='qe_factor_classification' AND column_name=%s;",
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to qe_factor_classification...")
                    cur.execute(f"ALTER TABLE qe_factor_classification ADD COLUMN {col_name} {col_type};")

            # ============================================================
            # qe_evolution_tasks 新增列迁移（Phase 4: 多入口演进）
            # ============================================================
            evo_task_migrations = [
                ("evolution_guidance", "TEXT"),
                ("source_type", "TEXT DEFAULT 'qe_experiment'"),
                ("source_task_id", "TEXT"),
                ("evolution_mode", "TEXT DEFAULT 'auto'"),
                ("fork_from_task_id", "TEXT"),
                ("fork_from_loop_index", "INTEGER"),
                ("inherit_history", "BOOLEAN DEFAULT FALSE"),
                ("label_horizon", "INTEGER NOT NULL DEFAULT 1"),
            ]
            for col_name, col_type in evo_task_migrations:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='qe_evolution_tasks' AND column_name=%s;",
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to qe_evolution_tasks...")
                    cur.execute(f"ALTER TABLE qe_evolution_tasks ADD COLUMN {col_name} {col_type};")
            cur.execute("""
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = 'ck_qe_evolution_tasks_label_horizon'
            """)
            label_horizon_constraint = cur.fetchone()
            label_horizon_constraint_def = str(label_horizon_constraint[0]) if label_horizon_constraint else ""
            existing_label_horizons = {
                int(value)
                for value in re.findall(
                    r"(?<![\w.])-?\d+(?![\w.])",
                    label_horizon_constraint_def,
                )
            }
            if label_horizon_constraint and existing_label_horizons != set(ALLOWED_LABEL_HORIZONS):
                cur.execute("""
                    ALTER TABLE qe_evolution_tasks
                    DROP CONSTRAINT ck_qe_evolution_tasks_label_horizon
                """)
                label_horizon_constraint = None
            if not label_horizon_constraint:
                cur.execute(f"""
                    ALTER TABLE qe_evolution_tasks
                    ADD CONSTRAINT ck_qe_evolution_tasks_label_horizon
                    CHECK (label_horizon IN ({label_horizons_sql}))
                """)

            # ============================================================
            # qe_evolution_tasks 新增列迁移（Phase: 策略演进）
            # ============================================================
            strategy_evo_migrations = [
                ("task_type", "TEXT DEFAULT 'evolution'"),
                ("strategy_evo_config", "JSONB"),
                ("strategy_evo_execution_mode", "TEXT DEFAULT 'serial'"),
                ("model_source_task_id", "TEXT"),
                ("model_source_loop_index", "INTEGER"),
            ]
            for col_name, col_type in strategy_evo_migrations:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='qe_evolution_tasks' AND column_name=%s;",
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"Adding column '{col_name}' to qe_evolution_tasks (Strategy Evolution)...")
                    cur.execute(f"ALTER TABLE qe_evolution_tasks ADD COLUMN {col_name} {col_type};")

            # ============================================================
            # 因子可用性管理: is_available 列 + 因子黑名单
            # ============================================================
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='aistock_factor_catalog' AND column_name='is_available'
            """)
            if not cur.fetchone():
                print("Adding column 'is_available' to aistock_factor_catalog...")
                cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN is_available BOOLEAN NOT NULL DEFAULT TRUE")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_factor_catalog_available
                    ON aistock_factor_catalog (is_available) WHERE is_available = FALSE
                """)

            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='qe_evolution_tasks' AND column_name='factor_blacklist'
            """)
            if not cur.fetchone():
                print("Adding column 'factor_blacklist' to qe_evolution_tasks...")
                cur.execute("ALTER TABLE qe_evolution_tasks ADD COLUMN factor_blacklist JSONB DEFAULT '[]'::jsonb")

            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='qe_evolution_loops' AND column_name='node_id'
            """)
            if not cur.fetchone():
                print("Adding column 'node_id' to qe_evolution_loops...")
                cur.execute("ALTER TABLE qe_evolution_loops ADD COLUMN node_id TEXT")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_qe_evolution_loops_task_node
                ON qe_evolution_loops(task_id, node_id)
            """)

            # ============================================================
            # catalog_id 关联列迁移（已有数据库升级用，新库由 CREATE TABLE 定义）
            # ============================================================
            def _add_col(table, column, col_type):
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name=%s AND column_name=%s;",
                    (table, column),
                )
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type};")
                    print(f"[db][upgrade] Added {table}.{column}")

            # 因子侧
            _add_col("qe_loop_factor_records", "factor_catalog_id", "BIGINT")
            _add_col("qe_loop_factor_records", "factor_source", "TEXT")
            _add_col("qe_factor_classification", "factor_catalog_id", "BIGINT")
            _add_col("aistock_factor_catalog", "correlation_computed_at", "TIMESTAMPTZ DEFAULT NULL")
            _add_col("aistock_factor_catalog", "correlation_pair_count", "INTEGER DEFAULT 0")
            _add_col("qe_factor_experiment_metrics", "factor_catalog_id", "BIGINT")
            _add_col("factor_live_track", "factor_catalog_id", "BIGINT")

            # 模型侧
            _add_col("qe_loop_model_records", "model_catalog_id", "BIGINT")
            _add_col("qe_loop_factor_records", "model_catalog_id", "BIGINT")
            _add_col("qe_experiments", "model_catalog_id", "BIGINT")
            _add_col("qe_factor_experiment_metrics", "model_catalog_id", "BIGINT")

            # ============================================================
            # Phase 2 日内执行: execution_algorithm_catalog 表
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.execution_algorithm_catalog (
                    id              SERIAL PRIMARY KEY,
                    algo_code       TEXT NOT NULL UNIQUE,
                    algo_name       TEXT NOT NULL,
                    algo_type       TEXT NOT NULL DEFAULT 'execution',
                    description     TEXT,
                    source          TEXT DEFAULT 'builtin',
                    source_code     TEXT,
                    default_config  JSONB DEFAULT '{}'::jsonb,
                    param_schema    JSONB DEFAULT '{}'::jsonb,
                    supported_freqs TEXT[] DEFAULT '{5m}',
                    min_bars        INTEGER DEFAULT 1,

                    category        TEXT,
                    category_reason TEXT,
                    grade           TEXT,
                    grade_score     INTEGER,
                    analysis_profile JSONB DEFAULT '{}'::jsonb,
                    llm_analysis_at TIMESTAMPTZ,

                    is_enabled      BOOLEAN DEFAULT TRUE,
                    sort_order      INTEGER DEFAULT 0,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            print("Created/Checked table: execution_algorithm_catalog")

            # 插入 6 个初始执行算法（ON CONFLICT 跳过已存在的）
            cur.execute("""
                INSERT INTO execution_algorithm_catalog
                    (algo_code, algo_name, source, description, source_code, default_config, param_schema, supported_freqs, min_bars, sort_order)
                VALUES
                ('CLOSE_PRICE', '收盘价执行', 'builtin',
                 '盘后按当日收盘价一次性执行全部订单。当前默认模式，无需分钟线数据。',
                 'exec_price = daily_close\nexec_quantity = total_quantity\n# 一次性执行，无拆分',
                 '{}', '{}', '{1d}', 0, 0),

                ('TWAP', '时间加权均价 (TWAP)', 'qlib',
                 '将目标订单均匀拆分到 N 个时间步执行。来源: Qlib TWAPStrategy。最后一步强制执行剩余数量，按100股对齐。',
                 'step_qty = total_quantity / split_count\nfor t in range(split_count):\n    execute(step_qty, bar[t].close)\n# 最后一步: remaining = total - executed',
                 '{"split_count": 12, "start_time": "09:35", "end_time": "14:50"}',
                 '{"type": "object", "properties": {"split_count": {"type": "integer", "minimum": 2, "maximum": 48, "default": 12}, "start_time": {"type": "string", "default": "09:35"}, "end_time": {"type": "string", "default": "14:50"}}}',
                 '{1m,5m,15m,30m}', 1, 10),

                ('VWAP', '成交量加权均价 (VWAP)', 'custom',
                 '按历史成交量分布拆分订单。高成交量时段分配更多份额，低成交量时段减少份额。需要历史分钟成交量数据。',
                 'vol_profile[t] = avg_volume(t, lookback_days)\nweight[t] = vol_profile[t] / sum(vol_profile)\nstep_qty[t] = total_quantity * weight[t]\n# VWAP = sum(price*volume) / sum(volume)',
                 '{"lookback_days": 5, "start_time": "09:35", "end_time": "14:50", "participation_rate": 0.1}',
                 '{"type": "object", "properties": {"lookback_days": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5}, "participation_rate": {"type": "number", "minimum": 0.01, "maximum": 0.5, "default": 0.1}}}',
                 '{1m,5m,15m}', 20, 20),

                ('SBB_EMA', 'EMA择时执行 (SBB)', 'qlib',
                 '在相邻两个 Bar 中选择更优时机执行。使用 EMA(10)-EMA(20) 预测短期价格趋势：看多时提前买入/延后卖出，看空时反之。来源: Qlib SBBStrategyEMA。',
                 'signal = EMA(close, fast) - EMA(close, slow)\nif signal > 0:  # TREND_LONG\n    buy at bar[0], sell at bar[1]\nelif signal < 0:  # TREND_SHORT\n    sell at bar[0], buy at bar[1]\nelse:  # TREND_MID → TWAP fallback',
                 '{"ema_fast": 10, "ema_slow": 20}',
                 '{"type": "object", "properties": {"ema_fast": {"type": "integer", "minimum": 3, "maximum": 30, "default": 10}, "ema_slow": {"type": "integer", "minimum": 10, "maximum": 60, "default": 20}}}',
                 '{1m,5m,15m,30m}', 20, 30),

                ('AC_OPTIMAL', 'Almgren-Chriss 最优执行', 'qlib',
                 '基于市场波动率计算最优执行计划。高波动期间减少交易量（降低市场冲击），低波动期间加大执行。无信号时降级为 TWAP。来源: Qlib ACStrategy。',
                 'sigma = rolling_std(returns, vol_window)\nkappa = sqrt(lambda / eta)\nratio[t] = sinh(kappa*(T-t)) / sinh(kappa*T)\nstep_qty[t] = total_quantity * ratio[t]\n# lambda: 时间风险厌恶, eta: 市场冲击系数',
                 '{"lambda": 1e-6, "eta": 2.5e-6, "vol_window": 20}',
                 '{"type": "object", "properties": {"lambda": {"type": "number", "minimum": 1e-8, "maximum": 1e-3, "default": 1e-6}, "eta": {"type": "number", "minimum": 1e-8, "maximum": 1e-3, "default": 2.5e-6}, "vol_window": {"type": "integer", "minimum": 5, "maximum": 60, "default": 20}}}',
                 '{1m,5m,15m}', 30, 40),

                ('POV', '参与率执行 (POV)', 'custom',
                 '按当前市场成交量的固定比例执行。确保不超过市场成交量的 N%，降低市场冲击。需要实时成交量数据。',
                 'step_qty[t] = min(\n    remaining_quantity,\n    market_volume[t] * target_participation,\n    market_volume[t] * max_participation\n)\n# 跟随市场成交量节奏执行',
                 '{"target_participation": 0.05, "max_participation": 0.15, "start_time": "09:35", "end_time": "14:50"}',
                 '{"type": "object", "properties": {"target_participation": {"type": "number", "minimum": 0.01, "maximum": 0.3, "default": 0.05}, "max_participation": {"type": "number", "minimum": 0.05, "maximum": 0.5, "default": 0.15}}}',
                 '{1m,5m,15m}', 30, 50),

                ('V24_PLAN', 'v24 方向感知执行计划 (SL)', 'custom',
                 'v24 B1: 1D-CNN + 归一化缺口 embedding 执行计划网络。开盘30分钟采集特征后生成210分钟softmax执行分布。买入PA相比v20提升+30%, 总PA=+6.35bps。继承尾盘未成交再分配(TAIL_BOOST/TAIL_SUBSTITUTE)。需要模型文件v24_plan_net.pt (168K参数)。',
                 '# v24 Plan Net: 归一化缺口感知 + 买卖不对称\ngap_ratio = gap_pct / limit_pct  # [-1, +1]\nminute_feats = CNN_1D(close, vol, high, low, rsi)[0:30]  # [30, 5]\nplan[210] = softmax(MLP(CNN(minute_feats) || day_feats || gap_embedding))\n# 条件比例执行: frac = plan[t] / sum(plan[t:])',
                 '{"model_path": "/home/lc999/data/rl_models/v24/v24_plan_net.pt", "warmup_minutes": 30, "warmup_alloc": 0.20, "device": "cpu"}',
                 '{"type": "object", "properties": {"model_path": {"type": "string", "description": "v24 plan net 模型文件路径 (.pt)"}, "warmup_minutes": {"type": "integer", "minimum": 10, "maximum": 60, "default": 30, "description": "WARMUP 采集分钟数"}, "warmup_alloc": {"type": "number", "minimum": 0.05, "maximum": 0.50, "default": 0.20, "description": "WARMUP 期间预分配比例"}, "device": {"type": "string", "enum": ["cpu", "cuda"], "default": "cpu", "description": "模型推理设备"}}}',
                 '{1m}', 30, 5)

                ON CONFLICT (algo_code) DO NOTHING
            """)
            print("Inserted initial execution algorithms (7 items, ON CONFLICT skip)")

            # ============================================================
            # 注册 execution_analyst LLM prompt
            # ============================================================
            cur.execute("""
                INSERT INTO qe_agent_prompts (agent_type, prompt_key, display_name, description, system_prompt, user_prompt_template)
                VALUES (
                    'execution_analyst',
                    'analyze_execution_algo',
                    '执行算法分析',
                    '分析执行算法的分类、适用场景、优劣势和A股适用性',
                    '你是量化交易执行算法专家，精通 TWAP/VWAP/AC/POV 等主流执行算法和 A股市场微观结构（T+1、涨跌停、100股整手、集合竞价）。请基于算法描述和核心公式进行专业分析。',
                    '请分析以下日内执行算法：

算法名称: {algo_name}
算法代码: {algo_code}
算法描述: {description}
核心代码/公式:
{source_code}
默认参数: {default_config}
支持频率: {supported_freqs}

请返回严格 JSON 格式（不要 markdown 包裹）：
{{
  "category": "SCHEDULE 或 ADAPTIVE 或 PASSIVE 或 AGGRESSIVE 或 HYBRID",
  "category_reason": "分类理由（一句话）",
  "scores": {{
    "execution_quality": 0到100的整数,
    "adaptiveness": 0到100的整数,
    "data_feasibility": 0到100的整数,
    "complexity_benefit": 0到100的整数,
    "a_share_suitability": 0到100的整数,
    "robustness": 0到100的整数
  }},
  "applicable_scenarios": ["场景1", "场景2"],
  "advantages": ["优势1", "优势2"],
  "disadvantages": ["劣势1", "劣势2"],
  "a_share_notes": "A股特殊注意事项（T+1、涨跌停、100股整手等）",
  "usage_guidance": "使用建议（一段话）",
  "best_for": ["最适合场景1", "最适合场景2"],
  "avoid_for": ["应避免场景1", "应避免场景2"]
}}

分类定义：
- SCHEDULE（定时调度型）：按预设时间表机械执行，不依赖实时行情
- ADAPTIVE（自适应型）：根据实时市场状态动态调整执行节奏
- PASSIVE（被动跟随型）：跟随市场成交量分布执行，最小化冲击
- AGGRESSIVE（激进执行型）：追求最快完成或最优价格
- HYBRID（混合型）：组合多种执行逻辑

评分维度（各0-100分）：
- execution_quality: 执行质量（价格改善/滑点控制）
- adaptiveness: 市场自适应能力
- data_feasibility: 数据可获取性（AIStock现有数据能否支撑）
- complexity_benefit: 复杂度收益比
- a_share_suitability: A股适用性
- robustness: 鲁棒性'
                )
                ON CONFLICT (agent_type, prompt_key) DO NOTHING
            """)
            print("Registered execution_analyst prompt")

            conn.commit()
            cur.close()
            print("Database initialization completed successfully.")
    except Exception as e:
        print(f"Error during database initialization: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()
