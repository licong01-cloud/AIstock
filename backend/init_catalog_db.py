import sys
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

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='aistock_factor_catalog' AND column_name='source_code_relpath';")
                    if not cur.fetchone():
                        print("Adding missing column 'source_code_relpath' to aistock_factor_catalog...")
                        cur.execute("ALTER TABLE aistock_factor_catalog ADD COLUMN source_code_relpath TEXT;")

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
                "source_code_relpath": "TEXT",
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
                        analyzed_at TIMESTAMPTZ DEFAULT NOW(),
                        analyzed_by TEXT DEFAULT 'factor_analyst',
                        UNIQUE(factor_name, factor_source)
                    );
                """,
                "qe_factor_correlations": """
                    CREATE TABLE IF NOT EXISTS qe_factor_correlations (
                        id SERIAL PRIMARY KEY,
                        factor_a TEXT NOT NULL,
                        factor_b TEXT NOT NULL,
                        correlation DOUBLE PRECISION,
                        method TEXT DEFAULT 'pearson',
                        computed_at TIMESTAMPTZ DEFAULT NOW(),
                        data_period TEXT,
                        UNIQUE(factor_a, factor_b, method)
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

                        UNIQUE(factor_name, factor_source, experiment_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_fexp_factor
                        ON qe_factor_experiment_metrics(factor_name, factor_source);
                    CREATE INDEX IF NOT EXISTS idx_fexp_experiment
                        ON qe_factor_experiment_metrics(experiment_id);
                    CREATE INDEX IF NOT EXISTS idx_fexp_ic
                        ON qe_factor_experiment_metrics(ic DESC NULLS LAST);
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
                "qe_evolution_tasks": """
                    CREATE TABLE IF NOT EXISTS qe_evolution_tasks (
                        task_id TEXT PRIMARY KEY,
                        task_name TEXT NOT NULL,
                        target_desc TEXT,
                        max_loops INTEGER DEFAULT 10,
                        current_loop INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',
                        base_experiment_id TEXT REFERENCES qe_experiments(experiment_id),
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
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
            }
            for tbl_name, tbl_sql in qe_tables_sql.items():
                print(f"Creating/Checking QE table: {tbl_name}...")
                cur.execute(tbl_sql)

            conn.commit()
            cur.close()
            print("Database initialization completed successfully.")
    except Exception as e:
        print(f"Error during database initialization: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()
