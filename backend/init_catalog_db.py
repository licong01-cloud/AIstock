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
            
            conn.commit()
            cur.close()
            print("Database initialization completed successfully.")
    except Exception as e:
        print(f"Error during database initialization: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()
