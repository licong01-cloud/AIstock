#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建调度中心相关表 (infra schema)
- infra.compute_nodes  — 计算节点注册表
- infra.dispatch_tasks — 调度任务表
- infra.dispatch_task_events — 任务事件日志

并为现有表增加 node_id 列。

手动执行: python -m backend.db.migrations.create_dispatch_tables
"""

import os
import psycopg2

os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
os.environ.setdefault("TDX_DB_PORT", "5432")
os.environ.setdefault("TDX_DB_NAME", "aistock")
os.environ.setdefault("TDX_DB_USER", "postgres")
os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")


def run_migration():
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_HOST"],
        port=int(os.environ["TDX_DB_PORT"]),
        database=os.environ["TDX_DB_NAME"],
        user=os.environ["TDX_DB_USER"],
        password=os.environ["TDX_DB_PASSWORD"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    # ── 1. infra schema ──
    cur.execute("CREATE SCHEMA IF NOT EXISTS infra")

    # ── 2. compute_nodes ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS infra.compute_nodes (
            node_id          TEXT PRIMARY KEY,
            display_name     TEXT NOT NULL,
            api_base_url     TEXT NOT NULL,
            gpu_model        TEXT,
            gpu_vram_mb      INTEGER,
            capabilities     JSONB DEFAULT '[]'::jsonb,
            status           TEXT DEFAULT 'unknown',
            current_task_id  TEXT,
            last_heartbeat   TIMESTAMPTZ,
            metrics_snapshot JSONB,
            grafana_dashboard_url TEXT,
            prometheus_target TEXT,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    print("infra.compute_nodes 创建/确认完成")

    # ── 3. dispatch_tasks ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS infra.dispatch_tasks (
            task_id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
            task_name        TEXT NOT NULL,
            task_type        TEXT NOT NULL,
            node_id          TEXT REFERENCES infra.compute_nodes(node_id),
            status           TEXT DEFAULT 'pending',
            config           JSONB NOT NULL DEFAULT '{}'::jsonb,
            env_overrides    JSONB DEFAULT '{}'::jsonb,
            current_loop     INTEGER DEFAULT 0,
            total_loops      INTEGER,
            progress_pct     REAL DEFAULT 0,
            remote_task_id   TEXT,
            qe_task_id       TEXT,
            best_ic          DOUBLE PRECISION,
            best_sharpe      DOUBLE PRECISION,
            best_ann_return  DOUBLE PRECISION,
            best_max_dd      DOUBLE PRECISION,
            sota_factors     INTEGER DEFAULT 0,
            sota_models      INTEGER DEFAULT 0,
            started_at       TIMESTAMPTZ,
            finished_at      TIMESTAMPTZ,
            time_limit       INTERVAL,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW(),
            log_tail         TEXT DEFAULT '',
            error_message    TEXT
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_status
        ON infra.dispatch_tasks (status)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_node_id
        ON infra.dispatch_tasks (node_id)
    """)
    print("infra.dispatch_tasks 创建/确认完成")

    # ── 4. dispatch_task_events ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS infra.dispatch_task_events (
            id         BIGSERIAL PRIMARY KEY,
            task_id    TEXT REFERENCES infra.dispatch_tasks(task_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            event_data JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dispatch_task_events_task_id
        ON infra.dispatch_task_events (task_id)
    """)
    print("infra.dispatch_task_events 创建/确认完成")

    # ── 5. 现有表增加 node_id 列 ──
    alter_stmts = [
        ("public", "aistock_task_catalog", "node_id", "TEXT"),
        ("public", "aistock_factor_catalog", "node_id", "TEXT"),
        ("public", "aistock_model_catalog", "node_id", "TEXT"),
        ("public", "aistock_strategy_catalog", "node_id", "TEXT"),
        ("public", "qe_evolution_tasks", "node_id", "TEXT"),
        ("public", "qe_evolution_loops", "node_id", "TEXT"),
    ]
    for schema, table, col, col_type in alter_stmts:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """, (schema, table, col))
        if cur.fetchone() is None:
            cur.execute(f'ALTER TABLE {schema}.{table} ADD COLUMN {col} {col_type}')
            print(f"  ALTER TABLE {schema}.{table} ADD COLUMN {col} — 完成")
        else:
            print(f"  {schema}.{table}.{col} 已存在 — 跳过")

    # ── 6. 回填现有数据 ──
    backfill_tables = [
        "aistock_task_catalog",
        "aistock_factor_catalog",
        "aistock_model_catalog",
        "aistock_strategy_catalog",
        "qe_evolution_tasks",
        "qe_evolution_loops",
    ]
    for table in backfill_tables:
        cur.execute(f"UPDATE {table} SET node_id = 'wsl2-5080' WHERE node_id IS NULL")
        print(f"  回填 {table}.node_id = 'wsl2-5080' — 完成 ({cur.rowcount} 行)")

    # ── 7. 初始节点数据 ──
    cur.execute("""
        INSERT INTO infra.compute_nodes (node_id, display_name, api_base_url, gpu_model, gpu_vram_mb, capabilities, prometheus_target)
        VALUES
          ('wsl2-5080', '本机 WSL (RTX 5080)', 'http://127.0.0.1:9000', 'RTX 5080', 16384,
           '["fin_factor","fin_model","fin_quant","fin_factor_report","qe_evolution"]',
           'localhost:9100'),
          ('rdagent-node1', 'Linux 独立机 (RTX 2060)', 'http://192.168.50.215:9000', 'RTX 2060', 6144,
           '["fin_factor","fin_model","fin_quant","fin_factor_report","qe_evolution"]',
           '192.168.50.215:9100')
        ON CONFLICT (node_id) DO NOTHING
    """)
    print(f"初始节点数据插入完成 ({cur.rowcount} 行)")

    # ── 8. updated_at 触发器 ──
    for table in ("infra.compute_nodes", "infra.dispatch_tasks"):
        func_name = f"update_{table.replace('.', '_')}_updated_at"
        trigger_name = f"trigger_{func_name}"
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {func_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)
        cur.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger WHERE tgname = '{trigger_name}'
                ) THEN
                    CREATE TRIGGER {trigger_name}
                    BEFORE UPDATE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION {func_name}();
                END IF;
            END $$
        """)
    print("updated_at 触发器创建完成")

    cur.close()
    conn.close()
    print("\n=== 调度中心数据库迁移全部完成 ===")


if __name__ == "__main__":
    run_migration()
