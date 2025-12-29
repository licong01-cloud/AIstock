"""调整 aistock_loop_catalog 的唯一约束为 (task_run_id, loop_id, workspace_id).

原始建表脚本中使用了 UNIQUE(task_run_id, loop_id),
而 loop_catalog.json 中同一 task_run_id + loop_id 可能对应多个 workspace_id,
批量 INSERT ... ON CONFLICT 时会触发 PostgreSQL 的 CardinalityViolation。

运行本脚本将：
- 删除旧的唯一约束 (task_run_id, loop_id)（如存在）
- 创建新的唯一约束 (task_run_id, loop_id, workspace_id)

使用 .env 中的 TDX_DB_* 信息连接 PostgreSQL。

运行方式（在项目根目录 F:\Dev\AIstock 下）：

    python -m scripts.alter_aistock_loop_catalog_unique
"""

from __future__ import annotations

import os
from typing import Any, Dict

import psycopg2
from dotenv import load_dotenv


load_dotenv(override=True)


def _db_cfg() -> Dict[str, Any]:
    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "application_name": "AIstock-alter-loop-unique",
    }


def main() -> None:
    cfg = _db_cfg()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            # 删除旧唯一约束（如果存在），约束名按常规命名推测
            cur.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conrelid = 'aistock_loop_catalog'::regclass
                          AND contype = 'u'
                          AND conname = 'aistock_loop_catalog_task_run_id_loop_id_key'
                    ) THEN
                        ALTER TABLE aistock_loop_catalog
                        DROP CONSTRAINT aistock_loop_catalog_task_run_id_loop_id_key;
                    END IF;
                END
                $$;
                """
            )

            # 创建新的唯一约束 (task_run_id, loop_id, workspace_id)
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conrelid = 'aistock_loop_catalog'::regclass
                          AND contype = 'u'
                          AND conname = 'aistock_loop_catalog_task_run_loop_ws_key'
                    ) THEN
                        ALTER TABLE aistock_loop_catalog
                        ADD CONSTRAINT aistock_loop_catalog_task_run_loop_ws_key
                        UNIQUE (task_run_id, loop_id, workspace_id);
                    END IF;
                END
                $$;
                """
            )

        print("aistock_loop_catalog unique constraint adjusted to (task_run_id, loop_id, workspace_id)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
