"""验证 RD-Agent Phase2 Catalog 三张表的数据情况.

检查内容：
- aistock_factor_catalog 行数与样例行
- aistock_strategy_catalog 行数与样例行
- aistock_loop_catalog 行数与样例行

使用 .env 中的 TDX_DB_* 环境变量连接 PostgreSQL。

运行方式（在项目根目录 F:\Dev\AIstock 下）：

    python -m scripts.verify_rdagent_catalog_tables
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Tuple

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
        "application_name": "AIstock-rdagent-catalog-verify",
    }


def _print_rows(title: str, rows: Iterable[Tuple[Any, ...]]) -> None:
    print(f"\n== {title} ==")
    for row in rows:
        print("  ", row)


def main() -> None:
    cfg = _db_cfg()
    conn = psycopg2.connect(**cfg)

    try:
        with conn.cursor() as cur:
            # 因子表
            cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog")
            factor_cnt = cur.fetchone()[0]
            print(f"aistock_factor_catalog: {factor_cnt} rows")

            cur.execute(
                """
                SELECT factor_name, source, region, tags
                FROM aistock_factor_catalog
                ORDER BY factor_name
                LIMIT 5
                """
            )
            _print_rows("aistock_factor_catalog sample", cur.fetchall())

            # 策略表
            cur.execute("SELECT COUNT(*) FROM aistock_strategy_catalog")
            strategy_cnt = cur.fetchone()[0]
            print(f"aistock_strategy_catalog: {strategy_cnt} rows")

            cur.execute(
                """
                SELECT strategy_id, step_name, action, example_workspace_path
                FROM aistock_strategy_catalog
                ORDER BY strategy_id
                LIMIT 5
                """
            )
            _print_rows("aistock_strategy_catalog sample", cur.fetchall())

            # loop 表
            cur.execute("SELECT COUNT(*) FROM aistock_loop_catalog")
            loop_cnt = cur.fetchone()[0]
            print(f"aistock_loop_catalog: {loop_cnt} rows")

            cur.execute(
                """
                SELECT task_run_id, loop_id, strategy_id, status, metrics
                FROM aistock_loop_catalog
                ORDER BY task_run_id, loop_id
                LIMIT 5
                """
            )
            _print_rows("aistock_loop_catalog sample", cur.fetchall())

    finally:
        conn.close()


if __name__ == "__main__":
    main()
