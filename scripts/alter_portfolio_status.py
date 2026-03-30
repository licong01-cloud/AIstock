"""验证性脚本 — 确认 portfolio_config.status (TEXT) 支持新状态值.

portfolio_config.status 是 TEXT 类型，无需 ALTER TABLE。
此脚本仅做文档性断言，验证数据库支持新状态值。

新状态值: catching_up, caught_up（加上已有的 created, running, paused, stopped）

用法: python -m scripts.alter_portfolio_status
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.db.pg_pool import get_conn


EXPECTED_STATES = {"created", "running", "paused", "stopped", "catching_up", "caught_up"}


def verify_status_column():
    """验证 status 列是 TEXT 类型，支持任意字符串值."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 确认列类型
            cur.execute(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'paper_trading'
                  AND table_name = 'portfolio_config'
                  AND column_name = 'status'
                """
            )
            row = cur.fetchone()
            if row is None:
                print("ERROR: status 列不存在于 paper_trading.portfolio_config")
                return False

            data_type = row[0]
            print(f"status 列类型: {data_type}")

            if data_type not in ("text", "character varying"):
                print(f"WARNING: status 列类型为 {data_type}，可能不支持任意字符串")
                return False

            # 查询当前所有状态值
            cur.execute(
                "SELECT DISTINCT status FROM paper_trading.portfolio_config ORDER BY status"
            )
            existing = {r[0] for r in cur.fetchall()}
            print(f"当前已有状态值: {existing}")
            print(f"期望支持的状态值: {EXPECTED_STATES}")

            missing = EXPECTED_STATES - existing
            if missing:
                print(f"以下状态值尚未在数据中出现（这是正常的）: {missing}")

            print("验证通过: TEXT 类型无需 ALTER TABLE，可直接使用新状态值")
            return True


if __name__ == "__main__":
    ok = verify_status_column()
    sys.exit(0 if ok else 1)
