from __future__ import annotations

"""TimescaleDB 参数调整脚本

通过 backend.app_pg.get_conn 修改 timescaledb.max_tuples_decompressed_per_dml_transaction。

用法示例（在项目根目录）：

  # 将参数改为 200000
  python -m backend.scripts.timescaledb_tuning --value 200000

注意：
- 需要对数据库有足够权限（通常需要超级用户或具有 ALTER SYSTEM/ALTER DATABASE 权限）。
- 修改后可能需要重启或 reload 配置，具体取决于参数类型和数据库设置。
"""

import argparse
from typing import Optional

from app_pg import get_conn


def get_current_setting() -> Optional[str]:
    sql = "SHOW timescaledb.max_tuples_decompressed_per_dml_transaction;"
    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    return row[0] if row else None


def reload_conf() -> None:
    """执行 pg_reload_conf() 以让 ALTER SYSTEM 生效（针对 SIGHUP 类参数）。"""

    with get_conn() as conn:  # type: ignore[attr-defined]
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_reload_conf();")


def set_setting(value: int) -> None:
    """将 timescaledb.max_tuples_decompressed_per_dml_transaction 设置为指定值.

    这里使用 ALTER SYSTEM，适用于大多数单实例场景。
    如果你的部署有更严格规范，可以手动改为 ALTER DATABASE/ALTER ROLE 等更细粒度方式。
    """

    sql = "ALTER SYSTEM SET timescaledb.max_tuples_decompressed_per_dml_transaction = %s;"
    # ALTER SYSTEM 不能在事务块中执行，这里临时开启 autocommit
    with get_conn() as conn:  # type: ignore[attr-defined]
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, (value,))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="调整 TimescaleDB timescaledb.max_tuples_decompressed_per_dml_transaction 参数",
    )
    parser.add_argument(
        "--value",
        type=int,
        default=200000,
        help="要设置的新值，默认 200000",
    )

    args = parser.parse_args()
    new_value: int = args.value

    before = get_current_setting()
    print(f"[当前值] timescaledb.max_tuples_decompressed_per_dml_transaction = {before}")

    print(f"[修改] 即将设置为: {new_value}")
    set_setting(new_value)

    # 尝试 reload 配置，让新值立即生效（若该参数支持 SIGHUP 级别变更）
    reload_conf()

    after = get_current_setting()
    print(f"[修改后] timescaledb.max_tuples_decompressed_per_dml_transaction = {after}")
    print("[提示] 若值仍未改变，说明该参数可能需要重启实例才能生效，或当前用户权限不足。")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
