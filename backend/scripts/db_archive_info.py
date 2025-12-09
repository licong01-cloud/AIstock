from __future__ import annotations

"""数据库归档/ WAL 配置检查脚本

通过 backend.app_pg.get_conn 查询当前实例的归档相关配置：
- wal_level
- archive_mode
- archive_command
- data_directory

用法示例（在项目根目录 AIstock 下）：

  python -m backend.scripts.db_archive_info
"""

from typing import List, Tuple

from app_pg import get_conn


def fetch_settings(keys: List[str]) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            for key in keys:
                cur.execute(f"SHOW {key};")
                row = cur.fetchone()
                value = row[0] if row else "<unknown>"
                results.append((key, value))
    return results


def main() -> int:
    keys = [
        "wal_level",
        "archive_mode",
        "archive_command",
        "data_directory",
    ]

    print("[归档/WAL 配置检查]")
    for key, value in fetch_settings(keys):
        print(f"- {key} = {value}")

    print("\n[说明]")
    print("- 若 archive_mode = on 且 wal_level 为 replica/logical，则说明启用了归档/高等级 WAL。")
    print("- archive_command 通常包含归档目录路径，可据此在数据库所在机器上查看归档文件大小。")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
