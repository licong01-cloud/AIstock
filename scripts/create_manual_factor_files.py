"""
为手工因子创建源代码文件，并更新数据库 asset_path 字段。

手工因子当前 asset_path='manual'，code_text 存有源代码。
本脚本从 code_text 创建 .py 文件到 rdagent_assets/manual_factors/ 目录，
格式与 rdagent_tasks 下的因子文件保持一致。
"""
import sys
import os
from pathlib import Path

# AIstock 根目录（脚本在 scripts/ 下）
AISTOCK_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(AISTOCK_ROOT))

MANUAL_FACTORS_DIR = AISTOCK_ROOT / "rdagent_assets" / "manual_factors"

# 加载 .env
env_file = AISTOCK_ROOT / ".env"
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip().strip("\r")
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            v = v.strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)

from backend.db.pg_pool import get_conn


def run():
    MANUAL_FACTORS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"目标目录: {MANUAL_FACTORS_DIR}")

    # 查询所有 asset_path='manual' 且有 code_text 的因子
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT factor_name, code_text
                FROM aistock_factor_catalog
                WHERE source = 'manual'
                  AND (asset_path IS NULL OR asset_path = 'manual')
                  AND code_text IS NOT NULL
                ORDER BY factor_name
            """)
            rows = cur.fetchall()

    print(f"需要处理的手工因子: {len(rows)} 个")

    updated = 0
    skipped = 0
    errors = []

    for factor_name, code_text in rows:
        factor_file = MANUAL_FACTORS_DIR / f"{factor_name}.py"
        try:
            # 计算相对于 aistock_root 的路径（与 rdagent 因子格式一致）
            asset_path_value = factor_file.relative_to(AISTOCK_ROOT).as_posix()

            # 写入源代码文件
            factor_file.write_text(code_text, encoding="utf-8")

            # 更新数据库
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE aistock_factor_catalog
                        SET asset_path = %s
                        WHERE factor_name = %s AND source = 'manual'
                    """, (asset_path_value, factor_name))
                    if cur.rowcount > 0:
                        updated += 1
                        print(f"  [OK] {factor_name} -> {asset_path_value}")
                    else:
                        skipped += 1
                        print(f"  [SKIP] {factor_name}: 数据库更新无效果")

        except Exception as e:
            errors.append(f"{factor_name}: {e}")
            print(f"  [ERR] {factor_name}: {e}")

    print(f"\n完成: 成功={updated}, 跳过={skipped}, 错误={len(errors)}")
    if errors:
        print("错误列表:")
        for e in errors:
            print(f"  {e}")


if __name__ == "__main__":
    run()
