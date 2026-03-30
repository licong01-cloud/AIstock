"""
修复 SOTA 因子缺失源代码问题

问题: LogCircMV, MainNetAmtRatio_5D, MainNetInflowRatio_1D, Momentum_10D
这 4 个 SOTA 因子在数据库中 asset_path=NULL, code_text=NULL，
原因是同步时因子文件名与数据库中因子名不匹配（中文前缀、前缀差异等）。

本脚本:
1. 从早期 task 目录中找到已有的源代码文件
2. 复制到统一目录 rdagent_assets/recovered_factors/{factor_name}.py
3. 更新数据库: asset_path + code_text
"""

import os
import sys
import shutil
import psycopg2
from pathlib import Path

# ─── 配置 ───────────────────────────────────────────────
AISTOCK_ROOT = Path(r"F:\Dev\AIstock")
RECOVERED_DIR = AISTOCK_ROOT / "rdagent_assets" / "recovered_factors"

DB_CONFIG = {
    "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("TDX_DB_PORT", "5432")),
    "user": os.getenv("TDX_DB_USER", "postgres"),
    "password": os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    "dbname": os.getenv("TDX_DB_NAME", "aistock"),
}

# 因子名 → 源代码文件路径（从早期 task 目录中已确认存在）
FACTOR_SOURCE_MAP = {
    "LogCircMV": AISTOCK_ROOT / "rdagent_assets/rdagent_tasks/2025-12-23_05-59-43-369830/factors/LogCircMV.py",
    "MainNetAmtRatio_5D": AISTOCK_ROOT / "rdagent_assets/rdagent_tasks/2026-01-10_07-31-04-239738/factors/主力资金净流入强度5日滚动（MainNetAmtRatio_5D）.py",
    "MainNetInflowRatio_1D": AISTOCK_ROOT / "rdagent_assets/rdagent_tasks/2026-01-08_11-40-03-352963/factors/MainNetInflowRatio_1D.py",
    "Momentum_10D": AISTOCK_ROOT / "rdagent_assets/rdagent_tasks/2026-01-04_01-57-00-088565/factors/Momentum_10D.py",
}

SOURCE = "rdagent_task_sync"


def main():
    # 1. 创建恢复目录
    RECOVERED_DIR.mkdir(parents=True, exist_ok=True)
    print(f"恢复目录: {RECOVERED_DIR}")

    # 2. 复制文件 + 读取代码
    factor_data = {}
    for factor_name, src_path in FACTOR_SOURCE_MAP.items():
        if not src_path.exists():
            print(f"  [ERROR] {factor_name}: 源文件不存在 {src_path}")
            continue

        dst_path = RECOVERED_DIR / f"{factor_name}.py"
        shutil.copy2(src_path, dst_path)

        code_text = dst_path.read_text(encoding="utf-8")
        asset_path = dst_path.relative_to(AISTOCK_ROOT).as_posix()

        factor_data[factor_name] = {
            "code_text": code_text,
            "asset_path": asset_path,
            "src": str(src_path),
        }
        print(f"  [OK] {factor_name}: {len(code_text)} chars -> {asset_path}")

    if not factor_data:
        print("\n没有可恢复的因子，退出。")
        return

    # 3. 更新数据库
    print(f"\n连接数据库 {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            # 先查看当前状态
            cur.execute("""
                SELECT factor_name, asset_path,
                       (code_text IS NOT NULL AND code_text != '') as has_code,
                       source_task_id
                FROM aistock_factor_catalog
                WHERE factor_name = ANY(%s) AND source = %s
            """, (list(factor_data.keys()), SOURCE))

            print("\n=== 更新前状态 ===")
            rows = cur.fetchall()
            if not rows:
                print("  [WARN] 数据库中未找到这些因子记录！")
                conn.close()
                return

            for row in rows:
                print(f"  {row[0]:30s} asset_path={row[1]}  has_code={row[2]}  task={row[3]}")

            # 执行更新
            update_sql = """
                UPDATE aistock_factor_catalog
                SET asset_path = %s,
                    code_text = %s
                WHERE factor_name = %s AND source = %s
            """

            updated = 0
            for factor_name, data in factor_data.items():
                cur.execute(update_sql, (
                    data["asset_path"],
                    data["code_text"],
                    factor_name,
                    SOURCE,
                ))
                if cur.rowcount > 0:
                    updated += 1
                    print(f"  [UPDATED] {factor_name}: asset_path={data['asset_path']}")
                else:
                    print(f"  [SKIP] {factor_name}: 记录不存在 (source={SOURCE})")

            conn.commit()
            print(f"\n更新完成: {updated}/{len(factor_data)} 条记录")

            # 验证更新结果
            cur.execute("""
                SELECT factor_name, asset_path,
                       (code_text IS NOT NULL AND code_text != '') as has_code
                FROM aistock_factor_catalog
                WHERE factor_name = ANY(%s) AND source = %s
            """, (list(factor_data.keys()), SOURCE))

            print("\n=== 更新后状态 ===")
            for row in cur.fetchall():
                status = "OK" if row[1] and row[2] else "STILL MISSING"
                print(f"  {row[0]:30s} asset_path={row[1]}  has_code={row[2]}  [{status}]")

    finally:
        conn.close()

    # 4. 验证文件可读
    print("\n=== 文件可读性验证 ===")
    for factor_name, data in factor_data.items():
        abs_path = AISTOCK_ROOT / data["asset_path"]
        if abs_path.exists():
            content = abs_path.read_text(encoding="utf-8")
            has_calculate = f"calculate_{factor_name}" in content
            print(f"  {factor_name:30s} file_exists=True  has_calculate_func={has_calculate}  size={len(content)}")
        else:
            print(f"  {factor_name:30s} file_exists=False  [ERROR]")


if __name__ == "__main__":
    main()
