"""
修复 SOTA 因子缺失指标问题

问题: LogCircMV, MainNetAmtRatio_5D, MainNetInflowRatio_1D, Momentum_10D
这 4 个因子 ic/annualized_return/max_drawdown/information_ratio 全部为 NULL。

根因: 同步服务 _fetch_loops_metrics() 使用 tested_factors 精确匹配因子名，
但 RDAgent 早期 loop 的 tested_factors 用中文名（如 "流通市值对数（LogCircMV）"），
而数据库中存的是英文名 "LogCircMV"，导致匹配不上。

本脚本:
1. 从 RDAgent pkl 日志中提取正确的 loop 指标
2. 通过中文名→英文名映射关联
3. 更新数据库指标字段

同时检查并修复其他缺失的元数据字段。
"""

import os
import sys
import re
import json
import pickle
import glob
import psycopg2
from pathlib import Path

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "postgres",
    "password": "lc78080808",
    "dbname": "aistock",
}

LOG_ROOT = Path(r"F:\Dev\RD-Agent-main\log")

# 因子名 → (source_task_id, 需要搜索的 loop)
FACTORS = {
    "LogCircMV": "2026-01-10_07-31-04-239738",
    "MainNetAmtRatio_5D": "2026-01-10_07-31-04-239738",
    "MainNetInflowRatio_1D": "2026-01-13_06-56-49-446055",
    "Momentum_10D": "2026-01-13_06-56-49-446055",
}

# 中文名→英文名提取
def normalize_factor_name(name: str) -> str:
    """从中文括号中提取英文名: '流通市值对数（LogCircMV）' -> 'LogCircMV'"""
    m = re.search(r'[（(]([A-Za-z_][A-Za-z0-9_]*)[）)]', name)
    return m.group(1) if m else name


def extract_metrics_from_pkl(task_id: str, target_factors: list) -> dict:
    """从 pkl 文件提取因子对应 loop 的指标。

    Returns:
        dict: {factor_name: {ic, annualized_return, max_drawdown, information_ratio, loop_id}}
    """
    task_dir = LOG_ROOT / task_id
    if not task_dir.exists():
        # 尝试 WSL 路径映射
        print(f"  [WARN] Task dir not found: {task_dir}")
        return {}

    session_files = sorted(glob.glob(str(task_dir / "__session__" / "*" / "*")))
    if not session_files:
        print(f"  [WARN] No session snapshots for {task_id}")
        return {}

    latest = session_files[-1]
    print(f"  Loading session: {latest}")

    with open(latest, "rb") as f:
        data = pickle.load(f)

    if not hasattr(data, "trace") or not hasattr(data.trace, "hist"):
        print(f"  [WARN] No trace.hist")
        return {}

    hist = data.trace.hist
    results = {}

    # 遍历所有 loop，找到目标因子出现的最早 loop（首次测试的指标最准确）
    for loop_idx, entry in enumerate(hist):
        exp = entry[0] if isinstance(entry, tuple) else entry

        # 收集因子名（规范化）
        factor_names_raw = []
        if hasattr(exp, "sub_workspace_list") and exp.sub_workspace_list:
            for sw in exp.sub_workspace_list:
                if hasattr(sw, "target_task"):
                    name = getattr(sw.target_task, "factor_name", None) or getattr(sw.target_task, "name", "?")
                    factor_names_raw.append(name)

        # 规范化因子名映射
        name_map = {}
        for raw_name in factor_names_raw:
            normalized = normalize_factor_name(raw_name)
            name_map[normalized] = raw_name

        # 检查目标因子是否在此 loop
        matched = [f for f in target_factors if f in name_map and f not in results]
        if not matched:
            continue

        # 提取指标
        result = getattr(exp, "result", None)
        metrics = {}
        if result is not None:
            if hasattr(result, "to_dict") and callable(result.to_dict):
                metrics = result.to_dict()
            elif isinstance(result, dict):
                metrics = result

        if not metrics:
            continue

        ic = metrics.get("IC")
        icir = metrics.get("ICIR")
        ann_ret = metrics.get("1day.excess_return_without_cost.annualized_return")
        max_dd = metrics.get("1day.excess_return_without_cost.max_drawdown")
        info_ratio = metrics.get("1day.excess_return_without_cost.information_ratio")

        for fname in matched:
            results[fname] = {
                "ic": ic,
                "icir": icir,
                "annualized_return": ann_ret,
                "max_drawdown": max_dd,
                "information_ratio": info_ratio,
                "source_loop_id": loop_idx,
                "source_loop_tag": str(loop_idx),
                "performance_metrics": json.dumps(metrics, ensure_ascii=False),
            }
            print(f"  [FOUND] {fname} @ Loop {loop_idx}: IC={ic}, ann_ret={ann_ret}, info_ratio={info_ratio}")

    return results


def main():
    print("=" * 70)
    print("修复 SOTA 因子缺失指标")
    print("=" * 70)

    # 1. 按 task 分组提取指标
    task_factors = {}
    for fname, task_id in FACTORS.items():
        task_factors.setdefault(task_id, []).append(fname)

    all_metrics = {}
    for task_id, factors in task_factors.items():
        print(f"\n--- Task: {task_id} ---")
        print(f"  Target factors: {factors}")
        metrics = extract_metrics_from_pkl(task_id, factors)
        all_metrics.update(metrics)

    if not all_metrics:
        print("\n[ERROR] 未能从 pkl 文件中提取任何指标")
        return

    # 2. 更新数据库
    print(f"\n{'=' * 70}")
    print(f"更新数据库: {len(all_metrics)} 条记录")
    print("=" * 70)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # 先显示当前状态
            cur.execute("""
                SELECT factor_name, ic, annualized_return, max_drawdown,
                       information_ratio, icir, sharpe, source_loop_id,
                       source_loop_tag, performance_metrics IS NOT NULL as has_perf,
                       factor_formulation, description_cn
                FROM aistock_factor_catalog
                WHERE factor_name = ANY(%s) AND source = 'rdagent_task_sync'
            """, (list(all_metrics.keys()),))

            print("\n[更新前]")
            for row in cur.fetchall():
                print(f"  {row[0]:30s} ic={row[1]}  ann_ret={row[2]}  loop_id={row[7]}  loop_tag={row[8]}  has_perf={row[9]}")

            # 执行更新
            update_sql = """
                UPDATE aistock_factor_catalog
                SET ic = %s,
                    icir = %s,
                    annualized_return = %s,
                    max_drawdown = %s,
                    information_ratio = %s,
                    sharpe = %s,
                    source_loop_id = %s,
                    source_loop_tag = %s,
                    performance_metrics = %s,
                    best_performance_ann_ret = %s,
                    best_performance_sharpe = %s
                WHERE factor_name = %s AND source = 'rdagent_task_sync'
            """

            updated = 0
            for fname, m in all_metrics.items():
                cur.execute(update_sql, (
                    m["ic"],
                    m["icir"],
                    m["annualized_return"],
                    m["max_drawdown"],
                    m["information_ratio"],
                    m["information_ratio"],  # sharpe 字段复用 information_ratio
                    m["source_loop_id"],
                    m["source_loop_tag"],
                    m["performance_metrics"],
                    m["annualized_return"],   # best_performance_ann_ret
                    m["information_ratio"],   # best_performance_sharpe
                    fname,
                ))
                if cur.rowcount > 0:
                    updated += 1
                    print(f"  [UPDATED] {fname}: ic={m['ic']:.6f}, ann_ret={m['annualized_return']:.4f}, loop={m['source_loop_id']}")

            conn.commit()
            print(f"\n更新完成: {updated}/{len(all_metrics)}")

            # 验证
            cur.execute("""
                SELECT factor_name, ic, annualized_return, max_drawdown,
                       information_ratio, icir, source_loop_tag
                FROM aistock_factor_catalog
                WHERE factor_name = ANY(%s) AND source = 'rdagent_task_sync'
            """, (list(all_metrics.keys()),))

            print("\n[更新后]")
            for row in cur.fetchall():
                ic_str = f"{row[1]:.6f}" if row[1] else "NULL"
                ann_str = f"{row[2]:.4f}" if row[2] else "NULL"
                dd_str = f"{row[3]:.4f}" if row[3] else "NULL"
                ir_str = f"{row[4]:.2f}" if row[4] else "NULL"
                print(f"  {row[0]:30s} IC={ic_str}  AnnRet={ann_str}  MaxDD={dd_str}  IR={ir_str}  Loop={row[6]}")

    finally:
        conn.close()

    # 3. 检查其他可能缺失的字段
    print(f"\n{'=' * 70}")
    print("检查其他数据缺失")
    print("=" * 70)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # 查找所有 SOTA 因子中指标为 NULL 的
            cur.execute("""
                SELECT factor_name, ic, annualized_return, asset_path,
                       (code_text IS NOT NULL AND code_text != '') as has_code
                FROM aistock_factor_catalog
                WHERE is_sota_factor = TRUE
                  AND source = 'rdagent_task_sync'
                  AND (ic IS NULL OR annualized_return IS NULL)
                ORDER BY factor_name
            """)

            remaining = cur.fetchall()
            if remaining:
                print(f"\n还有 {len(remaining)} 个 SOTA 因子缺失指标:")
                for row in remaining:
                    print(f"  {row[0]:45s} ic={row[1]}  ann_ret={row[2]}  has_code={row[4]}")
            else:
                print("\n所有 SOTA 因子的指标均已完整!")

            # 检查非 SOTA 因子中没有代码的
            cur.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN asset_path IS NULL OR asset_path = '' THEN 1 ELSE 0 END) as no_path,
                       SUM(CASE WHEN code_text IS NULL OR code_text = '' THEN 1 ELSE 0 END) as no_code,
                       SUM(CASE WHEN ic IS NULL THEN 1 ELSE 0 END) as no_ic
                FROM aistock_factor_catalog
                WHERE source = 'rdagent_task_sync'
            """)

            stats = cur.fetchone()
            print(f"\n全量因子统计 (source=rdagent_task_sync):")
            print(f"  总数: {stats[0]}")
            print(f"  缺少 asset_path: {stats[1]}")
            print(f"  缺少 code_text: {stats[2]}")
            print(f"  缺少 IC: {stats[3]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
