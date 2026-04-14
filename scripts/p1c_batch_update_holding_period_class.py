"""P1c: 批量更新现有因子的 holding_period_class 字段。

从 aistock_factor_metrics 读取每个因子最新的 ic_decay_half_life,
应用 classify_holding_period 规则,批量写入 qe_factor_classification.holding_period_class。

分界点: short(<8d) / medium(8-25d) / long(>25d) / unknown(NULL)

执行方式:
  cd F:/Dev/AIstock
  python scripts/p1c_batch_update_holding_period_class.py
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from backend.services.quantevolver.factor_analyst import classify_holding_period


def main():
    conn = psycopg2.connect(
        host=os.environ['TDX_DB_HOST'],
        port=os.environ['TDX_DB_PORT'],
        dbname=os.environ['TDX_DB_NAME'],
        user=os.environ['TDX_DB_USER'],
        password=os.environ['TDX_DB_PASSWORD'],
    )
    cur = conn.cursor()

    # 1. 读取每个因子的最新 full 窗口半衰期
    cur.execute("""
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    """)
    metrics_rows = cur.fetchall()
    print(f"从 aistock_factor_metrics 读取 {len(metrics_rows)} 个因子的最新半衰期")

    # 2. 读取 qe_factor_classification 中所有已分类因子 (考虑 factor_source 不同)
    cur.execute("""
        SELECT factor_name, factor_source FROM qe_factor_classification
    """)
    cls_rows = cur.fetchall()
    print(f"qe_factor_classification 中已有 {len(cls_rows)} 条分类记录")

    # 3. 构建 factor_name → half_life 映射
    hl_map = {name: hl for name, hl in metrics_rows}

    # 4. 批量更新
    updated = 0
    no_metrics = 0
    stats = {"short": 0, "medium": 0, "long": 0, "unknown": 0}

    for factor_name, factor_source in cls_rows:
        hl = hl_map.get(factor_name)
        cls = classify_holding_period(hl)
        stats[cls] += 1

        if hl is None and factor_name not in hl_map:
            no_metrics += 1

        cur.execute("""
            UPDATE qe_factor_classification
            SET holding_period_class = %s
            WHERE factor_name = %s AND factor_source = %s
        """, (cls, factor_name, factor_source))
        updated += cur.rowcount

    conn.commit()
    print(f"\n✓ 已更新 {updated} 条记录")
    print(f"  无 metrics 数据: {no_metrics}")
    print(f"\n--- 分类分布 ---")
    total = sum(stats.values())
    for k in ["short", "medium", "long", "unknown"]:
        v = stats[k]
        pct = v / total * 100 if total > 0 else 0
        print(f"  {k:>8s}: {v:5d} ({pct:5.1f}%)")

    # 5. 交叉分析: 有效因子(S/A/B)的分布
    cur.execute("""
        SELECT holding_period_class, grade, COUNT(*)
        FROM qe_factor_classification
        WHERE grade IN ('S', 'A', 'B')
        GROUP BY holding_period_class, grade
        ORDER BY holding_period_class, grade
    """)
    print(f"\n--- 有效因子 (S/A/B) 按分类 × grade 分布 ---")
    print(f"  {'class':>8s} {'grade':>5s} {'count':>6s}")
    for cls, grade, cnt in cur.fetchall():
        print(f"  {cls or 'NULL':>8s} {grade:>5s} {cnt:6d}")

    conn.close()


if __name__ == "__main__":
    main()
