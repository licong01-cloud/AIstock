"""诊断并补齐 aistock_factor_catalog 的 TASK 指标。

数据来源：performance_metrics JSONB 字段中保存了原始 TASK 运行指标。
本脚本从该字段提取 ic/sharpe/annualized_return 回填到平铺列。

执行方式：
  cd F:/dev/AIstock
  python -m backend.scripts.restore_task_metrics          # 执行补齐
  python -m backend.scripts.restore_task_metrics --dry-run # 仅诊断
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from backend.db.pg_pool import get_conn


def step0_diagnose():
    """诊断当前数据状态。"""
    print("=" * 60)
    print("Step 0: 诊断当前数据状态")
    print("=" * 60)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) factor_catalog 总体填充率
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(ic) AS has_ic,
                       COUNT(sharpe) AS has_sharpe,
                       COUNT(annualized_return) AS has_ann_ret,
                       COUNT(performance_metrics) AS has_perf
                FROM aistock_factor_catalog
            """)
            r = cur.fetchone()
            print(f"\n  aistock_factor_catalog:")
            print(f"    总数: {r[0]}")
            print(f"    有IC: {r[1]}, 有Sharpe: {r[2]}, 有年化: {r[3]}")
            print(f"    有performance_metrics: {r[4]}")

            # 2) perf_metrics有值但ic为NULL的（需要补齐）
            cur.execute("""
                SELECT COUNT(*) FROM aistock_factor_catalog
                WHERE performance_metrics IS NOT NULL
                  AND (performance_metrics->>'ic') IS NOT NULL
                  AND ic IS NULL
            """)
            need_backfill = cur.fetchone()[0]
            print(f"    需要从perf_metrics补齐ic的: {need_backfill}")

            # 3) 独立指标覆盖率
            cur.execute("""
                SELECT COUNT(DISTINCT factor_name) FROM aistock_factor_metrics
            """)
            ind_count = cur.fetchone()[0]
            print(f"\n  aistock_factor_metrics:")
            print(f"    有独立指标的因子数: {ind_count}")

            # 4) TASK vs 独立指标对比
            cur.execute("""
                SELECT c.factor_name,
                       c.ic AS task_ic,
                       c.sharpe AS task_sharpe,
                       c.annualized_return AS task_ann_ret,
                       m.ic_mean AS ind_ic,
                       m.rank_ic_mean AS ind_rank_ic,
                       m.top_excess_sharpe AS ind_sharpe,
                       m.top_excess_annual_return AS ind_ann_ret
                FROM aistock_factor_catalog c
                JOIN LATERAL (
                    SELECT ic_mean, rank_ic_mean, top_excess_sharpe,
                           top_excess_annual_return
                    FROM aistock_factor_metrics
                    WHERE factor_name = c.factor_name
                      AND eval_window = 'full'
                    ORDER BY calculated_at DESC LIMIT 1
                ) m ON TRUE
                WHERE c.ic IS NOT NULL
                ORDER BY c.sharpe DESC NULLS LAST
                LIMIT 10
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\n  TASK vs 独立指标对比 (Top10 by TASK Sharpe):")
                hdr = (f"  {'Factor':<32} {'T_IC':>7} {'I_IC':>7} "
                       f"{'T_Sh':>7} {'I_Sh':>7} "
                       f"{'T_Ann%':>8} {'I_Ann%':>8}")
                print(hdr)
                print("  " + "-" * 80)
                for row in rows:
                    nm = row[0][:30]
                    t_ic = f"{row[1]:.4f}" if row[1] else "  N/A"
                    i_ic = f"{row[4]:.4f}" if row[4] else "  N/A"
                    t_sh = f"{row[2]:.3f}" if row[2] else "  N/A"
                    i_sh = f"{row[6]:.3f}" if row[6] else "  N/A"
                    t_ar = f"{row[3]*100:.1f}%" if row[3] else "  N/A"
                    i_ar = f"{row[7]*100:.1f}%" if row[7] else "  N/A"
                    print(f"  {nm:<32} {t_ic:>7} {i_ic:>7} "
                          f"{t_sh:>7} {i_sh:>7} "
                          f"{t_ar:>8} {i_ar:>8}")
            else:
                print("\n  (无同时拥有TASK和独立指标的因子)")

    print()
    return need_backfill


def step1_backfill_from_perf_metrics():
    """从 performance_metrics JSONB 补齐 ic/sharpe/annualized_return。"""
    print("=" * 60)
    print("Step 1: 从 performance_metrics 补齐平铺列")
    print("=" * 60)

    sql = """
    UPDATE aistock_factor_catalog
    SET
        ic = COALESCE(ic,
            (performance_metrics->>'ic')::double precision),
        sharpe = COALESCE(sharpe,
            (performance_metrics->>'information_ratio')::double precision),
        annualized_return = COALESCE(annualized_return,
            (performance_metrics->>'annualized_return')::double precision)
    WHERE performance_metrics IS NOT NULL
      AND (ic IS NULL OR sharpe IS NULL OR annualized_return IS NULL)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            affected = cur.rowcount
            print(f"  补齐行数: {affected}")

    print()
    return affected


def step2_verify():
    """验证补齐结果。"""
    print("=" * 60)
    print("Step 2: 验证结果")
    print("=" * 60)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(ic) AS has_ic,
                       COUNT(sharpe) AS has_sharpe,
                       COUNT(annualized_return) AS has_ann_ret
                FROM aistock_factor_catalog
            """)
            r = cur.fetchone()
            print(f"\n  补齐后:")
            print(f"    总数: {r[0]}, 有IC: {r[1]}, "
                  f"有Sharpe: {r[2]}, 有年化: {r[3]}")

            # 采样
            cur.execute("""
                SELECT factor_name, ic, sharpe,
                       ROUND((annualized_return*100)::numeric, 2) AS ann_pct
                FROM aistock_factor_catalog
                WHERE ic IS NOT NULL
                ORDER BY sharpe DESC NULLS LAST
                LIMIT 5
            """)
            print(f"\n  Top5 by Sharpe:")
            for r in cur.fetchall():
                print(f"    {r[0]:<35} IC={r[1]:.4f} "
                      f"Sharpe={r[2]:.3f} Ann={r[3]}%")

    print()


if __name__ == "__main__":
    need = step0_diagnose()

    if "--dry-run" in sys.argv:
        print("DRY RUN 模式，不执行写入。")
        sys.exit(0)

    if need > 0:
        step1_backfill_from_perf_metrics()
        step2_verify()
    else:
        print("所有 performance_metrics 已正确映射到平铺列，无需补齐。")

    print("Done.")
