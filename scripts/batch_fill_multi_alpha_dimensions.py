"""
批量为所有因子填充 Multi-Alpha 架构所需的 4 个新分类维度:
  - data_source_group
  - update_freq
  - linearity
  - (ts_info_density 需要因子值数据，暂跳过)

注意：正式评级已迁移到统一规则引擎，本脚本不再更新任何正式 grade。

用法: python -m scripts.batch_fill_multi_alpha_dimensions
"""
import os
import sys
import logging

os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
os.environ.setdefault("TDX_DB_PORT", "5432")
os.environ.setdefault("TDX_DB_NAME", "aistock")
os.environ.setdefault("TDX_DB_USER", "postgres")
os.environ.setdefault("TDX_DB_PASSWORD", os.environ.get("TDX_DB_PASSWORD", os.environ.get("DB_PASSWORD", "")))

sys.path.insert(0, '/mnt/f/Dev/AIstock')

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger("batch_fill_ma_dims")

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.factor_analyst import (
    classify_data_source, determine_update_freq, compute_linearity,
)


def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 获取所有已有分类的因子（含 code_text / expression）
            cur.execute("""
                SELECT c.id, c.factor_name, c.source, c.code_text, c.expression,
                       fc.id as cls_id, fc.category, fc.grade,
                       fc.ic_value, fc.sharpe_value,
                       fc.data_source_group, fc.update_freq, fc.linearity
                FROM aistock_factor_catalog c
                LEFT JOIN qe_factor_classification fc
                    ON fc.factor_name = c.factor_name AND fc.factor_source = c.source
                WHERE c.is_available = TRUE
                ORDER BY c.id
            """)
            factors = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    print(f"扫描 {len(factors)} 个可用因子")

    stats = {"updated": 0, "skipped_nocls": 0, "errors": 0}
    ds_distribution = {}

    for f in factors:
        try:
            factor_name = f["factor_name"]
            factor_source = f["source"]

            if f["cls_id"] is None:
                stats["skipped_nocls"] += 1
                continue

            # ── 计算 4 个新维度 ──
            ds_group = classify_data_source(
                code_text=f.get("code_text"),
                expression=f.get("expression"),
                factor_name=factor_name,
            )
            upd_freq = determine_update_freq(ds_group)

            # 取 aistock_factor_metrics 的 pearson/spearman IC 算 linearity
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT ic_mean, rank_ic_mean,
                               icir_annualized, top_excess_sharpe, top_excess_annual_return,
                               ic_decay_half_life,
                               rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
                        FROM aistock_factor_metrics
                        WHERE factor_name = %s AND eval_window = 'full'
                        ORDER BY calculated_at DESC LIMIT 1
                    """, (factor_name,))
                    metrics_row = cur.fetchone()

            if metrics_row:
                (ic_mean, rank_ic_mean, icir_ann, sharpe, ann_ret,
                 half_life, rk1d, rk5d, rk10d, rk20d) = metrics_row
                linearity = compute_linearity(ic_mean, rank_ic_mean)
            else:
                linearity = None

            # ── 写入DB（仅更新分类维度，不写正式grade） ──
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_factor_classification SET
                            data_source_group = %s,
                            update_freq = %s,
                            linearity = COALESCE(%s, linearity)
                        WHERE id = %s
                    """, (ds_group, upd_freq, linearity, f["cls_id"]))

            ds_distribution[ds_group] = ds_distribution.get(ds_group, 0) + 1
            stats["updated"] += 1
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] < 5:
                print(f"  [ERR] {f.get('factor_name')}: {e}")

    print(f"\n=== 统计 ===")
    print(f"更新: {stats['updated']}")
    print(f"跳过(无分类): {stats['skipped_nocls']}")
    print(f"错误: {stats['errors']}")

    print(f"\n=== data_source_group 分布 ===")
    for k, v in sorted(ds_distribution.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print(f"\n=== 最终 data_source_group 分布 ===")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data_source_group, COUNT(*) FROM qe_factor_classification GROUP BY data_source_group ORDER BY COUNT(*) DESC NULLS LAST")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")


if __name__ == "__main__":
    main()
