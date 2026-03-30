"""增量因子指标批量计算脚本

对指定 Task 列表调用 RDAgent factor_metrics API 并写入 DB。
每个 Task 计算耗时 2-5 分钟（取决于因子数量）。

使用方式:
    python batch_factor_metrics_sync.py [--timeout 600]
"""
import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from dotenv import load_dotenv
_env = Path(__file__).resolve().parents[2] / ".env"
if _env.exists():
    load_dotenv(_env, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RDAGENT_URL = os.getenv("RDAGENT_API_URL", "http://localhost:9000")


# UPSERT SQL（与 rdagent_factor_metrics_sync.py 一致）
_UPSERT_SQL = """
INSERT INTO aistock_factor_metrics (
    factor_name, calculated_at, data_start, data_end, eval_window,
    return_horizon, universe,
    ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir, ic_positive_ratio,
    top_annual_return, top_excess_annual_return, top_sharpe,
    top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
    group_return_monotonicity, turnover, ic_decay_half_life,
    ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
    coverage, n_trading_days, source_task_id, calc_batch_id, calc_engine
) VALUES (
    %(factor_name)s, %(calculated_at)s, %(data_start)s, %(data_end)s, %(eval_window)s,
    %(return_horizon)s, %(universe)s,
    %(ic_mean)s, %(ic_std)s, %(rank_ic_mean)s, %(rank_ic_std)s, %(icir)s, %(rank_icir)s, %(ic_positive_ratio)s,
    %(top_annual_return)s, %(top_excess_annual_return)s, %(top_sharpe)s,
    %(top_max_drawdown)s, %(top_excess_sharpe)s, %(benchmark_annual_return)s,
    %(group_return_monotonicity)s, %(turnover)s, %(ic_decay_half_life)s,
    %(ic_csz_mean)s, %(rank_ic_1d)s, %(rank_ic_5d)s, %(rank_ic_10d)s, %(rank_ic_20d)s,
    %(coverage)s, %(n_trading_days)s, %(source_task_id)s, %(calc_batch_id)s, %(calc_engine)s
)
ON CONFLICT (factor_name, eval_window, data_start, data_end, calculated_at)
DO UPDATE SET
    ic_mean = EXCLUDED.ic_mean,
    rank_ic_mean = EXCLUDED.rank_ic_mean,
    icir = EXCLUDED.icir,
    rank_icir = EXCLUDED.rank_icir,
    top_annual_return = EXCLUDED.top_annual_return,
    top_sharpe = EXCLUDED.top_sharpe,
    top_max_drawdown = EXCLUDED.top_max_drawdown,
    source_task_id = EXCLUDED.source_task_id
"""


def insert_metrics_batch(metrics_list, source_task_id):
    """将指标列表批量写入数据库。"""
    from db.pg_pool import get_conn

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for m in metrics_list:
                params = {
                    "factor_name": m["factor_name"],
                    "calculated_at": m["calculated_at"],
                    "data_start": m["data_start"],
                    "data_end": m["data_end"],
                    "eval_window": m["eval_window"],
                    "return_horizon": m.get("return_horizon", "T2T1"),
                    "universe": m.get("universe", "all"),
                    "ic_mean": m.get("ic_mean"),
                    "ic_std": m.get("ic_std"),
                    "rank_ic_mean": m.get("rank_ic_mean"),
                    "rank_ic_std": m.get("rank_ic_std"),
                    "icir": m.get("icir"),
                    "rank_icir": m.get("rank_icir"),
                    "ic_positive_ratio": m.get("ic_positive_ratio"),
                    "top_annual_return": m.get("top_annual_return"),
                    "top_excess_annual_return": m.get("top_excess_annual_return"),
                    "top_sharpe": m.get("top_sharpe"),
                    "top_max_drawdown": m.get("top_max_drawdown"),
                    "top_excess_sharpe": m.get("top_excess_sharpe"),
                    "benchmark_annual_return": m.get("benchmark_annual_return"),
                    "group_return_monotonicity": m.get("group_return_monotonicity"),
                    "turnover": m.get("turnover"),
                    "ic_decay_half_life": m.get("ic_decay_half_life"),
                    "ic_csz_mean": m.get("ic_csz_mean"),
                    "rank_ic_1d": m.get("rank_ic_1d"),
                    "rank_ic_5d": m.get("rank_ic_5d"),
                    "rank_ic_10d": m.get("rank_ic_10d"),
                    "rank_ic_20d": m.get("rank_ic_20d"),
                    "coverage": m.get("coverage"),
                    "n_trading_days": m.get("n_trading_days"),
                    "source_task_id": source_task_id,
                    "calc_batch_id": m.get("calc_batch_id"),
                    "calc_engine": "rdagent",
                }
                cur.execute(_UPSERT_SQL, params)
                inserted += 1
    return inserted


def main():
    import requests

    parser = argparse.ArgumentParser(description="增量因子指标计算")
    parser.add_argument("--timeout", type=int, default=600, help="RDAgent API 超时 (默认 600s)")
    args = parser.parse_args()

    task_ids = [
        "2026-02-10_16-04-18-066673",
        "2026-02-06_16-50-41-691996",
        "2026-02-06_10-02-12-151236",
        "2026-02-05_15-00-54-712154",
        "2026-02-05_13-19-36-368740",
        "2026-02-04_14-08-57-830796",
        "2026-02-04_07-30-06-563166",
        "2026-02-04_06-08-38-001784",
        "2026-02-04_04-26-14-210887",
        "2026-02-03_14-35-06-358725",
        "2026-02-02_14-58-13-793902",
        "2026-01-09_16-11-14-215581",
        "2026-01-08_17-10-10-330444",
        "2026-01-08_11-40-03-352963",
        "2026-01-07_01-55-44-876621",
        "2026-01-06_06-00-53-321254",
        "2026-01-04_01-57-00-088565",
        "2026-01-03_03-53-51-394540",
        "2026-01-01_07-10-05-716729",
        "2025-12-30_10-24-18-730664",
        "2025-12-30_02-18-55-141459",
    ]

    # 检查已有数据
    from db.pg_pool import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT source_task_id
                FROM aistock_factor_metrics
                WHERE source_task_id = ANY(%s)
            """, (task_ids,))
            existing = {r[0] for r in cur.fetchall()}

    pending = [t for t in task_ids if t not in existing]
    logger.info(f"共 {len(task_ids)} 个 Task，已完成 {len(existing)} 个，待计算 {len(pending)} 个")
    if existing:
        logger.info(f"已完成: {sorted(existing)}")

    if not pending:
        logger.info("无需计算，退出")
        return

    total_ok = 0
    total_fail = 0
    total_inserted = 0

    for i, task_id in enumerate(pending, 1):
        logger.info(f"[{i}/{len(pending)}] 计算 {task_id}...")
        t0 = time.time()
        try:
            url = f"{RDAGENT_URL}/api/extractors/sota_factors/v2/{task_id}/factor_metrics"
            resp = requests.get(url, timeout=args.timeout)
            if resp.status_code != 200:
                total_fail += 1
                logger.warning(f"  FAIL: HTTP {resp.status_code} ({time.time()-t0:.1f}s)")
                continue

            data = resp.json()
            if not data.get("success"):
                total_fail += 1
                logger.warning(f"  FAIL: {data.get('error', 'unknown')} ({time.time()-t0:.1f}s)")
                continue

            metrics_list = data.get("metrics", [])
            factor_count = data.get("factor_count", 0)

            if not metrics_list:
                logger.info(f"  无指标数据 ({time.time()-t0:.1f}s)")
                total_ok += 1
                continue

            inserted = insert_metrics_batch(metrics_list, task_id)
            elapsed = time.time() - t0
            total_ok += 1
            total_inserted += inserted
            logger.info(f"  OK: {factor_count} factors, {inserted} metrics inserted ({elapsed:.1f}s)")

        except Exception as e:
            total_fail += 1
            logger.error(f"  ERROR: {e} ({time.time()-t0:.1f}s)")

    logger.info(
        f"批量完成: {total_ok} ok, {total_fail} fail, "
        f"{total_inserted} metrics inserted, 共 {len(pending)} 个 Task"
    )


if __name__ == "__main__":
    main()
