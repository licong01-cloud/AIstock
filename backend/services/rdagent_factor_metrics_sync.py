"""单因子指标批量同步服务。

从 RD-Agent 侧 API 获取 17 项单因子指标，写入 aistock_factor_metrics 表。
支持按 task_id 批量计算并入库，用于同步完成后的统一批处理。

数据来源：
- GET /api/extractors/sota_factors/v2/{task_id}/factor_metrics
- POST /api/extractors/sota_factors/v2/batch/factor_metrics
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..db.pg_pool import get_conn
from .rdagent_results_api_client import RDAgentResultsApiClient

logger = logging.getLogger("aistock.factor_metrics_sync")

_rdagent_client = RDAgentResultsApiClient()


def _normalize_factor_name(name: str) -> str:
    """归一化因子名：小写 + 去除下划线，用于模糊比较。

    例如：MF_MainNetAmtRatio_5D → mfmainnetamtratio5d
         mf_main_net_amt_ratio_5d → mfmainnetamtratio5d
    """
    return name.lower().replace("_", "")


def _build_factor_catalog_map(
    cur,
    factor_names: List[str],
    source_task_id: str,
) -> Dict[str, tuple]:
    """构建 API 因子名 → (catalog 因子名, catalog_id) 的映射。

    匹配优先级：
    1a. 当前 task 的 catalog 条目 — 精确匹配
    1b. 当前 task 的 catalog 条目 — 归一化匹配（解决 PascalCase vs snake_case 不一致）
    2.  全局 catalog — 精确匹配（处理跨 task 共享的因子）
    """
    result: Dict[str, tuple] = {}  # api_name → (catalog_name, catalog_id)
    if not factor_names:
        return result

    # Phase 1: 获取当前 task 的 catalog 条目，优先匹配
    task_catalog: Dict[str, int] = {}  # catalog_name → id
    if source_task_id:
        cur.execute("""
            SELECT factor_name, id
            FROM aistock_factor_catalog
            WHERE source_task_id = %s
        """, (source_task_id,))
        task_catalog = {row[0]: row[1] for row in cur.fetchall()}

    # 构建归一化名称索引
    norm_task_catalog: Dict[str, tuple] = {}  # normalized → (catalog_name, id)
    for cat_name, cat_id in task_catalog.items():
        norm_task_catalog[_normalize_factor_name(cat_name)] = (cat_name, cat_id)

    for api_name in factor_names:
        # 1a: 精确匹配当前 task
        if api_name in task_catalog:
            result[api_name] = (api_name, task_catalog[api_name])
            continue

        # 1b: 归一化匹配当前 task
        norm_api = _normalize_factor_name(api_name)
        if norm_api in norm_task_catalog:
            cat_name, cat_id = norm_task_catalog[norm_api]
            result[api_name] = (cat_name, cat_id)
            logger.info(
                f"因子名模糊匹配: parquet '{api_name}' → catalog '{cat_name}' (id={cat_id})"
            )
            continue

    # Phase 2: 剩余未匹配的做全局精确匹配
    unmatched = [n for n in factor_names if n not in result]
    if unmatched:
        ph = ",".join(["%s"] * len(unmatched))
        cur.execute(f"""
            SELECT DISTINCT ON (factor_name) factor_name, id
            FROM aistock_factor_catalog
            WHERE factor_name IN ({ph})
            ORDER BY factor_name, id
        """, unmatched)
        for row in cur.fetchall():
            result[row[0]] = (row[0], row[1])

    return result


@dataclass
class MetricsSyncResult:
    """因子指标同步结果。"""
    ok: bool
    task_id: str
    factor_count: int = 0
    metrics_inserted: int = 0
    metrics_skipped: int = 0
    errors: List[str] = field(default_factory=list)


# UPSERT SQL（冲突时更新，允许重新计算覆盖旧指标）
_UPSERT_SQL = """
INSERT INTO aistock_factor_metrics (
    factor_name, calculated_at, data_start, data_end, eval_window,
    return_horizon, universe,
    ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir, ic_positive_ratio,
    top_annual_return, top_excess_annual_return, top_sharpe,
    top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
    group_return_monotonicity, turnover, ic_decay_half_life,
    ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
    coverage, n_trading_days, source_task_id, calc_batch_id, calc_engine,
    factor_catalog_id
) VALUES (
    %(factor_name)s, %(calculated_at)s, %(data_start)s, %(data_end)s, %(eval_window)s,
    %(return_horizon)s, %(universe)s,
    %(ic_mean)s, %(ic_std)s, %(rank_ic_mean)s, %(rank_ic_std)s, %(icir)s, %(rank_icir)s, %(ic_positive_ratio)s,
    %(top_annual_return)s, %(top_excess_annual_return)s, %(top_sharpe)s,
    %(top_max_drawdown)s, %(top_excess_sharpe)s, %(benchmark_annual_return)s,
    %(group_return_monotonicity)s, %(turnover)s, %(ic_decay_half_life)s,
    %(ic_csz_mean)s, %(rank_ic_1d)s, %(rank_ic_5d)s, %(rank_ic_10d)s, %(rank_ic_20d)s,
    %(coverage)s, %(n_trading_days)s, %(source_task_id)s, %(calc_batch_id)s, %(calc_engine)s,
    %(factor_catalog_id)s
)
ON CONFLICT (factor_name, eval_window, data_start, data_end, calculated_at)
DO UPDATE SET
    ic_mean = EXCLUDED.ic_mean,
    ic_std = EXCLUDED.ic_std,
    rank_ic_mean = EXCLUDED.rank_ic_mean,
    rank_ic_std = EXCLUDED.rank_ic_std,
    icir = EXCLUDED.icir,
    rank_icir = EXCLUDED.rank_icir,
    ic_positive_ratio = EXCLUDED.ic_positive_ratio,
    top_annual_return = EXCLUDED.top_annual_return,
    top_excess_annual_return = EXCLUDED.top_excess_annual_return,
    top_sharpe = EXCLUDED.top_sharpe,
    top_max_drawdown = EXCLUDED.top_max_drawdown,
    top_excess_sharpe = EXCLUDED.top_excess_sharpe,
    benchmark_annual_return = EXCLUDED.benchmark_annual_return,
    group_return_monotonicity = EXCLUDED.group_return_monotonicity,
    turnover = EXCLUDED.turnover,
    ic_decay_half_life = EXCLUDED.ic_decay_half_life,
    ic_csz_mean = EXCLUDED.ic_csz_mean,
    rank_ic_1d = EXCLUDED.rank_ic_1d,
    rank_ic_5d = EXCLUDED.rank_ic_5d,
    rank_ic_10d = EXCLUDED.rank_ic_10d,
    rank_ic_20d = EXCLUDED.rank_ic_20d,
    coverage = EXCLUDED.coverage,
    n_trading_days = EXCLUDED.n_trading_days,
    source_task_id = EXCLUDED.source_task_id,
    factor_catalog_id = EXCLUDED.factor_catalog_id
"""


def _insert_metrics_batch(
    metrics_list: List[Dict[str, Any]],
    source_task_id: str,
) -> tuple[int, int]:
    """将指标列表批量写入数据库。返回 (inserted, skipped)。"""
    inserted = 0
    skipped = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 构建因子名映射（支持归一化匹配）
            factor_names = list({m["factor_name"] for m in metrics_list})
            catalog_map = _build_factor_catalog_map(cur, factor_names, source_task_id)

            for m in metrics_list:
                match = catalog_map.get(m["factor_name"])
                if match is None:
                    logger.warning(f"因子 {m['factor_name']} 未在 catalog 中找到，跳过指标写入")
                    skipped += 1
                    continue
                catalog_name, fc_id = match
                params = {
                    "factor_name": catalog_name,
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
                    "factor_catalog_id": fc_id,
                }
                cur.execute(_UPSERT_SQL, params)
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
    return inserted, skipped


_UPSERT_CALC_LOG_SQL = """
INSERT INTO aistock_factor_calc_log (
    calc_batch_id, source_task_id, factor_name, eval_window,
    status, error_message, n_trading_days, required_days,
    data_start, data_end, data_source, calc_engine, calculated_at,
    factor_catalog_id
) VALUES (
    %(calc_batch_id)s, %(source_task_id)s, %(factor_name)s, %(eval_window)s,
    %(status)s, %(error_message)s, %(n_trading_days)s, %(required_days)s,
    %(data_start)s, %(data_end)s, %(data_source)s, %(calc_engine)s, %(calculated_at)s,
    %(factor_catalog_id)s
)
ON CONFLICT (calc_batch_id, factor_name, eval_window)
DO UPDATE SET
    status = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    n_trading_days = EXCLUDED.n_trading_days,
    required_days = EXCLUDED.required_days,
    data_start = EXCLUDED.data_start,
    data_end = EXCLUDED.data_end,
    data_source = EXCLUDED.data_source,
    calculated_at = EXCLUDED.calculated_at,
    factor_catalog_id = EXCLUDED.factor_catalog_id
"""


def _insert_calc_log_batch(
    reports_list: List[Dict[str, Any]],
    source_task_id: str,
    calc_batch_id: str,
) -> int:
    """将因子计算报告批量写入 aistock_factor_calc_log。返回写入行数。"""
    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 构建因子名映射（支持归一化匹配）
            factor_names = list({r["factor_name"] for r in reports_list})
            catalog_map = _build_factor_catalog_map(cur, factor_names, source_task_id)

            for r in reports_list:
                match = catalog_map.get(r["factor_name"])
                if match is None:
                    logger.warning(f"因子 {r['factor_name']} 未在 catalog 中找到，跳过计算日志写入")
                    continue
                catalog_name, fc_id = match
                params = {
                    "calc_batch_id": calc_batch_id,
                    "source_task_id": source_task_id,
                    "factor_name": catalog_name,
                    "eval_window": r["eval_window"],
                    "status": r["status"],
                    "error_message": r.get("error_message"),
                    "n_trading_days": r.get("n_trading_days"),
                    "required_days": r.get("required_days"),
                    "data_start": r.get("data_start"),
                    "data_end": r.get("data_end"),
                    "data_source": r.get("data_source", "parquet"),
                    "calc_engine": r.get("calc_engine", "rdagent"),
                    "calculated_at": r.get("calculated_at"),
                    "factor_catalog_id": fc_id,
                }
                cur.execute(_UPSERT_CALC_LOG_SQL, params)
                inserted += 1
    return inserted


def sync_factor_metrics_for_task(task_id: str) -> MetricsSyncResult:
    """从 RD-Agent API 获取单个 task 的因子指标并入库。

    自动区分 SOTA 因子和 Loop 手动同步因子：
    - SOTA 因子：从 SOTA Loop 的 parquet 计算（现有逻辑）
    - Loop 同步因子：按 source_loop_tag 分组，从各自 Loop 的 parquet 计算
    """
    errors: List[str] = []
    total_inserted = 0
    total_skipped = 0
    total_factor_count = 0
    # 追踪 Step 1 已成功处理的因子（归一化名），Step 2 可跳过避免冗余 Loop API 调用
    sota_handled_norm: set = set()

    # ── 第一步：SOTA 因子指标（现有逻辑）──
    logger.info(f"[{task_id}] 开始获取 SOTA 因子指标...")
    try:
        resp = _rdagent_client.get_factor_metrics(task_id)
        if resp.get("success"):
            metrics_list = resp.get("metrics", [])
            if metrics_list:
                inserted, skipped = _insert_metrics_batch(metrics_list, task_id)
                total_inserted += inserted
                total_skipped += skipped
                total_factor_count += resp.get("factor_count", 0)
                logger.info(f"[{task_id}] SOTA 因子指标: {inserted} 插入, {skipped} 跳过")

                # 记录 SOTA parquet 中所有因子的归一化名（无论是否入库成功）
                for m in metrics_list:
                    sota_handled_norm.add(_normalize_factor_name(m["factor_name"]))

                # 计算日志
                factor_reports = resp.get("factor_reports", [])
                calc_batch_id = resp.get("calc_batch_id")
                if factor_reports and calc_batch_id:
                    try:
                        _insert_calc_log_batch(factor_reports, task_id, calc_batch_id)
                    except Exception as e:
                        logger.error(f"[{task_id}] SOTA 计算日志写入失败: {e}")
            else:
                logger.info(f"[{task_id}] SOTA 因子无指标数据")
        else:
            msg = resp.get("error", "未知错误")
            logger.warning(f"[{task_id}] SOTA 因子指标获取失败: {msg}")
            errors.append(f"SOTA: {msg}")
    except Exception as e:
        msg = f"SOTA API 调用失败: {e}"
        logger.error(f"[{task_id}] {msg}")
        errors.append(msg)

    # ── 第二步：Loop 因子指标（Step 1 未覆盖的因子） ──
    # 从 catalog 查询该 task 下所有有 loop_tag 的因子，按 Loop 分组
    # 包括：
    #   1. catalog_source='rdagent_loop_manual_sync'，loop_tag 格式 "loop_N_manual_sync"
    #   2. catalog_source='rdagent_task_sync'，loop_tag 格式为纯数字 "N"
    #      （SOTA 同步的因子若不在 SOTA parquet 中，需回退到对应 Loop 的 parquet 计算）
    # 排除 Step 1 已处理的因子，避免冗余 Loop API 调用
    loop_groups: Dict[int, List[str]] = {}  # loop_id -> [factor_names]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT factor_name, source_loop_tag, catalog_source
                    FROM aistock_factor_catalog
                    WHERE source_task_id = %s
                      AND source_loop_tag IS NOT NULL
                      AND catalog_source IN ('rdagent_loop_manual_sync', 'rdagent_task_sync')
                """, (task_id,))
                for row in cur.fetchall():
                    fname, tag, csource = row[0], row[1], row[2]

                    # 跳过 Step 1 已通过 SOTA parquet 处理的因子
                    if _normalize_factor_name(fname) in sota_handled_norm:
                        continue

                    lid: Optional[int] = None

                    # 格式 1: "loop_7_manual_sync" → loop_id=7
                    m = re.match(r"loop_(\d+)_manual_sync", tag or "")
                    if m:
                        lid = int(m.group(1))
                    else:
                        # 格式 2: 纯数字 "3" → loop_id=3
                        try:
                            lid = int(tag)
                        except (ValueError, TypeError):
                            pass

                    if lid is not None:
                        loop_groups.setdefault(lid, []).append(fname)
    except Exception as e:
        logger.error(f"[{task_id}] 查询 Loop 同步因子失败: {e}")
        errors.append(f"查询 Loop 因子: {e}")

    if loop_groups:
        logger.info(
            f"[{task_id}] 发现 {sum(len(v) for v in loop_groups.values())} 个 Loop 因子 "
            f"(分布在 {len(loop_groups)} 个 Loop)"
        )

        for loop_id, factor_names in loop_groups.items():
            try:
                loop_resp = _rdagent_client.get_loop_factor_metrics(task_id, loop_id)
                if not loop_resp.get("success"):
                    msg = loop_resp.get("error", "未知错误")
                    logger.warning(f"[{task_id}] Loop {loop_id} 因子指标获取失败: {msg}")
                    errors.append(f"Loop {loop_id}: {msg}")
                    continue

                loop_metrics = loop_resp.get("metrics", [])
                if not loop_metrics:
                    logger.info(f"[{task_id}] Loop {loop_id} 无指标数据")
                    continue

                # 只保留属于该 Loop 同步的因子（parquet 中也包含其他因子，需过滤）
                # 使用归一化匹配解决 catalog 名 vs parquet 列名的命名差异
                target_norm = {_normalize_factor_name(fn) for fn in factor_names}
                filtered_metrics = [
                    m for m in loop_metrics
                    if _normalize_factor_name(m["factor_name"]) in target_norm
                ]

                if filtered_metrics:
                    inserted, skipped = _insert_metrics_batch(filtered_metrics, task_id)
                    total_inserted += inserted
                    total_skipped += skipped
                    total_factor_count += len(set(m["factor_name"] for m in filtered_metrics))
                    logger.info(
                        f"[{task_id}] Loop {loop_id} 因子指标: "
                        f"{inserted} 插入, {skipped} 跳过 "
                        f"(过滤后 {len(filtered_metrics)}/{len(loop_metrics)} 条)"
                    )

                    # 计算日志
                    calc_batch_id = loop_resp.get("calc_batch_id")
                    factor_reports = loop_resp.get("factor_reports", [])
                    if factor_reports and calc_batch_id:
                        filtered_reports = [
                            r for r in factor_reports
                            if _normalize_factor_name(r["factor_name"]) in target_norm
                        ]
                        try:
                            _insert_calc_log_batch(filtered_reports, task_id, calc_batch_id)
                        except Exception as e:
                            logger.error(f"[{task_id}] Loop {loop_id} 计算日志写入失败: {e}")

            except Exception as e:
                msg = f"Loop {loop_id} API 调用失败: {e}"
                logger.error(f"[{task_id}] {msg}")
                errors.append(msg)

    logger.info(
        f"[{task_id}] 因子指标同步完成: "
        f"{total_factor_count} 个因子, {total_inserted} 插入, {total_skipped} 跳过"
        + (f", {len(errors)} 错误" if errors else "")
    )
    return MetricsSyncResult(
        ok=len(errors) == 0,
        task_id=task_id,
        factor_count=total_factor_count,
        metrics_inserted=total_inserted,
        metrics_skipped=total_skipped,
        errors=errors,
    )


def sync_factor_metrics_batch(task_ids: List[str]) -> List[MetricsSyncResult]:
    """批量同步多个 task 的因子指标。逐个调用以便精确追踪每个 task 的结果。"""
    results = []
    for tid in task_ids:
        r = sync_factor_metrics_for_task(tid)
        results.append(r)
    return results


async def sync_factor_metrics_batch_async(
    task_ids: List[str],
    concurrency: int = 4,
):
    """异步并发同步因子指标（AsyncGenerator，支持 SSE 流式推送进度）。

    按 Task 为单位并行调用 RDAgent 指标 API，Semaphore 限制并发度。

    Yields:
        {"type": "progress", "task_id": str, "ok": bool, "current": int, "total": int, ...}
        {"type": "error", "task_id": str, "error": str, "current": int, "total": int}
        {"type": "done", "total": int, "success": int, "failed": int, "total_inserted": int}
    """
    import asyncio

    semaphore = asyncio.Semaphore(concurrency)
    total = len(task_ids)
    completed = 0
    success_count = 0
    total_inserted = 0

    async def sync_one(task_id: str):
        async with semaphore:
            return await asyncio.to_thread(sync_factor_metrics_for_task, task_id)

    tasks = [sync_one(tid) for tid in task_ids]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        completed += 1
        if result.ok:
            success_count += 1
            total_inserted += result.metrics_inserted
            yield {
                "type": "progress",
                "task_id": result.task_id,
                "ok": True,
                "current": completed,
                "total": total,
                "factor_count": result.factor_count,
                "metrics_inserted": result.metrics_inserted,
                "metrics_skipped": result.metrics_skipped,
            }
        else:
            yield {
                "type": "error",
                "task_id": result.task_id,
                "ok": False,
                "current": completed,
                "total": total,
                "errors": result.errors,
            }

    yield {
        "type": "done",
        "total": total,
        "success": success_count,
        "failed": total - success_count,
        "total_inserted": total_inserted,
    }


def sync_all_factor_metrics_from_catalog() -> List[MetricsSyncResult]:
    """从 aistock_factor_catalog 中提取所有不重复的 source_task_id，
    批量计算并入库因子指标。用于一次性补齐历史数据。"""
    logger.info("从 aistock_factor_catalog 获取所有 source_task_id...")
    task_ids = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT source_task_id
                FROM aistock_factor_catalog
                WHERE source_task_id IS NOT NULL
                  AND source_task_id != ''
                ORDER BY source_task_id
            """)
            task_ids = [row[0] for row in cur.fetchall()]

    if not task_ids:
        logger.info("未找到任何 source_task_id，跳过")
        return []

    logger.info(f"找到 {len(task_ids)} 个 task，开始批量计算因子指标...")
    return sync_factor_metrics_batch(task_ids)
