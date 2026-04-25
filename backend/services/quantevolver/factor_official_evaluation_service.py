from __future__ import annotations

import gc
import logging
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn
from ...infra.wsl_qlib_runner import win_to_wsl_path
from .evaluation_universe_service import (
    EvaluationUniverseService,
    UNIVERSE_POLICY_VERSION,
)
from .factor_eligibility_service import FactorEligibilityService
from .factor_value_pipeline import FactorValuePipeline

logger = logging.getLogger("aistock.quantevolver.factor_official_evaluation")

CALC_ENGINE = "qe_eval_v2"
PIPELINE_VERSION = "qe_eval_v2"
CODE_SOURCE = "qe_code_path"
_DEFAULT_QLIB_BIN = Path(__file__).resolve().parents[3] / "qlib_bin" / "qlib_bin_20260311"
_DEFAULT_DISPATCH_NODE_ID = os.getenv("AISTOCK_DEFAULT_GPU_NODE_ID", "wsl2-5080")

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
    factor_catalog_id, snapshot_date
) VALUES (
    %(factor_name)s, %(calculated_at)s, %(data_start)s, %(data_end)s, %(eval_window)s,
    %(return_horizon)s, %(universe)s,
    %(ic_mean)s, %(ic_std)s, %(rank_ic_mean)s, %(rank_ic_std)s, %(icir)s, %(rank_icir)s, %(ic_positive_ratio)s,
    %(top_annual_return)s, %(top_excess_annual_return)s, %(top_sharpe)s,
    %(top_max_drawdown)s, %(top_excess_sharpe)s, %(benchmark_annual_return)s,
    %(group_return_monotonicity)s, %(turnover)s, %(ic_decay_half_life)s,
    %(ic_csz_mean)s, %(rank_ic_1d)s, %(rank_ic_5d)s, %(rank_ic_10d)s, %(rank_ic_20d)s,
    %(coverage)s, %(n_trading_days)s, %(source_task_id)s, %(calc_batch_id)s, %(calc_engine)s,
    %(factor_catalog_id)s, %(snapshot_date)s
)
ON CONFLICT (factor_name, eval_window, data_start, data_end, snapshot_date)
DO UPDATE SET
    calculated_at = EXCLUDED.calculated_at,
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
    calc_batch_id = EXCLUDED.calc_batch_id,
    calc_engine = EXCLUDED.calc_engine,
    factor_catalog_id = EXCLUDED.factor_catalog_id
"""


class FactorOfficialEvaluationService:
    """官方独立指标执行/读取服务。"""

    def __init__(self) -> None:
        self._eligibility_service = FactorEligibilityService()
        self._universe_service = EvaluationUniverseService()
        self._pipeline = FactorValuePipeline()
        self._dispatch_service = None

    def compute(
        self,
        factor_names: Optional[List[str]] = None,
        data_date: str = "",
        include_disabled: bool = False,
        max_workers: int = 4,
        timeout_per_factor: int = 600,
    ) -> Dict[str, Any]:
        try:
            return self._compute_via_dispatch(
                factor_names=factor_names,
                data_date=data_date,
                include_disabled=include_disabled,
                max_workers=max_workers,
                timeout_per_factor=timeout_per_factor,
            )
        except Exception as exc:
            logger.error("official evaluation compute 失败: %s", exc, exc_info=True)
            return {
                "success": False,
                "error": str(exc),
                "traceback": traceback.format_exc().splitlines()[-20:],
            }

    def _compute_via_dispatch(
        self,
        factor_names: Optional[List[str]],
        data_date: str,
        include_disabled: bool,
        max_workers: int,
        timeout_per_factor: int,
    ) -> Dict[str, Any]:
        import asyncio
        import time

        from ...services.dispatch_service import DispatchService

        if not data_date:
            raise ValueError("data_date 参数必填")

        requested = factor_names or []
        dispatch_service = self._dispatch_service or DispatchService()
        eligible = self._eligibility_service.list_eligible_factors(
            factor_names=factor_names,
            include_disabled=include_disabled,
        )
        eligible_names = [row["factor_name"] for row in eligible]
        skipped = sorted(set(requested) - set(eligible_names)) if requested else []

        if not eligible_names:
            return {
                "success": False,
                "error": "无满足 official evaluation 准入条件的因子",
                "requested_factors": requested,
                "eligible_factors": [],
                "skipped_factors": skipped,
            }

        payload = {
            "factor_names": eligible_names,
            "data_date": data_date,
            "include_disabled": include_disabled,
            "max_workers": max_workers,
            "timeout_per_factor": timeout_per_factor,
        }
        created = asyncio.run(dispatch_service.create_and_submit_task({
            "task_name": f"official_evaluation_{data_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "task_type": "official_evaluation",
            "node_id": _DEFAULT_DISPATCH_NODE_ID,
            "payload": payload,
        }))
        task_id = created["task_id"]

        deadline = time.time() + max(timeout_per_factor * max(len(eligible_names), 1), 600)
        last_task = created
        while time.time() < deadline:
            asyncio.run(dispatch_service.sync_running_tasks())
            last_task = dispatch_service.get_task(task_id) or last_task
            status = last_task.get("status")
            if status in {"success", "failed", "canceled"}:
                break
            time.sleep(2)
        else:
            raise TimeoutError(f"official evaluation dispatch task timeout: {task_id}")

        result_bundle = asyncio.run(dispatch_service.get_task_results(task_id))
        latest_result = result_bundle.get("latest_result") or {}
        latest_result.setdefault("requested_factors", requested or eligible_names)
        latest_result.setdefault("eligible_factors", eligible_names)
        latest_result.setdefault("skipped_factors", skipped)
        latest_result.setdefault("dispatch_task_id", task_id)
        latest_result.setdefault("remote_task_id", last_task.get("remote_task_id"))
        if last_task.get("status") == "success":
            latest_result.setdefault("success", True)
            return latest_result

        log_excerpt: list[str] = []
        try:
            log_file = dispatch_service.get_log_file_path(task_id)
            if log_file and log_file.exists():
                content = log_file.read_text(encoding="utf-8", errors="replace")
                if content.strip():
                    log_excerpt = content.splitlines()[-80:]
                else:
                    log_excerpt = ["[日志文件为空 — 节点可能未开始执行或启动即崩溃]"]
            else:
                log_excerpt = ["[日志文件不存在]"]
        except Exception as log_exc:
            logger.warning("读取 official evaluation dispatch 日志失败 (task=%s): %s", task_id, log_exc)

        # 构建有用的错误信息
        dispatch_error = last_task.get("error_message") or ""
        node_status = last_task.get("status", "unknown")
        error_parts = []
        if dispatch_error:
            error_parts.append(dispatch_error)
        # 从 latest_result 中提取 pipeline_summary 的逐因子错误
        pipeline_summary = latest_result.get("pipeline_summary") or {}
        factor_errors = pipeline_summary.get("factor_results", [])
        failed_factors = [f for f in factor_errors if f.get("error")]
        if failed_factors:
            sample = failed_factors[:3]
            error_parts.append(
                f"因子失败示例({len(failed_factors)}个): "
                + "; ".join(f"{f.get('name','?')}: {f.get('error','?')[:100]}" for f in sample)
            )
        if not error_parts:
            error_parts.append(f"dispatch task {node_status}, 无详细错误信息（检查节点日志）")

        latest_result.setdefault("success", False)
        latest_result.setdefault("dispatch_status", node_status)
        latest_result.setdefault("error", " | ".join(error_parts))
        if log_excerpt:
            latest_result.setdefault("logs", log_excerpt)
        return latest_result

    def _compute_local(
        self,
        factor_names: Optional[List[str]] = None,
        data_date: str = "",
        include_disabled: bool = False,
        max_workers: int = 4,
        timeout_per_factor: int = 600,
    ) -> Dict[str, Any]:
        if not data_date:
            raise ValueError("data_date 参数必填")

        requested = factor_names or []
        eligible = self._eligibility_service.list_eligible_factors(
            factor_names=factor_names,
            include_disabled=include_disabled,
        )
        eligible_names = [row["factor_name"] for row in eligible]
        skipped = sorted(set(requested) - set(eligible_names)) if requested else []

        if not eligible_names:
            return {
                "success": False,
                "error": "无满足 official evaluation 准入条件的因子",
                "requested_factors": requested,
                "eligible_factors": [],
                "skipped_factors": skipped,
            }

        as_of_date = f"{data_date[:4]}-{data_date[4:6]}-{data_date[6:8]}"
        universe_meta = self._universe_service.get_official_universe_with_meta(as_of_date=as_of_date)
        instruments = universe_meta["instruments"]

        # ── 准备指标计算共享上下文（在因子计算之前，只准备一次）──
        db_result = {"inserted": 0, "skipped": 0, "errors": []}
        metrics_error = None
        metrics_ctx = None
        calc_batch_id = str(__import__("uuid").uuid4())

        try:
            from rdagent.app.factor_metrics.engine import (
                prepare_shared_context,
                compute_single_factor_metrics,
            )
            qlib_bin_path = self._resolve_qlib_bin_path()
            # 从快照 meta 获取日期范围
            from .data_snapshot_manager import DataSnapshotManager
            snap_meta = DataSnapshotManager().load_meta(data_date)
            metrics_ctx = prepare_shared_context(
                qlib_bin_path=qlib_bin_path,
                start_date=snap_meta["start_date"],
                end_date=snap_meta["end_date"],
            )
            logger.info("指标计算共享上下文准备完成")
        except Exception as e:
            logger.error("指标计算共享上下文准备失败: %s", e, exc_info=True)
            metrics_error = f"指标计算初始化异常: {type(e).__name__}: {e}"
            db_result["errors"].append(metrics_error)

        # ── 回调函数：每个因子成功后立即计算指标 + 入库 ──
        # 缓存 factor_ids 避免每次查询全表
        _factor_ids_cache: Dict[str, int] = {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT factor_name, id FROM aistock_factor_catalog")
                    _factor_ids_cache = {row[0]: row[1] for row in cur.fetchall()}
        except Exception as e:
            logger.error("查询 factor_ids 失败: %s", e)
            if not metrics_error:
                metrics_error = f"查询 factor_ids 失败: {e}"
            db_result["errors"].append(f"factor_ids 查询失败: {e}")

        def _on_factor_success(factor_name: str, single_path: str, factor_df):
            if metrics_ctx is None:
                db_result["errors"].append(f"{factor_name}: 指标计算上下文未就绪")
                return
            try:
                result = compute_single_factor_metrics(factor_name, factor_df, metrics_ctx)
                # compute_single_factor_metrics 返回 {"metrics": {window: {...}}, ...}
                # _save_metrics 期望 {"metrics": [{"factor_name":..., "eval_window":..., ...}, ...]}
                # 需要转换格式
                metrics_by_window = result.get("metrics", {})
                if metrics_by_window:
                    flat_metrics = []
                    for window_name, window_metrics in metrics_by_window.items():
                        rec = dict(window_metrics)
                        rec["factor_name"] = factor_name
                        rec["eval_window"] = window_name
                        flat_metrics.append(rec)
                    save_result = self._save_metrics(
                        {"metrics": flat_metrics, "calc_batch_id": calc_batch_id},
                        snapshot_date=as_of_date,
                        factor_ids=_factor_ids_cache,
                    )
                    db_result["inserted"] += save_result.get("inserted", 0)
                    db_result["skipped"] += save_result.get("skipped", 0)
                    if save_result.get("errors"):
                        db_result["errors"].extend(save_result["errors"])
                    logger.info(f"因子 {factor_name} 指标已入库 (inserted={save_result.get('inserted', 0)})")
                    # 保存月频 IC 衰退序列（full 窗口）
                    full_metrics = metrics_by_window.get("full", {})
                    monthly_series = full_metrics.get("monthly_ic_series")
                    if monthly_series:
                        try:
                            self._save_monthly_ic(factor_name, as_of_date, monthly_series)
                        except Exception as mic_err:
                            logger.warning(f"因子 {factor_name} 月频IC入库失败: {mic_err}")
                            db_result["errors"].append(f"{factor_name}: 月频IC入库失败: {mic_err}")
                else:
                    logger.warning(f"因子 {factor_name} 指标计算返回空 metrics")
                    db_result["errors"].append(f"{factor_name}: 指标计算返回空")
            except Exception as e:
                logger.warning(f"因子 {factor_name} 指标计算/入库失败: {e}")
                db_result["errors"].append(f"{factor_name}: {e}")

        # ── 执行因子计算（每个因子成功后通过回调立即入库）──
        pipeline_result = self._pipeline.compute_factor_values(
            factor_names=eligible_names,
            instruments=instruments,
            data_date=data_date,
            max_workers=max_workers,
            timeout_per_factor=timeout_per_factor,
            save_parquet=True,
            on_factor_success=_on_factor_success,
        )

        # 释放指标计算上下文
        if metrics_ctx is not None:
            del metrics_ctx
            gc.collect()

        summary = pipeline_result.summary()
        success_count = pipeline_result.success
        failed_count = pipeline_result.failed

        # 构建错误信息（如果有失败因子）
        error_detail = None
        if failed_count > 0:
            factor_errors = summary.get("factor_results", [])
            failed_factors = [f for f in factor_errors if f.get("error")]
            if failed_factors:
                sample = failed_factors[:3]
                error_detail = (
                    f"{failed_count}/{failed_count + success_count}个因子失败, "
                    f"示例: " + "; ".join(
                        f"{f.get('name','?')}: {f.get('error','?')[:120]}" for f in sample
                    )
                )

        # 判断整体成功：必须有因子实际入库，且没有致命异常
        # inserted=0 + errors 非空 = 失败（不能静默报成功）
        has_db_errors = bool(db_result["errors"])
        overall_success = db_result["inserted"] > 0 and not metrics_error
        if success_count > 0 and db_result["inserted"] == 0:
            # 因子计算成功但入库全部失败 — 这是严重错误
            if not error_detail:
                error_detail = ""
            error_detail += f" | 因子计算成功 {success_count} 个但入库 0 条"
            if has_db_errors:
                error_detail += f", 入库错误: {'; '.join(db_result['errors'][:3])}"

        result = {
            "success": overall_success,
            "requested_factors": requested or eligible_names,
            "eligible_factors": eligible_names,
            "skipped_factors": skipped,
            "pipeline_version": PIPELINE_VERSION,
            "code_source": CODE_SOURCE,
            "universe_policy_version": UNIVERSE_POLICY_VERSION,
            "snapshot_date": as_of_date,
            "universe_count": universe_meta["count"],
            "pipeline_summary": summary,
            "db_result": db_result,
        }
        all_errors = []
        if error_detail:
            all_errors.append(error_detail)
        if metrics_error:
            all_errors.append(metrics_error)
        if all_errors:
            result["error"] = " | ".join(all_errors)

        return result

    def get_factor_metrics(
        self,
        factor_name: str,
        eval_window: Optional[str] = None,
        limit: int = 10,
    ) -> Dict[str, Any]:
        conditions = ["factor_name = %s", "calc_engine = %s"]
        params: List[Any] = [factor_name, CALC_ENGINE]
        if eval_window:
            conditions.append("eval_window = %s")
            params.append(eval_window)

        where = " AND ".join(conditions)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT factor_name, eval_window, data_start, data_end, calculated_at,
                           return_horizon, universe,
                           ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir,
                           ic_positive_ratio,
                           top_annual_return, top_excess_annual_return, top_sharpe,
                           top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
                           group_return_monotonicity, turnover, ic_decay_half_life,
                           ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
                           coverage, n_trading_days, source_task_id, calc_engine,
                           snapshot_date, calc_batch_id
                    FROM aistock_factor_metrics
                    WHERE {where}
                    ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC
                    LIMIT %s
                    """,
                    params + [limit],
                )
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        for row in rows:
            for key in ("data_start", "data_end", "calculated_at", "snapshot_date"):
                if row.get(key) is not None:
                    row[key] = str(row[key])
        return {"ok": True, "factor_name": factor_name, "metrics": rows, "total": len(rows)}

    def get_summary(self) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (factor_name)
                        factor_name, ic_mean, top_excess_sharpe, top_excess_annual_return,
                        rank_ic_mean, icir, calculated_at, snapshot_date, calc_batch_id
                    FROM aistock_factor_metrics
                    WHERE eval_window = 'full'
                      AND calc_engine = %s
                    ORDER BY factor_name, snapshot_date DESC NULLS LAST, calculated_at DESC
                    """,
                    (CALC_ENGINE,),
                )
                rows = cur.fetchall()

        summary = {}
        for row in rows:
            summary[row[0]] = {
                "ic_mean": row[1],
                "sharpe": row[2],
                "annual_return": row[3],
                "rank_ic_mean": row[4],
                "icir": row[5],
                "calculated_at": str(row[6]) if row[6] is not None else None,
                "snapshot_date": str(row[7]) if row[7] is not None else None,
                "calc_batch_id": row[8],
            }
        return {"ok": True, "summary": summary, "total": len(summary), "calc_engine": CALC_ENGINE}


    def _resolve_qlib_bin_path(self) -> Optional[Path]:
        env_path = (os.getenv("QLIB_BIN_PATH") or "").strip()
        if env_path:
            env_path_obj = Path(win_to_wsl_path(env_path) if os.name != "nt" else env_path)
            if env_path_obj.exists():
                return env_path_obj
            logger.warning(f"QLIB_BIN_PATH 不存在，忽略: {env_path_obj}")

        if _DEFAULT_QLIB_BIN.exists():
            return _DEFAULT_QLIB_BIN

        logger.warning(
            "官方独立指标未找到 qlib_bin 路径；请设置 QLIB_BIN_PATH。"
            f"默认路径也不存在: {_DEFAULT_QLIB_BIN}"
        )
        return None

    def _save_monthly_ic(self, factor_name: str, snapshot_date: str, monthly_series: list) -> int:
        """保存月频 IC 衰退序列到 aistock_factor_monthly_ic 表。

        每个因子只保留最新一次计算的月频 IC 序列：先删除该因子的所有旧行，再插入本次。
        """
        if not monthly_series:
            return 0
        inserted = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM aistock_factor_monthly_ic WHERE factor_name = %s",
                    (factor_name,),
                )
                deleted = cur.rowcount
                for rec in monthly_series:
                    month = rec.get("month")
                    if not month:
                        continue
                    cur.execute("""
                        INSERT INTO aistock_factor_monthly_ic
                            (factor_name, month_end, snapshot_date, ic_mean, rank_ic_mean, ic_std, ic_ewma_6m, n_days)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (factor_name, month_end, snapshot_date) DO UPDATE SET
                            ic_mean = EXCLUDED.ic_mean,
                            rank_ic_mean = EXCLUDED.rank_ic_mean,
                            ic_std = EXCLUDED.ic_std,
                            ic_ewma_6m = EXCLUDED.ic_ewma_6m,
                            n_days = EXCLUDED.n_days,
                            created_at = NOW()
                    """, (
                        factor_name, month, snapshot_date,
                        rec.get("ic_mean"), rec.get("rank_ic_mean"),
                        rec.get("ic_std"), rec.get("ic_ewma_6m"),
                        rec.get("n_days"),
                    ))
                    inserted += 1
                conn.commit()
        logger.info(f"因子 {factor_name} 月频IC入库: 删除旧 {deleted} 条, 插入新 {inserted} 条")
        return inserted

    def _save_metrics(self, engine_data: Dict[str, Any], snapshot_date: str, factor_ids: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        inserted = 0
        skipped = 0
        errors: List[str] = []
        calc_batch_id = engine_data.get("calc_batch_id", "")
        calculated_at = datetime.now(timezone.utc).isoformat()

        with get_conn() as conn:
            with conn.cursor() as cur:
                # factor_ids 缓存：外部传入则复用，否则查询一次
                if factor_ids is None:
                    cur.execute("SELECT factor_name, id FROM aistock_factor_catalog")
                    factor_ids = {row[0]: row[1] for row in cur.fetchall()}

                # 收集本批写入的因子名：先一次性删除所有旧行（跨所有 snapshot_date/data_start/data_end/eval_window），
                # 再插入当前 snapshot 的新行。确保每个因子只保留最新一次完整计算的 5 个 window 记录。
                batch_factor_names = sorted({
                    rec.get("factor_name")
                    for rec in engine_data.get("metrics", [])
                    if isinstance(rec, dict) and rec.get("factor_name")
                })
                if batch_factor_names:
                    cur.execute(
                        "DELETE FROM aistock_factor_metrics WHERE factor_name = ANY(%s)",
                        (batch_factor_names,),
                    )
                    logger.info(
                        "official evaluation _save_metrics: 清理 %s 个因子的旧行, 删除 %s 条",
                        len(batch_factor_names),
                        cur.rowcount,
                    )

                grouped_full_metrics: Dict[str, Dict[str, Any]] = {}
                for rec in engine_data.get("metrics", []):
                    if not isinstance(rec, dict):
                        errors.append(f"metrics 记录格式错误: 期望 dict, 实际 {type(rec).__name__}: {str(rec)[:100]}")
                        skipped += 1
                        continue
                    factor_name = rec.get("factor_name")
                    if not factor_name:
                        errors.append(f"metrics 记录缺少 factor_name: {str(rec)[:100]}")
                        skipped += 1
                        continue
                    catalog_id = factor_ids.get(factor_name)
                    if catalog_id is None:
                        errors.append(f"{factor_name}: factor_catalog_id not found")
                        skipped += 1
                        continue

                    params = {
                        "factor_name": factor_name,
                        "calculated_at": calculated_at,
                        "data_start": rec.get("data_start"),
                        "data_end": rec.get("data_end"),
                        "eval_window": rec.get("eval_window"),
                        "return_horizon": rec.get("return_horizon", "T2T1"),
                        "universe": rec.get("universe", "official_v1"),
                        "ic_mean": rec.get("ic_mean"),
                        "ic_std": rec.get("ic_std"),
                        "rank_ic_mean": rec.get("rank_ic_mean"),
                        "rank_ic_std": rec.get("rank_ic_std"),
                        "icir": rec.get("icir"),
                        "rank_icir": rec.get("rank_icir"),
                        "ic_positive_ratio": rec.get("ic_positive_ratio"),
                        "top_annual_return": rec.get("top_annual_return"),
                        "top_excess_annual_return": rec.get("top_excess_annual_return"),
                        "top_sharpe": rec.get("top_sharpe"),
                        "top_max_drawdown": rec.get("top_max_drawdown"),
                        "top_excess_sharpe": rec.get("top_excess_sharpe"),
                        "benchmark_annual_return": rec.get("benchmark_annual_return"),
                        "group_return_monotonicity": rec.get("group_return_monotonicity"),
                        "turnover": rec.get("turnover"),
                        "ic_decay_half_life": rec.get("ic_decay_half_life"),
                        "ic_csz_mean": rec.get("ic_csz_mean"),
                        "rank_ic_1d": rec.get("rank_ic_1d"),
                        "rank_ic_5d": rec.get("rank_ic_5d"),
                        "rank_ic_10d": rec.get("rank_ic_10d"),
                        "rank_ic_20d": rec.get("rank_ic_20d"),
                        "coverage": rec.get("coverage"),
                        "n_trading_days": rec.get("n_trading_days"),
                        "source_task_id": None,
                        "calc_batch_id": calc_batch_id,
                        "calc_engine": CALC_ENGINE,
                        "factor_catalog_id": catalog_id,
                        "snapshot_date": snapshot_date,
                    }
                    cur.execute(_UPSERT_SQL, params)
                    inserted += 1
                    if rec.get("eval_window") == "full":
                        grouped_full_metrics[factor_name] = rec

                for factor_name, full_m in grouped_full_metrics.items():
                    cur.execute(
                        """
                        UPDATE aistock_factor_catalog
                        SET ic = COALESCE(%s, ic),
                            sharpe = COALESCE(%s, sharpe),
                            annualized_return = COALESCE(%s, annualized_return)
                        WHERE factor_name = %s
                        """,
                        (
                            full_m.get("ic_mean"),
                            full_m.get("top_sharpe"),
                            full_m.get("top_annual_return"),
                            factor_name,
                        ),
                    )

        return {
            "inserted": inserted,
            "skipped": skipped,
            "errors": errors,
            "calc_engine": CALC_ENGINE,
        }
