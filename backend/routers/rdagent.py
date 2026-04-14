from __future__ import annotations

import os
import json
import logging
import asyncio
import contextvars
import queue
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from ..db.pg_pool import get_conn
from ..services.rdagent_registry_service import RDRegistryReader
from ..services.rdagent_signals_service import (
    load_signals_for_date,
    load_signals_overview,
    load_symbol_series,
)

from ..services.rdagent_selection_service import build_loop_selection

from ..services.rdagent_task_sync_service import rdagent_task_sync_service  # reload trigger
from ..services.rdagent_candidate_service import get_candidate_service

from ..inference_engine import InferenceEngine

router = APIRouter(prefix="/rdagent", tags=["rdagent"])

# 初始化推理引擎
inference_engine = InferenceEngine()

class InferenceRequest(BaseModel):
    version_tag: str = Field(..., description="版本标签，如 'v1'")
    trade_date: Optional[str] = Field(None, description="推理日期 YYYY-MM-DD，默认当日")

class BatchInferenceRequest(BaseModel):
    version_tag: str = Field("latest", description="版本标签")
    trade_date: Optional[str] = Field(None, description="推理日期")

class TaskSelectionRequest(BaseModel):
    trade_date: Optional[str] = Field(None, description="推理日期 YYYY-MM-DD，默认当日")
    cutoff_date: Optional[str] = Field(None, description="数据截止日期 YYYY-MM-DD；若设置，则推理取数不得晚于该日期")
    top_k: int = Field(50, ge=1, le=500, description="返回候选数量，默认 50")
    loop_id: Optional[int] = Field(None, description="可选：指定 loop_id；不传则从 task manifest 推导")


class TaskSyncRequest(BaseModel):
    task_ids: List[str] = Field(..., description="要同步的 task_id 列表")
    operator: str = Field("ui", description="操作者标识（UI/脚本等）")
    mode: str = Field("log_only", description="同步模式：log_only / api / auto")
    node_id: Optional[str] = Field(None, description="指定同步的节点 ID（None 使用默认节点）")


class TaskSyncAllRequest(BaseModel):
    operator: str = Field("ui:sync_all", description="操作者标识（UI/脚本等）")
    limit: int = Field(20000, ge=1, le=20000, description="最多扫描的 task 数量")
    force: bool = Field(False, description="是否强制重跑已 success 的 task")


logger = logging.getLogger("aistock.rdagent_router")

_selection_stream_reqid: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("selection_stream_reqid", default=None)


class _SelectionStreamHandler(logging.Handler):
    def __init__(self, q: "queue.Queue[str]"):
        super().__init__()
        self._q = q

    def emit(self, record: logging.LogRecord) -> None:
        reqid = _selection_stream_reqid.get()
        if not reqid:
            return
        if getattr(record, "_selection_reqid", None) not in (None, reqid):
            return
        try:
            msg = self.format(record)
        except Exception:
            try:
                msg = str(record.getMessage())
            except Exception:
                return
        try:
            self._q.put_nowait(msg)
        except Exception:
            return


def _selection_sse_format(event: str, data: str) -> str:
    data = (data or "").replace("\r", "")
    lines = data.split("\n")
    out = [f"event: {event}"]
    for ln in lines:
        out.append(f"data: {ln}")
    out.append("")
    return "\n".join(out) + "\n"


@router.post("/selection-center/inference", summary="批量触发选股中心策略推理")
def trigger_batch_inference(req: BatchInferenceRequest) -> Dict[str, Any]:
    """REQ-UI-P3-020: 批量触发选股中心已勾选策略的推理任务。"""
    # 1. 获取选股中心已勾选的策略
    sql = "SELECT strategy_id FROM aistock_strategy_catalog WHERE in_selection_center = TRUE"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                strategy_ids = [r[0] for r in cur.fetchall()]
        
        if not strategy_ids:
            return {"ok": True, "message": "选股中心无勾选策略", "count": 0}

        t_date = datetime.now()
        if req.trade_date:
            t_date = datetime.strptime(req.trade_date, "%Y-%m-%d")

        results = []
        # 2. 循环触发推理 (生产环境建议使用线程池或异步任务队列，此处保持简单同步调用)
        for sid in strategy_ids:
            try:
                scores = inference_engine.run_inference(
                    strategy_id=sid,
                    version_tag=req.version_tag,
                    trade_date=t_date
                )
                results.append({"strategy_id": sid, "status": "success", "count": len(scores)})
            except Exception as e:
                logger.error(f"策略 {sid} 批量推理失败: {e}")
                results.append({"strategy_id": sid, "status": "failed", "error": str(e)})

        return {
            "ok": True,
            "total": len(strategy_ids),
            "results": results,
            "trade_date": t_date.strftime("%Y-%m-%d")
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/tasks/latest", summary="获取 RD-Agent 最新 task 列表（供 UI 增量同步前预览）")
def list_latest_tasks(limit: int = Query(20, ge=1, le=10000, description="返回数量，默认 20")) -> Dict[str, Any]:
    return rdagent_task_sync_service.list_latest_tasks(limit=limit)


@router.get("/tasks/sync-candidates", summary="获取 task 同步候选（latest+summary+本地同步状态合并，支持翻页）")
def list_task_sync_candidates(
    limit: int = Query(20, ge=1, le=10000, description="每页数量，默认 20"),
    offset: int = Query(0, ge=0, description="偏移量，用于翻页"),
    node_id: Optional[str] = Query(None, description="按节点过滤"),
) -> Dict[str, Any]:
    return rdagent_task_sync_service.list_sync_candidates(limit=limit, offset=offset, node_id=node_id)


@router.get("/tasks/{task_id}/summary", summary="获取 RD-Agent 单 task 概要信息")
def get_task_summary(task_id: str) -> Dict[str, Any]:
    return rdagent_task_sync_service.get_task_summary(task_id=task_id)


@router.get("/tasks/{task_id}/loops", summary="获取 Task 所有 LOOP 的详细信息")
def get_task_loops(task_id: str) -> Dict[str, Any]:
    """获取指定task的所有LOOP详细信息，包括任务类型、回测结果、SOTA因子等。"""
    return rdagent_task_sync_service.get_task_loops(task_id=task_id)


@router.post("/tasks/sync", summary="同步选中的 task 资产到 AIstock（落盘 task 目录/manifest/生成 factor_entry.py/落库）")
def sync_tasks(req: TaskSyncRequest) -> Dict[str, Any]:
    task_ids = [str(x).strip() for x in (req.task_ids or []) if str(x).strip()]
    if not task_ids:
        raise HTTPException(status_code=422, detail="task_ids 不能为空")

    mode = str(req.mode or "").strip().lower() or "log_only"

    results: List[Dict[str, Any]] = []
    ok_cnt = 0
    for tid in task_ids:
        if mode == "log_only":
            r = rdagent_task_sync_service.sync_task_from_log(task_id=tid, operator=req.operator, node_id=req.node_id)
        elif mode == "api":
            r = rdagent_task_sync_service.sync_task(task_id=tid, operator=req.operator, node_id=req.node_id)
        else:
            r = rdagent_task_sync_service.sync_task(task_id=tid, operator=req.operator, node_id=req.node_id)
        rr = {
            "task_id": r.task_id,
            "ok": bool(r.ok),
            "sync_status": r.sync_status,
            "task_dir": r.task_dir,
            "manifest_path": r.manifest_path,
            "error": r.error,
            "diagnostics": r.diagnostics,
        }
        if rr["ok"]:
            ok_cnt += 1
        results.append(rr)

    return {"ok": True, "total": len(task_ids), "success": ok_cnt, "results": results}


@router.post("/tasks/sync-stream", summary="同步选中 task（SSE 流式进度）")
async def sync_tasks_stream(req: TaskSyncRequest):
    """SSE 流式同步：实时推送每个 task 的同步日志和进度事件。"""
    task_ids = [str(x).strip() for x in (req.task_ids or []) if str(x).strip()]
    if not task_ids:
        raise HTTPException(status_code=422, detail="task_ids 不能为空")

    mode = str(req.mode or "").strip().lower() or "log_only"
    total = len(task_ids)

    q: "queue.Queue[str]" = queue.Queue()
    done = threading.Event()
    results_holder: List[Dict[str, Any]] = []

    # 复用 _SelectionStreamHandler 捕获同步过程中的所有日志
    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%H:%M:%S")
    handler = _SelectionStreamHandler(q)
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)

    sync_loggers = [
        logging.getLogger("aistock.rdagent_task_sync_service"),
        logging.getLogger("aistock.rdagent_model_catalog_sync"),
        logging.getLogger("aistock.factor_catalog_sync"),
    ]

    def _run_sync():
        # 给 handler 注入一个固定 reqid 以通过过滤
        reqid = str(uuid.uuid4())
        token = _selection_stream_reqid.set(reqid)
        for lg in sync_loggers:
            try:
                lg.addHandler(handler)
                lg.setLevel(logging.INFO)
            except Exception:
                pass
        try:
            ok_cnt = 0
            for i, tid in enumerate(task_ids, 1):
                q.put_nowait(json.dumps({
                    "_event": "task_begin",
                    "current": i, "total": total, "task_id": tid,
                }, ensure_ascii=False))

                try:
                    if mode == "log_only":
                        r = rdagent_task_sync_service.sync_task_from_log(task_id=tid, operator=req.operator, node_id=req.node_id)
                    else:
                        r = rdagent_task_sync_service.sync_task(task_id=tid, operator=req.operator, node_id=req.node_id)
                    rr = {
                        "task_id": r.task_id,
                        "ok": bool(r.ok),
                        "sync_status": r.sync_status,
                        "error": r.error,
                        "diagnostics": r.diagnostics,
                    }
                except Exception as exc:
                    rr = {"task_id": tid, "ok": False, "sync_status": "failed", "error": str(exc)}

                if rr["ok"]:
                    ok_cnt += 1
                results_holder.append(rr)

                q.put_nowait(json.dumps({
                    "_event": "task_done",
                    "current": i, "total": total,
                    "task_id": tid, "ok": rr["ok"],
                    "sync_status": rr.get("sync_status"),
                    "error": rr.get("error"),
                }, ensure_ascii=False))

            q.put_nowait(json.dumps({
                "_event": "done",
                "total": total, "success": ok_cnt,
                "failed": total - ok_cnt,
            }, ensure_ascii=False))
        finally:
            for lg in sync_loggers:
                try:
                    lg.removeHandler(handler)
                except Exception:
                    pass
            _selection_stream_reqid.reset(token)
            done.set()

    # 在后台线程中执行同步
    sync_thread = threading.Thread(target=_run_sync, daemon=True)
    sync_thread.start()

    async def _gen():
        yield _selection_sse_format("start", json.dumps({
            "total": total, "task_ids": task_ids,
        }, ensure_ascii=False))

        last_ping = time.time()
        while not done.is_set() or not q.empty():
            try:
                line = q.get_nowait()
                # 区分结构化事件和普通日志
                try:
                    parsed = json.loads(line)
                    evt = parsed.pop("_event", None)
                    if evt:
                        yield _selection_sse_format(evt, json.dumps(parsed, ensure_ascii=False))
                        continue
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
                yield _selection_sse_format("log", line)
                continue
            except queue.Empty:
                pass
            now = time.time()
            if now - last_ping >= 10:
                last_ping = now
                yield _selection_sse_format("ping", "")
            await asyncio.sleep(0.15)

        # 发送最终完整结果
        yield _selection_sse_format("result", json.dumps({
            "total": total,
            "success": sum(1 for r in results_holder if r.get("ok")),
            "failed": sum(1 for r in results_holder if not r.get("ok")),
            "results": results_holder,
        }, ensure_ascii=False))

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/tasks/sync-log-only", summary="log-only 模式同步：直接从 RD-Agent log 落盘资产（包含 factor_order.json）")
def sync_tasks_log_only(req: TaskSyncRequest) -> Dict[str, Any]:
    task_ids = [str(x).strip() for x in (req.task_ids or []) if str(x).strip()]
    if not task_ids:
        raise HTTPException(status_code=422, detail="task_ids 不能为空")

    results: List[Dict[str, Any]] = []
    ok_cnt = 0
    for tid in task_ids:
        r = rdagent_task_sync_service.sync_task_from_log(task_id=tid, operator=req.operator, node_id=req.node_id)
        rr = {
            "task_id": r.task_id,
            "ok": bool(r.ok),
            "sync_status": r.sync_status,
            "task_dir": r.task_dir,
            "manifest_path": r.manifest_path,
            "error": r.error,
            "diagnostics": r.diagnostics,
        }
        if rr["ok"]:
            ok_cnt += 1
        results.append(rr)

    return {"ok": True, "total": len(task_ids), "success": ok_cnt, "results": results}


@router.post("/tasks/sync-all", summary="全量初始化同步：遍历 RD-Agent log 下所有 task 并按 log-only 落盘资产")
def sync_all_tasks(req: TaskSyncAllRequest) -> Dict[str, Any]:
    return rdagent_task_sync_service.sync_all_tasks_from_log(
        operator=req.operator,
        limit=req.limit,
        force=bool(req.force),
    )


@router.post("/tasks/{task_id}/enable_for_selection", summary="将 task 加入 Task 选股")
def enable_task_for_selection(task_id: str) -> Dict[str, Any]:
    return rdagent_task_sync_service.enable_for_selection(task_id=task_id, operator="ui")


@router.post("/tasks/{task_id}/disable_for_selection", summary="将 task 从 Task 选股移除")
def disable_task_for_selection(task_id: str) -> Dict[str, Any]:
    return rdagent_task_sync_service.disable_for_selection(task_id=task_id, operator="ui")


@router.get("/tasks/local", summary="本地 task 列表（来自 aistock_task_catalog）")
def list_local_tasks(
    limit: int = Query(200, ge=1, le=1000, description="返回数量，默认 200"),
    offset: int = Query(0, ge=0, description="偏移量"),
) -> Dict[str, Any]:
    return rdagent_task_sync_service.list_local_tasks(limit=limit, offset=offset)


@router.get("/tasks/local/audit_assets", summary="审计本地 task 资产落盘情况（manifest/factor_entry/model_weight）")
def audit_local_task_assets(
    limit: int = Query(5000, ge=1, le=20000, description="最多扫描的 task_dir 数量，默认 5000"),
) -> Dict[str, Any]:
    return rdagent_task_sync_service.audit_local_task_assets(limit=limit)


@router.get("/tasks/local/{task_id}", summary="本地 task 详情（来自 aistock_task_catalog）")
def get_local_task(task_id: str) -> Dict[str, Any]:
    return rdagent_task_sync_service.get_local_task(task_id=task_id)


@router.get("/tasks/local/{task_id}/manifest", summary="读取本地 task manifest.json（AIstock 同步生成）")
def get_local_task_manifest(task_id: str) -> Dict[str, Any]:
    return rdagent_task_sync_service.get_local_manifest_text(task_id=task_id)


@router.get("/tasks/local-with-metrics", summary="Task 列表 + SOTA 聚合指标")
def list_local_tasks_with_metrics(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("best_ic", description="排序字段"),
    sort_order: str = Query("desc", description="asc/desc"),
) -> Dict[str, Any]:
    """返回 aistock_task_catalog 中的 task 列表，附带从 factor/model catalog 聚合的 SOTA 指标。"""
    from psycopg2.extras import RealDictCursor

    allowed_sort = {"best_ic", "best_sharpe", "best_ann_return", "best_max_drawdown", "sota_factors", "sota_models", "task_id"}
    sort_col = sort_by if sort_by in allowed_sort else "best_ic"
    order = "ASC" if sort_order.lower() == "asc" else "DESC"

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT t.task_id,
                       t.sync_status,
                       t.is_enabled_for_selection,
                       t.sota_factors_count,
                       t.sota_models_count,
                       t.loops_count,
                       t.created_at_utc,
                       agg.sota_factors,
                       agg.sota_models,
                       agg.best_ic,
                       agg.best_sharpe,
                       agg.best_ann_return,
                       agg.best_max_drawdown,
                       agg.best_model_ic,
                       agg.best_model_sharpe,
                       agg.best_model_ann_return,
                       agg.best_model_max_drawdown
                FROM aistock_task_catalog t
                LEFT JOIN LATERAL (
                    SELECT
                        COALESCE(f_agg.cnt, 0) AS sota_factors,
                        COALESCE(m_agg.cnt, 0) AS sota_models,
                        GREATEST(f_agg.best_ic, m_agg.best_ic) AS best_ic,
                        GREATEST(f_agg.best_sharpe, m_agg.best_sharpe) AS best_sharpe,
                        GREATEST(f_agg.best_ann_return, m_agg.best_ann_return) AS best_ann_return,
                        LEAST(f_agg.best_max_drawdown, m_agg.best_max_drawdown) AS best_max_drawdown,
                        m_agg.best_ic AS best_model_ic,
                        m_agg.best_sharpe AS best_model_sharpe,
                        m_agg.best_ann_return AS best_model_ann_return,
                        m_agg.best_max_drawdown AS best_model_max_drawdown
                    FROM
                        (SELECT COUNT(*) AS cnt, MAX(ic) AS best_ic, MAX(sharpe) AS best_sharpe,
                                MAX(annualized_return) AS best_ann_return, MAX(max_drawdown) AS best_max_drawdown
                         FROM aistock_factor_catalog
                         WHERE source_task_id = t.task_id AND is_sota_factor = TRUE) f_agg,
                        (SELECT COUNT(*) AS cnt, MAX(ic) AS best_ic, MAX(sharpe) AS best_sharpe,
                                MAX(annualized_return) AS best_ann_return, MAX(max_drawdown) AS best_max_drawdown
                         FROM aistock_model_catalog
                         WHERE task_run_id = t.task_id AND is_sota = TRUE) m_agg
                ) agg ON TRUE
                ORDER BY agg.{sort_col} {order} NULLS LAST
                LIMIT %s OFFSET %s
            """, (limit, offset))
            items = [dict(r) for r in cur.fetchall()]

            cur.execute("SELECT COUNT(*) FROM aistock_task_catalog")
            total = cur.fetchone()["count"]

    return {"ok": True, "items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/tasks/local/{task_id}/sota-details", summary="Task 的 SOTA 因子和模型详情")
def get_task_sota_details(task_id: str) -> Dict[str, Any]:
    """返回指定 task 的 SOTA 因子列表和 SOTA 模型列表。"""
    from psycopg2.extras import RealDictCursor

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # SOTA factors
            cur.execute("""
                SELECT factor_name, source, ic, sharpe, annualized_return, max_drawdown,
                       icir, factor_type, data_source, description_cn,
                       source_loop_tag, code_text, expression
                FROM aistock_factor_catalog
                WHERE source_task_id = %s AND is_sota_factor = TRUE
                ORDER BY ic DESC NULLS LAST
            """, (task_id,))
            factors = [dict(r) for r in cur.fetchall()]

            # SOTA models
            cur.execute("""
                SELECT model_name, model_type, ic, sharpe, annualized_return, max_drawdown,
                       information_ratio, loop_id, model_grade, grade_reason,
                       best_epoch, total_epochs, convergence_ratio, overfit_ratio,
                       training_failed, train_loss_final, val_loss_final,
                       hypothesis_text, feedback_decision, feedback_reason,
                       code_text
                FROM aistock_model_catalog
                WHERE task_run_id = %s AND is_sota = TRUE
                ORDER BY ic DESC NULLS LAST
            """, (task_id,))
            models = [dict(r) for r in cur.fetchall()]

            # Task basic info
            cur.execute("""
                SELECT task_id, sync_status, is_enabled_for_selection,
                       sota_factors_count, sota_models_count, loops_count,
                       created_at_utc, task_dir, manifest_path
                FROM aistock_task_catalog WHERE task_id = %s
            """, (task_id,))
            task_info = cur.fetchone()

    return {
        "ok": True,
        "task_id": task_id,
        "task_info": dict(task_info) if task_info else None,
        "sota_factors": factors,
        "sota_models": models,
    }


def _try_parse_json_text(txt: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(txt)
    except Exception:
        return None


def _first_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _derive_task_run_and_loop_from_manifest(task_manifest: Dict[str, Any]) -> Dict[str, Any]:
    diags: Dict[str, Any] = {"candidates": {}, "chosen": {}, "notes": []}

    # AIstock manifest 会包含 source_task_manifest；同时也兼容直接使用 RD-Agent manifest
    src = task_manifest.get("source_task_manifest") if isinstance(task_manifest.get("source_task_manifest"), dict) else task_manifest

    cand_task_run_id = src.get("task_run_id") or src.get("taskRunId")
    if not cand_task_run_id and isinstance(src.get("source"), dict):
        cand_task_run_id = (src.get("source") or {}).get("task_run_id")
    if cand_task_run_id:
        diags["candidates"]["task_run_id"] = cand_task_run_id

    # 常见 loop 字段候选
    loop_keys = ["loop_id", "loopId", "best_loop_id", "bestLoopId", "selected_loop_id", "selectedLoopId"]
    cand_loop_id: Optional[int] = None
    for k in loop_keys:
        cand_loop_id = _first_int(src.get(k))
        if cand_loop_id is not None:
            diags["candidates"]["loop_id"] = {"key": k, "value": cand_loop_id}
            break

    # 兼容 loops 列表结构
    if cand_loop_id is None and isinstance(src.get("loops"), list):
        loops = [x for x in (src.get("loops") or []) if isinstance(x, dict)]
        diags["candidates"]["loops_count"] = len(loops)
        # 优先 is_best / best / selected 标记
        for flag_key in ["is_best", "best", "selected", "is_selected", "isSelected"]:
            for it in loops:
                if bool(it.get(flag_key)):
                    cand_loop_id = _first_int(it.get("loop_id") or it.get("loopId") or it.get("id"))
                    if cand_loop_id is not None:
                        diags["notes"].append(f"loop_id 由 loops[].{flag_key} 推导")
                        break
            if cand_loop_id is not None:
                break
        # 若仍未找到，取第一个可解析 loop_id
        if cand_loop_id is None:
            for it in loops:
                cand_loop_id = _first_int(it.get("loop_id") or it.get("loopId") or it.get("id"))
                if cand_loop_id is not None:
                    diags["notes"].append("loop_id 由 loops[0] 可解析项兜底")
                    break

    diags["chosen"] = {"task_run_id": cand_task_run_id, "loop_id": cand_loop_id}
    return {"task_run_id": cand_task_run_id, "loop_id": cand_loop_id, "diagnostics": diags}


def _get_local_task_assets_status(*, manifest_path: Optional[str], manifest_obj: Dict[str, Any]) -> Dict[str, Any]:
    mp = Path(str(manifest_path)).resolve() if manifest_path else None
    task_dir = mp.parent if mp and mp.exists() else None

    primary_assets = manifest_obj.get("primary_assets") if isinstance(manifest_obj.get("primary_assets"), dict) else {}
    factor_rel = primary_assets.get("factor_entry_relpath")
    model_rel = primary_assets.get("model_weight_relpath")

    factor_abs = (task_dir / str(factor_rel)).resolve() if task_dir and factor_rel else None
    model_abs = (task_dir / str(model_rel)).resolve() if task_dir and model_rel else None

    return {
        "task_dir": str(task_dir) if task_dir else None,
        "primary_assets": {"factor_entry_relpath": factor_rel, "model_weight_relpath": model_rel},
        "factor_entry": {
            "path": str(factor_abs) if factor_abs else None,
            "exists": bool(factor_abs and factor_abs.exists() and factor_abs.is_file()),
        },
        "model_weight": {
            "path": str(model_abs) if model_abs else None,
            "exists": bool(model_abs and model_abs.exists() and model_abs.is_file()),
        },
    }


def _task_only_selection(*, task_id: str, manifest_path: Optional[str], manifest_obj: Dict[str, Any], req: TaskSelectionRequest) -> Dict[str, Any]:
    derived = _derive_task_run_and_loop_from_manifest(manifest_obj)
    task_run_id = derived.get("task_run_id")
    loop_id = derived.get("loop_id")

    assets_status = _get_local_task_assets_status(manifest_path=manifest_path, manifest_obj=manifest_obj)
    if not bool(assets_status.get("factor_entry", {}).get("exists")):
        raise HTTPException(status_code=422, detail="task_factor_entry_missing")
    if not bool(assets_status.get("model_weight", {}).get("exists")):
        raise HTTPException(status_code=422, detail="task_model_weight_missing")

    task_dir = assets_status.get("task_dir")
    if not task_dir:
        raise HTTPException(status_code=422, detail="task_dir_missing")

    payload = inference_engine.run_task_inference(
        task_id=task_id,
        task_dir=str(task_dir),
        factor_entry_path=assets_status["factor_entry"]["path"],
        model_weight_path=assets_status["model_weight"]["path"],
        task_run_id=task_run_id,
        loop_id=loop_id,
        trade_date=req.trade_date,
        cutoff_date=req.cutoff_date,
        top_k=req.top_k,
    )
    return {"ok": True, "task_id": task_id, "mode": "task_only", "derived": derived, **payload}


@router.post("/tasks/{task_id}/selection", summary="基于 task 一键选股（从 task manifest 推导 loop 并复用 build_loop_selection）")
def trigger_task_selection(task_id: str, req: TaskSelectionRequest) -> Dict[str, Any]:
    tid = str(task_id).strip()
    if not tid:
        raise HTTPException(status_code=422, detail="task_id 不能为空")

    # 1) 读取本地 manifest
    mf = rdagent_task_sync_service.get_local_manifest_text(task_id=tid)
    if not mf.get("ok"):
        raise HTTPException(status_code=404, detail=mf.get("error") or "manifest_not_found")
    manifest_obj = _try_parse_json_text(mf.get("content") or "")
    if not isinstance(manifest_obj, dict):
        raise HTTPException(status_code=422, detail="manifest.json 解析失败")

    try:
        return _task_only_selection(
            task_id=tid,
            manifest_path=mf.get("manifest_path"),
            manifest_obj=manifest_obj,
            req=req,
        )
    except Exception as exc:
        msg = str(exc)
        logging.getLogger(__name__).error(f"Task 选股失败: {msg}", exc_info=True)
        known_asset_issue = (
            "parquet 回放型" in msg
            or "Qlib handler" in msg
            or "config_file" in msg
            or "data_handler_config" in msg
            or "训练" in msg and "特征" in msg and "不一致" in msg
            or "训练特征存在缺失值" in msg
            or "nan_cols_sample" in msg
            or "Column_" in msg
            or "关键数据未入库" in msg
            or "fundamental" in msg.lower() and ("required" in msg.lower() or "failed" in msg.lower())
            or "get_history_window" in msg and "fundamental" in msg.lower()
            or "factor_entry" in msg
            or "model_weight" in msg
        )
        raise HTTPException(status_code=422 if known_asset_issue else 500, detail=msg)


@router.get("/tasks/{task_id}/selection/stream", summary="基于 task 一键选股（SSE：推导 loop 并流式日志）")
async def trigger_task_selection_stream(
    task_id: str,
    trade_date: Optional[str] = Query(None, description="推理日期 YYYY-MM-DD，默认当日"),
    cutoff_date: Optional[str] = Query(None, description="数据截止日期 YYYY-MM-DD；若设置，则推理取数不得晚于该日期"),
    top_k: int = Query(50, ge=1, le=500, description="返回候选数量，默认 50"),
    loop_id: Optional[int] = Query(None, description="可选：指定 loop_id；不传则从 task manifest 推导"),
):
    tid = str(task_id).strip()
    if not tid:
        raise HTTPException(status_code=422, detail="task_id 不能为空")

    mf = rdagent_task_sync_service.get_local_manifest_text(task_id=tid)
    if not mf.get("ok"):
        raise HTTPException(status_code=404, detail=mf.get("error") or "manifest_not_found")
    manifest_obj = _try_parse_json_text(mf.get("content") or "")
    if not isinstance(manifest_obj, dict):
        raise HTTPException(status_code=422, detail="manifest.json 解析失败")

    derived = _derive_task_run_and_loop_from_manifest(manifest_obj)
    chosen_loop_id = loop_id if loop_id is not None else derived.get("loop_id")
    task_run_id = derived.get("task_run_id")
    if not task_run_id or chosen_loop_id is None:
        assets_status = _get_local_task_assets_status(
            manifest_path=mf.get("manifest_path"),
            manifest_obj=manifest_obj,
        )
        factor_ok = bool(assets_status.get("factor_entry", {}).get("exists"))
        model_ok = bool(assets_status.get("model_weight", {}).get("exists"))

        if factor_ok and model_ok:
            task_run_id = tid
            chosen_loop_id = int(loop_id) if loop_id is not None else 0
            derived = {**(derived or {}), "fallback": True}
        else:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "task_not_selectable",
                    "reason": "无法从 task manifest 推导 task_run_id/loop_id；且本地 primary_assets 未提供可推理的完整资产（factor_entry/model.pkl）。",
                    "derived": derived,
                    "manifest_path": mf.get("manifest_path"),
                    "assets_status": assets_status,
                    "hint": "该 task 可能只有因子实验、没有模型实验/模型权重；一期 Task-only strict 下不可选股。请选择具备 model.pkl/params.pkl 的 task，或使用 loop 选股接口。",
                },
            )

    # 复用 loop 的 SSE 逻辑：通过内部调用 build_loop_selection 并挂载日志 handler
    reqid = str(uuid.uuid4())
    q: "queue.Queue[str]" = queue.Queue()
    done = threading.Event()
    result_holder: Dict[str, Any] = {}
    error_holder: Dict[str, Any] = {}

    fmt = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    handler = _SelectionStreamHandler(q)
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)

    targets = [
        logging.getLogger("aistock.rdagent_selection"),
        logging.getLogger("aistock.inference"),
        logging.getLogger("aistock.timescaledb_adapter"),
        logging.getLogger("aistock.rdagent_router"),
    ]

    async def _run() -> None:
        token = _selection_stream_reqid.set(reqid)
        for lg in targets:
            try:
                lg.addHandler(handler)
                lg.setLevel(logging.INFO)
                lg.propagate = True
            except Exception:
                pass
        try:
            q.put_nowait(
                f"task_selection_stream_start reqid={reqid} task_id={tid} task_run_id={task_run_id} loop_id={chosen_loop_id} trade_date={trade_date} top_k={top_k} cutoff_date={cutoff_date}"
            )
            payload = await run_in_threadpool(
                build_loop_selection,
                task_run_id=str(task_run_id),
                loop_id=int(chosen_loop_id),
                trade_date=trade_date,
                cutoff_date=cutoff_date,
                top_k=top_k,
            )
            result_holder["payload"] = {
                "ok": True,
                "task_id": tid,
                "task_run_id": str(task_run_id),
                "loop_id": int(chosen_loop_id),
                "derived": derived,
                **payload,
            }
        except Exception as exc:
            msg = str(exc)
            error_holder["detail"] = msg
            error_holder["status_code"] = 500
            known_asset_issue = (
                "parquet 回放型" in msg
                or "Qlib handler" in msg
                or "config_file" in msg
                or "data_handler_config" in msg
                or "训练" in msg and "特征" in msg and "不一致" in msg
                or "训练特征存在缺失值" in msg
                or "nan_cols_sample" in msg
                or "Column_" in msg
                or "关键数据未入库" in msg
                or "fundamental" in msg.lower() and ("required" in msg.lower() or "failed" in msg.lower())
                or "get_history_window" in msg and "fundamental" in msg.lower()
            )
            if known_asset_issue:
                error_holder["status_code"] = 422
            q.put_nowait(f"task_selection_stream_error status_code={error_holder['status_code']} detail={msg}")
        finally:
            for lg in targets:
                try:
                    lg.removeHandler(handler)
                except Exception:
                    pass
            _selection_stream_reqid.reset(token)
            done.set()

    asyncio_task = asyncio.create_task(_run())

    async def _gen():
        yield _selection_sse_format(
            "start",
            json.dumps(
                {
                    "reqid": reqid,
                    "task_id": tid,
                    "task_run_id": str(task_run_id),
                    "loop_id": int(chosen_loop_id),
                    "trade_date": trade_date,
                    "cutoff_date": cutoff_date,
                    "top_k": top_k,
                    "derived": derived,
                },
                ensure_ascii=False,
            ),
        )
        last_ping = time.time()
        while not done.is_set() or not q.empty():
            try:
                line = q.get_nowait()
                yield _selection_sse_format("log", line)
                continue
            except queue.Empty:
                pass
            now = time.time()
            if now - last_ping >= 10:
                last_ping = now
                yield _selection_sse_format("ping", "")
            await asyncio.sleep(0.2)

        if error_holder:
            yield _selection_sse_format("error", json.dumps(error_holder, ensure_ascii=False))
        else:
            yield _selection_sse_format("result", json.dumps(result_holder.get("payload") or {}, ensure_ascii=False))

        try:
            await asyncio_task
        except Exception:
            pass

    return StreamingResponse(_gen(), media_type="text/event-stream")


@router.post("/strategies/{strategy_id}/inference", summary="触发策略预览推理任务")
def trigger_strategy_inference(strategy_id: str, req: InferenceRequest) -> Dict[str, Any]:
    """
    手动触发指定策略版本的推理任务。
    执行流程：加载因子 -> 拉取行情/基本面 -> 模型预测 -> 结果入库。
    """
    try:
        t_date = datetime.now()
        if req.trade_date:
            t_date = datetime.strptime(req.trade_date, "%Y-%m-%d")
            
        # 执行推理
        # 注意：这里会产出打分并存入 trading.rdagent_signal 表
        scores = inference_engine.run_inference(
            strategy_id=strategy_id,
            version_tag=req.version_tag,
            trade_date=t_date
        )
        
        return {
            "ok": True,
            "message": f"策略 {strategy_id} 推理任务执行成功",
            "symbols_count": len(scores),
            "trade_date": t_date.strftime("%Y-%m-%d")
        }
    except Exception as exc:
        msg = str(exc)
        logging.getLogger(__name__).error(f"推理任务执行失败: {msg}", exc_info=True)
        known_asset_issue = (
            "parquet 回放型" in msg
            or "Qlib handler" in msg
            or "config_file" in msg
            or "data_handler_config" in msg
            or "训练" in msg and "特征" in msg and "不一致" in msg
            or "provider_uri" in msg
        )
        raise HTTPException(status_code=422 if known_asset_issue else 500, detail=msg)


def _normalize_workspace_path(raw: str) -> str:
    p = (raw or "").strip()
    if os.name != "nt":
        return p
    if p.startswith("/mnt/") and len(p) > 6:
        drive = p[5]
        if p[6:7] == "/":
            rest = p[7:]
            return f"{drive.upper()}:/{rest}"
    return p


def _safe_read_json(abs_path: Path) -> Any | None:
    try:
        if not abs_path.exists() or not abs_path.is_file():
            return None
        if abs_path.stat().st_size > 2 * 1024 * 1024:
            return {"_error": "file too large", "path": str(abs_path)}
        return json.loads(abs_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"_error": str(exc), "path": str(abs_path)}


@router.get("/strategies", summary="列出已导入的 RD-Agent 策略")
def list_rdagent_strategies(
    enabled: Optional[bool] = Query(None, description="按是否启用过滤"),
) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = [
                    "SELECT s.strategy_id, s.strategy_name, s.strategy_kind, s.output_mode,",
                    "       s.enabled, s.created_at, s.updated_at,",
                    "       s.source_strategy_key, ss.source_type, ss.name AS source_name",
                    "FROM trading.strategy AS s",
                    "JOIN trading.strategy_source AS ss ON s.source_id = ss.source_id",
                    "WHERE ss.source_type = 'rdagent'",
                ]
                params: list[Any] = []
                if enabled is not None:
                    sql.append("AND s.enabled = %s")
                    params.append(enabled)
                sql.append("ORDER BY s.created_at DESC")
                cur.execute("\n".join(sql), params)
                rows = cur.fetchall()

        strategies: List[Dict[str, Any]] = []
        for r in rows:
            (
                strategy_id,
                strategy_name,
                strategy_kind,
                output_mode,
                enabled_val,
                created_at,
                updated_at,
                source_strategy_key,
                source_type,
                source_name,
            ) = r
            strategies.append(
                {
                    "strategy_id": str(strategy_id),
                    "strategy_name": strategy_name,
                    "strategy_kind": strategy_kind,
                    "output_mode": output_mode,
                    "enabled": bool(enabled_val),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "source_strategy_key": source_strategy_key,
                    "source_type": source_type,
                    "source_name": source_name,
                },
            )

        return {"items": strategies}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/strategies/{strategy_id}/result", summary="获取 RD-Agent 策略的回测结果概览")
def get_rdagent_strategy_result(strategy_id: str) -> Dict[str, Any]:
    """Return minimal backtest metrics and equity curve for a given RD-Agent strategy.

    This endpoint:
    - looks up the strategy row in trading.strategy and ensures source_type='rdagent';
    - parses source_strategy_key to extract task_run/loop/workspace identifiers;
    - uses RDRegistryReader to locate the corresponding workspace in RD-Agent registry;
    - reads backtest_metrics (qlib_res.csv) and backtest_curve (ret.pkl) if available;
    - returns key metrics and an equity curve series.
    """

    try:
        # 1) resolve RD-Agent source strategy
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.source_strategy_key
                    FROM trading.strategy AS s
                    JOIN trading.strategy_source AS ss ON s.source_id = ss.source_id
                    WHERE s.strategy_id = %s
                      AND ss.source_type = 'rdagent'
                    """,
                    (strategy_id,),
                )
                row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="strategy not found or not rdagent source")

        source_strategy_key = row[0]

        # 2) parse workspace_id from source_strategy_key: task_run:XXX/loop:YYY/workspace:ZZZ
        workspace_id: Optional[str] = None
        try:
            parts = str(source_strategy_key).split("/")
            for p in parts:
                if p.startswith("workspace:"):
                    workspace_id = p.split(":", 1)[1]
                    break
        except Exception:
            workspace_id = None

        if not workspace_id:
            raise HTTPException(status_code=400, detail="invalid source_strategy_key format (missing workspace)")

        # 3) locate workspace in RD-Agent registry
        db_path = RDRegistryReader.resolve_db_path()
        reader = RDRegistryReader(db_path)
        try:
            ws = reader.get_workspace(workspace_id)
        except KeyError as exc:  # noqa: PERF203
            raise HTTPException(status_code=404, detail=str(exc))

        raw_workspace_path = _normalize_workspace_path(ws.workspace_path)
        workspace_root = Path(raw_workspace_path)
        if not workspace_root.exists():
            raise HTTPException(status_code=404, detail=f"workspace_path not found: {workspace_root}")

        # 4) resolve artifact file paths relative to workspace
        metrics_rel = reader.find_backtest_metrics_file(workspace_id)
        curve_rel = reader.find_backtest_curve_file(workspace_id)

        metrics_abs = workspace_root / metrics_rel if metrics_rel else None
        curve_abs = workspace_root / curve_rel if curve_rel else None

        metrics: Dict[str, Any] = {}
        equity_curve: List[Dict[str, Any]] = []

        # 5) load metrics from qlib_res.csv (if present)
        if metrics_abs and metrics_abs.exists():
            try:
                df_metrics = pd.read_csv(metrics_abs)
                if not df_metrics.empty:
                    row0 = df_metrics.iloc[0].to_dict()
                    # best-effort selection of common fields
                    preferred_keys = [
                        "ann_ret",
                        "annual_return",
                        "excess_return_annual",
                        "max_drawdown",
                        "mdd",
                        "sharpe",
                        "information_ratio",
                        "info_ratio",
                        "ic",
                        "ic_mean",
                    ]
                    for k in preferred_keys:
                        if k in row0:
                            metrics[k] = row0[k]
                    # always include raw row for inspection
                    metrics["raw"] = row0
            except Exception as exc:  # noqa: BLE001
                metrics["_error"] = str(exc)

        # 6) load equity curve from ret.pkl (if present)
        if curve_abs and curve_abs.exists():
            try:
                obj = pd.read_pickle(curve_abs)
                # heuristics: Series or DataFrame
                if isinstance(obj, pd.Series):
                    series = obj
                elif isinstance(obj, pd.DataFrame):
                    col = None
                    for c in [
                        "cum",
                        "cum_ret",
                        "nav",
                        "equity",
                        "value",
                        "portfolio_value",
                    ]:
                        if c in obj.columns:
                            col = c
                            break
                    if col is None:
                        col = obj.columns[0]
                    series = obj[col]
                else:
                    series = None

                if series is not None:
                    series = series.dropna().copy()
                    # attempt to interpret as returns and build cumulative nav if looks small
                    vals = series.astype(float)
                    if (vals.abs() < 0.5).all():
                        nav = (1.0 + vals).cumprod()
                    else:
                        nav = vals
                    # convert to list of {date, nav}
                    if isinstance(nav.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                        for ts, v in nav.items():
                            equity_curve.append({"date": str(ts.date()), "nav": float(v)})
                    else:
                        for i, v in enumerate(nav.values):
                            equity_curve.append({"index": int(i), "nav": float(v)})
            except Exception as exc:  # noqa: BLE001
                metrics.setdefault("curve_error", str(exc))

        return {
            "registry_db_path": db_path,
            "workspace_id": workspace_id,
            "workspace_path": ws.workspace_path,
            "metrics": metrics,
            "equity_curve": equity_curve,
        }
    except HTTPException:
        # already structured
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/strategies/{strategy_id}/versions", summary="列出指定 RD-Agent 策略的所有版本")
def list_rdagent_strategy_versions(strategy_id: str) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT strategy_version_id, version_tag, artifact_root_path,
                           import_status, created_at
                    FROM trading.strategy_version
                    WHERE strategy_id = %s
                    ORDER BY created_at DESC
                    """,
                    (strategy_id,),
                )
                rows = cur.fetchall()

        versions: List[Dict[str, Any]] = []
        for r in rows:
            strategy_version_id, version_tag, artifact_root_path, import_status, created_at = r
            versions.append(
                {
                    "strategy_version_id": str(strategy_version_id),
                    "version_tag": version_tag,
                    "artifact_root_path": artifact_root_path,
                    "import_status": import_status,
                    "created_at": created_at,
                },
            )

        return {"items": versions}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/overview", summary="signals 概览")
def signals_overview(strategy_version_id: str = Query(...)) -> Dict[str, Any]:
    try:
        return load_signals_overview(strategy_version_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/by_date", summary="按日期获取 signals")
def signals_by_date(
    strategy_version_id: str = Query(...),
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    k: int = Query(50, ge=1, le=5000),
) -> Dict[str, Any]:
    try:
        rows = load_signals_for_date(strategy_version_id, trade_date, k=k)
        return {"trade_date": trade_date, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/symbol_series", summary="按标的获取 signals 时间序列")
def symbol_series(
    strategy_version_id: str = Query(...),
    symbol: str = Query(...),
    limit: int = Query(200, ge=1, le=5000),
) -> Dict[str, Any]:
    try:
        rows = load_symbol_series(strategy_version_id, symbol, limit=limit)
        return {"symbol": symbol, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ==================== 备选TASK和LOOP管理 API ====================

@router.get("/candidate-tasks", summary="获取备选TASK列表（带缓存）")
def get_candidate_tasks(
    limit: Optional[int] = Query(50, description="限制返回数量，默认50"),
    include_deleted: bool = Query(False, description="是否包含已删除的TASK"),
    auto_scan: bool = Query(True, description="是否自动扫描新TASK")
) -> Dict[str, Any]:
    """
    获取备选TASK列表，优先从数据库缓存读取
    
    如果auto_scan=True，会先通过API扫描各计算节点发现新TASK
    """
    try:
        candidate_service = get_candidate_service()
        
        # 自动扫描新TASK
        scan_result = None
        if auto_scan:
            scan_result = candidate_service.scan_and_sync_tasks(limit=limit)
        
        # 获取TASK列表
        tasks = candidate_service.get_candidate_tasks(limit=limit, include_deleted=include_deleted)
        
        return {
            "ok": True,
            "count": len(tasks),
            "tasks": tasks,
            "scan_result": scan_result
        }
    except Exception as e:
        logging.error(f"获取备选TASK列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/candidate-loops", summary="获取TASK的LOOP详情（带缓存）")
def get_task_candidate_loops(
    task_id: str,
    force_refresh: bool = Query(False, description="是否强制刷新（重新从API获取）")
) -> Dict[str, Any]:
    """
    获取TASK的LOOP详情，优先从数据库缓存读取
    
    如果缓存不存在或force_refresh=True，会从RD-Agent API获取并缓存
    """
    try:
        candidate_service = get_candidate_service()
        
        loops, from_cache = candidate_service.get_task_loops(task_id, force_refresh=force_refresh)
        
        return {
            "ok": True,
            "task_id": task_id,
            "count": len(loops),
            "loops": loops,
            "from_cache": from_cache
        }
    except Exception as e:
        logging.error(f"获取TASK {task_id} LOOP详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/loops/{loop_id}/sync-factors", summary="同步指定Loop的因子到因子库")
def sync_loop_factors(task_id: str, loop_id: int) -> Dict[str, Any]:
    """从 RD-Agent API 获取指定 Loop 的因子代码和指标，
    同步到 aistock_factor_catalog 表，标记为该 task 的 SOTA 因子。

    同时更新 aistock_task_catalog 的 sota_factors_count。
    """
    from ..services.rdagent_results_api_client import RDAgentResultsApiClient
    from ..services.rdagent_factor_catalog_sync import sync_factors_from_loop

    try:
        # 0. 前置检查：Task 必须已同步到 aistock_task_catalog
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sync_status FROM aistock_task_catalog WHERE task_id = %s",
                    (task_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Task {task_id} 尚未同步到 AIstock，请先在 Task Sync 页面同步该 Task",
                    )
                if row[0] != "success":
                    raise HTTPException(
                        status_code=400,
                        detail=f"Task {task_id} 同步状态为 {row[0]}，请先确保 Task 同步成功",
                    )

        # 1. 从 RDAgent API 获取指定 Loop 的因子
        client = RDAgentResultsApiClient()
        api_path = f"/api/extractors/sota_factors/v2/{task_id}/loops/{loop_id}/factors"
        loop_factors_data = client._task_get_json(api_path, timeout=300.0)

        if not loop_factors_data.get("success"):
            raise HTTPException(
                status_code=404,
                detail=loop_factors_data.get("error", f"Loop {loop_id} 因子提取失败"),
            )

        factor_count = loop_factors_data.get("factor_count", 0)
        if factor_count == 0:
            return {
                "ok": True,
                "task_id": task_id,
                "loop_id": loop_id,
                "message": f"Loop {loop_id} 没有通过 final_decision 的因子",
                "inserted": 0,
            }

        # 2. 确定 task_dir
        aistock_root = Path(os.environ.get("AISTOCK_ROOT") or "f:/Dev/AIstock").resolve()
        task_dir = aistock_root / "rdagent_assets" / "rdagent_tasks" / task_id
        task_dir.mkdir(parents=True, exist_ok=True)

        # 3. 调用同步函数入库
        sync_result = sync_factors_from_loop(
            task_id=task_id,
            loop_id=loop_id,
            loop_factors_data=loop_factors_data,
            task_dir=str(task_dir),
        )

        return {
            "ok": sync_result.ok,
            "task_id": task_id,
            "loop_id": loop_id,
            "total_factors": sync_result.total_sota_factors,
            "inserted": sync_result.inserted,
            "updated": sync_result.updated,
            "dedup_skipped": sync_result.dedup_skipped,
            "errors": sync_result.errors,
            "message": (
                f"Loop {loop_id} 因子同步完成: "
                f"{sync_result.inserted} 个因子入库"
                + (f", {sync_result.dedup_skipped} 去重跳过" if sync_result.dedup_skipped else "")
                + (f", {len(sync_result.errors)} 错误" if sync_result.errors else "")
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"同步 Loop {loop_id} 因子失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/synced-loop-ids", summary="查询已同步因子的Loop ID列表")
def get_synced_loop_ids(task_id: str) -> Dict[str, Any]:
    """从 aistock_factor_catalog 查询该 Task 中哪些 Loop 已手动同步过因子。

    返回 synced_loop_ids 列表和每个 Loop 同步的因子数。
    """
    import re as _re
    try:
        result: Dict[int, int] = {}  # loop_id -> factor_count
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source_loop_tag, COUNT(*) AS cnt
                    FROM aistock_factor_catalog
                    WHERE source_task_id = %s
                      AND catalog_source = 'rdagent_loop_manual_sync'
                      AND source_loop_tag IS NOT NULL
                    GROUP BY source_loop_tag
                """, (task_id,))
                for row in cur.fetchall():
                    tag, cnt = row[0], row[1]
                    m = _re.match(r"loop_(\d+)_manual_sync", tag or "")
                    if m:
                        result[int(m.group(1))] = cnt
        return {
            "ok": True,
            "task_id": task_id,
            "synced_loop_ids": sorted(result.keys()),
            "synced_loop_details": {str(k): v for k, v in result.items()},
        }
    except Exception as e:
        logging.error(f"查询 Task {task_id} 已同步 Loop 失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/workspaces", summary="获取Task的workspace信息（自动路由到所属节点）")
def get_task_workspaces(
    task_id: str,
    quick: bool = Query(True, description="快速模式：只返回log目录信息，不扫描workspace"),
) -> Dict[str, Any]:
    """通过自动路由获取 task 所属计算节点的 workspace 信息。
    quick=True: 秒级返回log目录大小+Loop数量；quick=False: 完整pickle扫描获取workspace列表。
    """
    try:
        candidate_service = get_candidate_service()
        return candidate_service.get_task_workspaces(task_id, quick=quick)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"获取 Task {task_id} workspace 信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}", summary="删除Task及其所有数据（自动路由到所属节点）")
def delete_task(task_id: str) -> Dict[str, Any]:
    """
    自动路由到 task 所属计算节点执行删除，并清理 AIstock 本地数据：
    1. 远端: task log + workspace + scheduler log（通过节点 API）
    2. 本地: dispatch_logs/{task_id}/ 目录
    3. DB: rdagent_candidate_loops + rdagent_candidate_tasks 缓存记录

    远端无文件时仍继续清理本地DB和dispatch日志。
    """
    import shutil

    candidate_service = get_candidate_service()
    remote_warning = None

    # 1. 调用远端节点 API 删除 task log + workspace + scheduler log
    try:
        remote_result = candidate_service.delete_task_on_node(task_id)
        if not remote_result.get("ok"):
            remote_warning = f"远端节点: {remote_result.get('error', '未知错误')}"
    except Exception as e:
        remote_warning = f"远端节点不可达: {e}"

    # 2. 删除 AIstock 本地 dispatch 日志
    dispatch_dir = Path(__file__).resolve().parents[2] / "dispatch_logs" / task_id
    dispatch_deleted = False
    if dispatch_dir.exists() and dispatch_dir.is_dir():
        shutil.rmtree(dispatch_dir)
        dispatch_deleted = True

    # 3. 清理 DB 缓存
    db_loops_deleted = 0
    db_tasks_deleted = 0
    conn = candidate_service._get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM rdagent.rdagent_candidate_loops WHERE task_id = %s", (task_id,))
        db_loops_deleted = cur.rowcount
        cur.execute("DELETE FROM rdagent.rdagent_candidate_tasks WHERE task_id = %s", (task_id,))
        db_tasks_deleted = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()

    parts = []
    if remote_warning:
        parts.append(remote_warning)
    else:
        parts.append(remote_result.get("message", "远端文件已删除"))
    if dispatch_deleted:
        parts.append("dispatch日志已删除")
    if db_loops_deleted or db_tasks_deleted:
        parts.append(f"DB缓存已清理(loops={db_loops_deleted}, tasks={db_tasks_deleted})")

    return {
        "ok": True,
        "task_id": task_id,
        "message": "；".join(parts),
        "remote_result": remote_result if not remote_warning else None,
        "remote_warning": remote_warning,
        "dispatch_deleted": dispatch_deleted,
        "db_loops_deleted": db_loops_deleted,
        "db_tasks_deleted": db_tasks_deleted,
    }


@router.post("/candidate-tasks/refresh", summary="手动刷新备选TASK列表")
def refresh_candidate_tasks(
    limit: Optional[int] = Query(None, description="限制扫描数量")
) -> Dict[str, Any]:
    """
    手动触发扫描RD-Agent log目录，发现新TASK并更新数据库
    """
    try:
        candidate_service = get_candidate_service()
        result = candidate_service.scan_and_sync_tasks(limit=limit)
        
        return {
            "ok": True,
            **result
        }
    except Exception as e:
        logging.error(f"刷新备选TASK列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/refresh", summary="刷新指定TASK的数据")
def refresh_single_task(task_id: str) -> Dict[str, Any]:
    """
    清除指定TASK的LOOP缓存，重新获取V2对齐信息和LOOP数据

    注意：不再删除 rdagent_candidate_tasks 记录（保留节点路由信息和V2历史数据），
    而是通过 UPDATE 刷新 V2 对齐字段。
    """
    try:
        candidate_service = get_candidate_service()

        # 1. 仅删除LOOP缓存（保留 candidate_tasks 记录以维持节点路由信息）
        conn = candidate_service._get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM rdagent.rdagent_candidate_loops
            WHERE task_id = %s
        """, (task_id,))
        deleted_loops = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"已删除Task {task_id} 的 {deleted_loops} 条LOOP缓存")

        # 2. 重新获取V2对齐信息并UPDATE候选任务记录
        v2_updated = False
        try:
            client = candidate_service._get_client_for_task(task_id)
            # 从DB读取节点信息
            conn2 = candidate_service._get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute("SELECT log_dir FROM rdagent.rdagent_candidate_tasks WHERE task_id = %s", (task_id,))
            row = cur2.fetchone()
            cur2.close()
            conn2.close()

            node_id = "default"
            if row and row[0] and ":" in row[0]:
                potential = row[0].split(":", 1)[0]
                if not (len(potential) == 1 and potential.isalpha()):
                    node_id = potential

            task_info = candidate_service._fetch_task_info(task_id, node_id=node_id, client=client)
            if task_info:
                import json as _json
                conn3 = candidate_service._get_db_connection()
                cur3 = conn3.cursor()
                cur3.execute("""
                    UPDATE rdagent.rdagent_candidate_tasks
                    SET has_sota = %s,
                        sota_factors_count = %s,
                        sota_checked_at = %s,
                        alpha_factors_count = %s,
                        model_feature_count = %s,
                        is_aligned = %s,
                        v2_checked_at = %s,
                        sota_factors_list = %s,
                        alpha_factors_list = %s,
                        hist_len = %s,
                        task_status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE task_id = %s
                """, (
                    task_info['has_sota'],
                    task_info['sota_factors_count'],
                    task_info.get('sota_checked_at'),
                    task_info.get('alpha_factors_count', 0),
                    task_info.get('model_feature_count'),
                    task_info.get('is_aligned'),
                    task_info.get('v2_checked_at'),
                    _json.dumps(task_info['sota_factors_list']) if task_info.get('sota_factors_list') else None,
                    _json.dumps(task_info['alpha_factors_list']) if task_info.get('alpha_factors_list') else None,
                    task_info.get('hist_len', 0),
                    task_info.get('task_status'),
                    task_id,
                ))
                conn3.commit()
                cur3.close()
                conn3.close()
                v2_updated = True
                logging.info(f"Task {task_id} V2信息已更新: sota={task_info['sota_factors_count']}, aligned={task_info.get('is_aligned')}")
        except Exception as e:
            logging.warning(f"刷新Task {task_id} V2信息失败（继续刷新LOOP）: {e}")

        # 3. 强制从RD-Agent API重新获取LOOP数据
        loops, from_cache = candidate_service.get_task_loops(task_id, force_refresh=True)

        return {
            "ok": True,
            "task_id": task_id,
            "deleted_loops": deleted_loops,
            "v2_updated": v2_updated,
            "refreshed_loops": len(loops),
            "message": (
                f"已刷新Task {task_id}：清除 {deleted_loops} 条LOOP缓存，"
                f"{'V2信息已更新，' if v2_updated else ''}重新获取了 {len(loops)} 个LOOP"
            )
        }
    except Exception as e:
        logging.error(f"刷新Task {task_id} 失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
