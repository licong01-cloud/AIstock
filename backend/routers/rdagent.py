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
async def trigger_batch_inference(req: BatchInferenceRequest) -> Dict[str, Any]:
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
def list_latest_tasks(limit: int = Query(20, ge=1, le=200, description="返回数量，默认 20")) -> Dict[str, Any]:
    return rdagent_task_sync_service.list_latest_tasks(limit=limit)


@router.get("/tasks/sync-candidates", summary="获取 task 同步候选（latest+summary+本地同步状态合并）")
def list_task_sync_candidates(limit: int = Query(20, ge=1, le=200, description="候选数量，默认 20")) -> Dict[str, Any]:
    return rdagent_task_sync_service.list_sync_candidates(limit=limit)


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
            r = rdagent_task_sync_service.sync_task_from_log(task_id=tid, operator=req.operator)
        elif mode == "api":
            r = rdagent_task_sync_service.sync_task(task_id=tid, operator=req.operator)
        else:
            r = rdagent_task_sync_service.sync_task(task_id=tid, operator=req.operator)
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


@router.post("/tasks/sync-log-only", summary="log-only 模式同步：直接从 RD-Agent log 落盘资产（包含 factor_order.json）")
def sync_tasks_log_only(req: TaskSyncRequest) -> Dict[str, Any]:
    task_ids = [str(x).strip() for x in (req.task_ids or []) if str(x).strip()]
    if not task_ids:
        raise HTTPException(status_code=422, detail="task_ids 不能为空")

    results: List[Dict[str, Any]] = []
    ok_cnt = 0
    for tid in task_ids:
        r = rdagent_task_sync_service.sync_task_from_log(task_id=tid, operator=req.operator)
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
    
    如果auto_scan=True，会先扫描RD-Agent log目录发现新TASK
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
    清除指定TASK在数据库中的LOOP缓存和候选任务缓存，并从RD-Agent API重新获取最新数据
    
    这将：
    1. 删除数据库中该TASK的所有LOOP缓存记录（rdagent_candidate_loops）
    2. 删除数据库中该TASK的候选任务缓存记录（rdagent_candidate_tasks），
       使 list_sync_candidates 下次调用时重新从 RD-Agent API 获取最新 SOTA 数量
    3. 从RD-Agent API获取最新的LOOP数据并重新缓存
    """
    try:
        candidate_service = get_candidate_service()
        
        conn = candidate_service._get_db_connection()
        cur = conn.cursor()
        
        # 1. 删除数据库中的LOOP缓存
        cur.execute("""
            DELETE FROM rdagent.rdagent_candidate_loops
            WHERE task_id = %s
        """, (task_id,))
        deleted_loops = cur.rowcount

        # 2. 删除候选任务缓存（rdagent_candidate_tasks），
        #    使 list_sync_candidates 重新调用 v2_alignment_preview API 获取最新 SOTA 数量
        cur.execute("""
            DELETE FROM rdagent.rdagent_candidate_tasks
            WHERE task_id = %s
        """, (task_id,))
        deleted_candidate = cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        
        logging.info(f"已删除Task {task_id} 的 {deleted_loops} 条LOOP缓存、{deleted_candidate} 条候选任务缓存")
        
        # 3. 强制从RD-Agent API重新获取LOOP数据
        loops, from_cache = candidate_service.get_task_loops(task_id, force_refresh=True)
        
        return {
            "ok": True,
            "task_id": task_id,
            "deleted_loops": deleted_loops,
            "deleted_candidate_cache": deleted_candidate,
            "refreshed_loops": len(loops),
            "message": (
                f"已刷新Task {task_id}：清除 {deleted_loops} 条LOOP缓存、"
                f"{deleted_candidate} 条候选任务缓存，重新获取了 {len(loops)} 个LOOP"
            )
        }
    except Exception as e:
        logging.error(f"刷新Task {task_id} 失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
