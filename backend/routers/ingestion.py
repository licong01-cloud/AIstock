from __future__ import annotations

import datetime as dt
import importlib
import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional

import psycopg2.extras as pgx
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field
import requests

from ..db.pg_pool import get_conn
from ..ingestion.tdx_scheduler import scheduler  # 1:1 复用现有调度器实现
from ..services.tushare_dataset_specs import DATASET_REGISTRY
from ..services.trading_calendar_status import TradingCalendarStatusService
from ..services.trading_core.errors import DataUnavailableError


router = APIRouter(prefix="/api", tags=["ingestion"])

FINANCIAL_EVENT_RAW_DATASETS = {
    "tushare_forecast_raw",
    "tushare_express_raw",
    "tushare_fina_indicator_raw",
}


# ---------------------------------------------------------------------------
# 通用 JSON / 时间处理工具（保持与 tdx_backend 中实现一致）
# ---------------------------------------------------------------------------


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _json_load(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).warning("_json_load: failed to parse value as JSON, returning empty dict: %.120s", value)
        return {}


def _uses_calendar_date_sequence(extra_info: Any) -> bool:
    info = _json_load(extra_info)
    if not isinstance(info, dict):
        return False
    return str(info.get("date_sequence") or "").strip().lower() in {"calendar", "calendar_day", "natural_day"}


def _uses_refresh_audit_cursor(extra_info: Any) -> bool:
    info = _json_load(extra_info)
    if not isinstance(info, dict):
        return False
    return str(info.get("cursor_source") or "").strip().lower() in {"refresh_audit", "audit"}


def _trading_calendar_service() -> TradingCalendarStatusService:
    return TradingCalendarStatusService()


def _latest_trading_day_on_or_before(as_of_date: Optional[dt.date] = None) -> Optional[dt.date]:
    return _trading_calendar_service().latest_trading_day_on_or_before(as_of_date or dt.date.today())


def _next_trading_day_after(anchor_date: dt.date) -> dt.date:
    return _trading_calendar_service().next_trading_day(anchor_date)


def _raise_trading_calendar_unavailable(exc: DataUnavailableError) -> None:
    raise HTTPException(
        status_code=400,
        detail={
            "error_code": exc.error_code,
            "message": exc.message,
            "context": exc.context,
        },
    ) from exc


def _isoformat(value: Optional[dt.datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc).isoformat()
    return value.astimezone(dt.timezone.utc).isoformat()


def _date_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def _fetchall(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def _fetchone(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    rows = _fetchall(sql, params)
    return rows[0] if rows else None


def _execute(sql: str, params: tuple) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


# ---------------------------------------------------------------------------
# 数据结构（与 tdx_backend 保持 1:1）
# ---------------------------------------------------------------------------


SUPPORTED_INGESTION_MODES = {"init", "incremental"}


class ToggleRequest(BaseModel):
    enabled: bool


class IngestionRunRequest(BaseModel):
    dataset: str
    mode: str
    triggered_by: str = "api"
    options: Dict[str, Any] = Field(default_factory=dict)

    def validate_mode(self) -> None:
        if self.mode not in SUPPORTED_INGESTION_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"mode must be one of {sorted(SUPPORTED_INGESTION_MODES)}",
            )


class IngestionScheduleUpsertRequest(BaseModel):
    schedule_id: Optional[uuid.UUID] = None
    dataset: str
    mode: str
    frequency: str
    enabled: bool = True
    options: Dict[str, Any] = Field(default_factory=dict)

    def validate_mode(self) -> None:
        if self.mode not in SUPPORTED_INGESTION_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"mode must be one of {sorted(SUPPORTED_INGESTION_MODES)}",
            )


class IngestionInitRequest(BaseModel):
    dataset: str
    options: Dict[str, Any] = Field(default_factory=dict)


class TestingRunRequest(BaseModel):
    triggered_by: str = "api"
    options: Dict[str, Any] = Field(default_factory=dict)


class TestingScheduleUpsertRequest(BaseModel):
    schedule_id: Optional[uuid.UUID] = None
    frequency: str
    enabled: bool = True
    options: Dict[str, Any] = Field(default_factory=dict)


class IngestionLogKey(BaseModel):
    job_id: uuid.UUID
    ts: dt.datetime


class BulkDeleteIngestionLogsRequest(BaseModel):
    items: List[IngestionLogKey] = Field(default_factory=list)
    delete_all: bool = False


class BulkDeleteTestingRunsRequest(BaseModel):
    run_ids: List[uuid.UUID] = Field(default_factory=list)
    delete_all: bool = False


# ---------------------------------------------------------------------------
# 内部辅助：job / schedule / log 序列化（复制自 tdx_backend）
# ---------------------------------------------------------------------------


def _ensure_testing_schedule(schedule_id: uuid.UUID) -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT schedule_id, enabled, frequency, options,
               last_run_at, next_run_at, last_status, last_error,
               created_at, updated_at
          FROM market.testing_schedules
         WHERE schedule_id = %s
        """,
        (schedule_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Testing schedule not found")
    return rows[0]


def _ensure_ingestion_schedule(schedule_id: uuid.UUID) -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT schedule_id, dataset, mode, enabled, frequency, options,
               last_run_at, next_run_at, last_status, last_error,
               created_at, updated_at
          FROM market.ingestion_schedules
         WHERE schedule_id = %s
        """,
        (schedule_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Ingestion schedule not found")
    return rows[0]


def _serialize_schedule(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schedule_id": str(row.get("schedule_id")),
        "enabled": row.get("enabled", True),
        "frequency": row.get("frequency"),
        "options": _json_load(row.get("options")) or {},
        "last_run_at": _isoformat(row.get("last_run_at")),
        "next_run_at": _isoformat(row.get("next_run_at")),
        "last_status": row.get("last_status"),
        "last_error": row.get("last_error"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _serialize_ingestion_schedule(row: Dict[str, Any]) -> Dict[str, Any]:
    base = _serialize_schedule(row)
    base.update({
        "dataset": row.get("dataset"),
        "mode": row.get("mode"),
    })
    # Extract inserted_rows from the latest job's summary (joined in query)
    job_summary_raw = row.get("last_job_summary")
    if job_summary_raw:
        job_summary = _json_load(job_summary_raw) if isinstance(job_summary_raw, str) else job_summary_raw
        if isinstance(job_summary, dict):
            base["last_inserted_rows"] = job_summary.get("inserted_rows") or job_summary.get("rows")
            # 仅对数据检查类调度透传完整报告，避免其他调度传输大量无关数据
            if row.get("dataset") == "_auto_retry_stale":
                base["last_job_summary"] = job_summary
    return base


def _serialize_ingestion_log(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = _json_load(row.get("message"))
    if not isinstance(payload, dict):
        payload = {"raw": payload}

    summary_raw = row.get("summary")
    summary: Dict[str, Any] = {}
    if summary_raw is not None:
        tmp = _json_load(summary_raw)
        if isinstance(tmp, dict):
            summary = tmp
    if summary and "summary" not in payload:
        payload["summary"] = summary

    dataset: Optional[str] = None
    mode: Optional[str] = None
    if summary:
        ds = summary.get("dataset")
        if not ds:
            ds_list = summary.get("datasets")
            if isinstance(ds_list, list) and ds_list:
                ds = ds_list[0]
        if isinstance(ds, str):
            dataset = ds
        mode_val = summary.get("mode")
        if isinstance(mode_val, str):
            mode = mode_val

    return {
        "run_id": str(row.get("job_id")) if row.get("job_id") else None,
        "timestamp": _isoformat(row.get("ts")),
        "level": row.get("level"),
        "dataset": dataset,
        "mode": mode,
        "payload": payload,
    }


def _serialize_testing_run(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "run_id": str(row.get("run_id")),
        "schedule_id": str(row.get("schedule_id")) if row.get("schedule_id") else None,
        "triggered_by": row.get("triggered_by"),
        "status": row.get("status"),
        "started_at": _isoformat(row.get("started_at")),
        "finished_at": _isoformat(row.get("finished_at")),
        "summary": _json_load(row.get("summary")) or {},
        "detail": _json_load(row.get("detail")) or {},
    }


def _infer_dataset(summary: Dict[str, Any]) -> Optional[str]:
    ds = summary.get("dataset")
    if not ds:
        ds_list = summary.get("datasets")
        if isinstance(ds_list, list) and ds_list:
            ds = ds_list[0]
    if isinstance(ds, str):
        return ds
    return None


def _infer_mode(job_type: Optional[str], summary: Dict[str, Any]) -> Optional[str]:
    mode = summary.get("mode")
    if isinstance(mode, str) and mode:
        return mode
    if job_type in {"init", "full"}:
        return "init"
    if job_type in {"incremental"}:
        return "incremental"
    return None


def _infer_source(dataset: Optional[str]) -> Optional[str]:
    ds = (dataset or "").strip().lower()
    if not ds:
        return None
    if ds in {"kline_daily_raw", "kline_minute_raw"}:
        return "tdx_api"
    if ds in {
        "stock_moneyflow_ts",
        "stock_basic",
        "stock_st",
        "stock_st_events",
        "bak_basic",
        "daily_basic",
        "stk_limit",
        "suspend_d",
        "margin_detail",
        *FINANCIAL_EVENT_RAW_DATASETS,
    }:
        return "tushare"
    if ds in {"index_daily", "index_basic"}:
        return "tushare"
    if ds in {"sw_index_classify", "sw_index_member", "sw_daily", "sw_sector", "sector_data"}:
        return "tushare"
    if ds == "anns_metadata":
        return "eastmoney_cninfo"
    if ds in {"kline_weekly"}:
        return "derived_from_kline_daily_raw"
    if ds.startswith("xtquant_"):
        return "xtquant"
    return None


def _infer_date_range(summary: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Best-effort extraction of [start_date, end_date] from heterogeneous summaries.

    不修改 summary 本身，只是为任务监视器提供统一视图，便于前端展示。
    """

    start: Optional[str] = None
    end: Optional[str] = None

    # 常用键名优先
    for key in ("start_date", "start_date_override", "start"):
        val = summary.get(key)
        if isinstance(val, str) and val:
            start = val
            break

    for key in ("end_date", "date", "target_date"):
        val = summary.get(key)
        if isinstance(val, str) and val:
            end = val
            break

    return {"start_date": start, "end_date": end}


def _create_init_job(summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    _execute(
        """
        INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, summary)
        VALUES (%s, 'init', 'queued', NOW(), %s)
        """,
        (job_id, _json_dump(summary)),
    )
    return job_id


def _create_job(job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    _execute(
        """
        INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, summary)
        VALUES (%s, %s, 'queued', NOW(), %s)
        """,
        (job_id, job_type, _json_dump(summary)),
    )
    return job_id


def _job_status(job_id: uuid.UUID) -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT job_id, job_type, status, created_at, started_at, finished_at, summary
          FROM market.ingestion_jobs
         WHERE job_id=%s
        """,
        (job_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Job not found")
    job = rows[0]
    summary = _json_load(job.get("summary")) or {}
    trows = _fetchall(
        """
        SELECT status, COUNT(*) AS cnt
          FROM market.ingestion_job_tasks
         WHERE job_id=%s
         GROUP BY status
        """,
        (job_id,),
    )
    total = done = failed = success = running = pending = 0
    for r in trows:
        cnt = int(r.get("cnt") or 0)
        total += cnt
        st = (r.get("status") or "").lower()
        if st == "success":
            success += cnt
            done += cnt
        elif st == "failed":
            failed += cnt
            done += cnt
        elif st in {"running"}:
            running += cnt
        elif st in {"queued", "pending"}:
            pending += cnt

    percent = 0
    if total > 0:
        if done > 0:
            percent = min(100, int((done / total) * 100))
        else:
            avg_rows = _fetchall(
                """
                SELECT COALESCE(AVG(progress), 0) AS avg_progress
                  FROM market.ingestion_job_tasks
                 WHERE job_id=%s
                """,
                (job_id,),
            )
            try:
                avg_progress = int(float((avg_rows[0] or {}).get("avg_progress") or 0))
            except Exception:  # noqa: BLE001
                logging.getLogger(__name__).debug("progress parse fallback: avg_progress")
                avg_progress = 0
            percent = max(percent, min(100, avg_progress))
    else:
        stats = summary.get("stats") or {}
        total_codes = int(summary.get("total_codes") or stats.get("total_codes") or 0)
        success_codes = int(summary.get("success_codes") or stats.get("success_codes") or 0)
        failed_codes = int(summary.get("failed_codes") or stats.get("failed_codes") or 0)
        total_days = int(summary.get("total_days") or 0)
        done_days = int(summary.get("done_days") or 0)
        if total_codes > 0:
            percent = min(100, int(((success_codes + failed_codes) / total_codes) * 100))
            total = total_codes
            done = success_codes + failed_codes
            success = success_codes
            failed = failed_codes
        elif total_days > 0:
            percent = min(100, int((done_days / total_days) * 100))
            total = total_days
            done = done_days
        else:
            # 兼容纯 Python 脚本（如 adj_factor）写入的 summary.counters / summary.progress
            counters_from_summary = summary.get("counters") or {}
            try:
                total_c = int(counters_from_summary.get("total") or 0)
                done_c = int(counters_from_summary.get("done") or 0)
            except Exception:  # noqa: BLE001
                logging.getLogger(__name__).debug("progress parse fallback: total_c")
                total_c = 0
                done_c = 0
            if total_c > 0:
                total = total_c
                done = done_c
                success = int(counters_from_summary.get("success") or success)
                failed = int(counters_from_summary.get("failed") or failed)
                try:
                    # 若脚本已经写入 progress，优先使用；否则按 done/total 计算
                    progress_val = counters_from_summary.get("progress")
                    if progress_val is None:
                        progress_val = summary.get("progress")
                    if progress_val is not None:
                        percent = max(0, min(100, int(float(progress_val))))
                    else:
                        percent = min(100, int((done / total) * 100))
                except Exception:  # noqa: BLE001
                    logging.getLogger(__name__).debug("progress parse fallback: percent")
                    percent = min(100, int((done / total) * 100)) if total > 0 else 0

    log_rows = _fetchall(
        """
        SELECT message
          FROM market.ingestion_logs
         WHERE job_id=%s
         ORDER BY ts DESC
         LIMIT 5
        """,
        (job_id,),
    )
    logs = [str(r.get("message")) for r in (log_rows or []) if r.get("message") is not None]

    error_rows = _fetchall(
        """
        SELECT e.run_id, e.ts_code, e.message, e.detail
          FROM market.ingestion_errors e
          JOIN market.ingestion_runs r ON r.run_id = e.run_id
         WHERE r.params->>'job_id' = %s
         ORDER BY e.run_id, e.ts_code
         LIMIT 20
        """,
        (str(job_id),),
    )
    error_samples: List[Dict[str, Any]] = []
    for r in error_rows or []:
        error_samples.append(
            {
                "run_id": str(r.get("run_id")),
                "ts_code": r.get("ts_code"),
                "message": r.get("message"),
                "detail": r.get("detail"),
            }
        )

    stats = summary.get("stats") or {}
    inserted_rows = int(summary.get("inserted_rows") or summary.get("rows") or stats.get("inserted_rows") or 0)

    # 优先合并脚本写入的 counters，以便展示更准确的统计
    counters_from_summary = summary.get("counters") or {}
    counters = {
        "total": counters_from_summary.get("total", total),
        "done": counters_from_summary.get("done", done),
        "running": counters_from_summary.get("running", running),
        "pending": counters_from_summary.get("pending", pending),
        "failed": counters_from_summary.get("failed", failed),
        "success": counters_from_summary.get("success", success),
        "inserted_rows": counters_from_summary.get("inserted_rows", inserted_rows),
        "success_codes": int(summary.get("success_codes") or stats.get("success_codes") or 0),
    }

    dataset = _infer_dataset(summary)
    mode = _infer_mode(job.get("job_type"), summary)
    source = _infer_source(dataset)
    date_range = _infer_date_range(summary)

    meta = {
        "dataset": dataset,
        "mode": mode,
        "type": job.get("job_type"),
        "source": source,
        "start_date": date_range["start_date"],
        "end_date": date_range["end_date"],
        # 直接透传常见过滤条件，便于前端展示更详细的任务说明
        "exchanges": summary.get("exchanges"),
        "freq_minutes": summary.get("freq_minutes"),
        "symbols_scope": summary.get("symbols_scope"),
    }

    return {
        "job_id": str(job.get("job_id")),
        "job_type": job.get("job_type"),
        "status": job.get("status"),
        "created_at": _isoformat(job.get("created_at")),
        "started_at": _isoformat(job.get("started_at")),
        "finished_at": _isoformat(job.get("finished_at")),
        "summary": summary,
        "progress": percent,
        "counters": counters,
        "logs": logs,
        "error_samples": error_samples,
        "meta": meta,
    }


def _batch_job_statuses(job_ids: List[uuid.UUID]) -> List[Dict[str, Any]]:
    """批量获取多个 job 的状态 — 替代逐个调用 _job_status() 的 N+1 模式。

    将 N×5 次独立 DB 连接合并为单连接 5 次查询，
    50 个 job 从 ~250 次 DB 连接降至 1 次。
    """
    if not job_ids:
        return []

    id_strs = [str(jid) for jid in job_ids]
    ph = ",".join(["%s"] * len(id_strs))
    id_tuple = tuple(id_strs)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Q1: 全部 job 主信息
            cur.execute(
                f"SELECT job_id, job_type, status, created_at, started_at, finished_at, summary"
                f" FROM market.ingestion_jobs WHERE job_id IN ({ph})",
                id_tuple,
            )
            cols = [c[0] for c in cur.description]
            jobs_raw = [dict(zip(cols, r)) for r in cur.fetchall()]
            job_map = {str(j["job_id"]): j for j in jobs_raw}

            # Q2: task 统计 (按 job_id + status 分组)
            cur.execute(
                f"SELECT job_id::text AS jid, status, COUNT(*) AS cnt"
                f" FROM market.ingestion_job_tasks"
                f" WHERE job_id IN ({ph})"
                f" GROUP BY job_id, status",
                id_tuple,
            )
            task_cols = [c[0] for c in cur.description]
            task_rows = [dict(zip(task_cols, r)) for r in cur.fetchall()]
            task_map: Dict[str, Dict[str, int]] = {}
            for r in task_rows:
                task_map.setdefault(r["jid"], {})[(r["status"] or "").lower()] = int(r["cnt"])

            # Q3: avg progress (用于 tasks 全部 running 但无 done 的情况)
            cur.execute(
                f"SELECT job_id::text AS jid, COALESCE(AVG(progress), 0) AS avg_progress"
                f" FROM market.ingestion_job_tasks"
                f" WHERE job_id IN ({ph})"
                f" GROUP BY job_id",
                id_tuple,
            )
            avg_cols = [c[0] for c in cur.description]
            avg_rows = [dict(zip(avg_cols, r)) for r in cur.fetchall()]
            avg_progress_map = {r["jid"]: float(r["avg_progress"] or 0) for r in avg_rows}

            # Q4: logs (每个 job 最新 5 条)
            cur.execute(
                f"SELECT jid, message FROM ("
                f"  SELECT job_id::text AS jid, message,"
                f"    ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY ts DESC) AS rn"
                f"  FROM market.ingestion_logs"
                f"  WHERE job_id IN ({ph})"
                f") sub WHERE rn <= 5",
                id_tuple,
            )
            log_cols = [c[0] for c in cur.description]
            log_rows = [dict(zip(log_cols, r)) for r in cur.fetchall()]
            log_map: Dict[str, List[str]] = {}
            for r in log_rows:
                log_map.setdefault(r["jid"], []).append(str(r["message"]))

            # Q5: errors (每个 job 最多 20 条)
            cur.execute(
                f"SELECT run_id, ts_code, message, detail, jid FROM ("
                f"  SELECT e.run_id, e.ts_code, e.message, e.detail,"
                f"    r.params->>'job_id' AS jid,"
                f"    ROW_NUMBER() OVER (PARTITION BY r.params->>'job_id' ORDER BY e.run_id, e.ts_code) AS rn"
                f"  FROM market.ingestion_errors e"
                f"  JOIN market.ingestion_runs r ON r.run_id = e.run_id"
                f"  WHERE r.params->>'job_id' IN ({ph})"
                f") sub WHERE rn <= 20",
                id_tuple,
            )
            err_cols = [c[0] for c in cur.description]
            err_rows = [dict(zip(err_cols, r)) for r in cur.fetchall()]
            error_map: Dict[str, List[Dict]] = {}
            for r in err_rows:
                error_map.setdefault(r["jid"], []).append({
                    "run_id": str(r["run_id"]),
                    "ts_code": r["ts_code"],
                    "message": r["message"],
                    "detail": r["detail"],
                })

    # 组装结果 (保持与 _job_status() 完全一致的输出格式)
    items = []
    for jid_str in id_strs:
        job = job_map.get(jid_str)
        if not job:
            continue
        summary = _json_load(job.get("summary")) or {}
        stats_by_status = task_map.get(jid_str, {})

        # --- 计算 counters & percent (与 _job_status 逻辑完全一致) ---
        total = sum(stats_by_status.values())
        success = stats_by_status.get("success", 0)
        failed = stats_by_status.get("failed", 0)
        running = stats_by_status.get("running", 0)
        pending = stats_by_status.get("queued", 0) + stats_by_status.get("pending", 0)
        done = success + failed

        percent = 0
        if total > 0:
            if done > 0:
                percent = min(100, int((done / total) * 100))
            else:
                percent = max(0, min(100, int(avg_progress_map.get(jid_str, 0))))
        else:
            stats = summary.get("stats") or {}
            total_codes = int(summary.get("total_codes") or stats.get("total_codes") or 0)
            success_codes = int(summary.get("success_codes") or stats.get("success_codes") or 0)
            failed_codes = int(summary.get("failed_codes") or stats.get("failed_codes") or 0)
            total_days = int(summary.get("total_days") or 0)
            done_days = int(summary.get("done_days") or 0)
            if total_codes > 0:
                percent = min(100, int(((success_codes + failed_codes) / total_codes) * 100))
                total = total_codes
                done = success_codes + failed_codes
                success = success_codes
                failed = failed_codes
            elif total_days > 0:
                percent = min(100, int((done_days / total_days) * 100))
                total = total_days
                done = done_days
            else:
                counters_from_summary = summary.get("counters") or {}
                try:
                    total_c = int(counters_from_summary.get("total") or 0)
                    done_c = int(counters_from_summary.get("done") or 0)
                except Exception:  # noqa: BLE001
                    logging.getLogger(__name__).debug("progress parse fallback: total_c")
                    total_c = 0
                    done_c = 0
                if total_c > 0:
                    total = total_c
                    done = done_c
                    success = int(counters_from_summary.get("success") or success)
                    failed = int(counters_from_summary.get("failed") or failed)
                    try:
                        progress_val = counters_from_summary.get("progress")
                        if progress_val is None:
                            progress_val = summary.get("progress")
                        if progress_val is not None:
                            percent = max(0, min(100, int(float(progress_val))))
                        else:
                            percent = min(100, int((done / total) * 100))
                    except Exception:  # noqa: BLE001
                        logging.getLogger(__name__).debug("progress parse fallback: percent")
                        percent = min(100, int((done / total) * 100)) if total > 0 else 0

        inserted_rows = int(
            summary.get("inserted_rows") or (summary.get("stats") or {}).get("inserted_rows") or 0
        )
        counters_from_summary = summary.get("counters") or {}
        counters = {
            "total": counters_from_summary.get("total", total),
            "done": counters_from_summary.get("done", done),
            "running": counters_from_summary.get("running", running),
            "pending": counters_from_summary.get("pending", pending),
            "failed": counters_from_summary.get("failed", failed),
            "success": counters_from_summary.get("success", success),
            "inserted_rows": counters_from_summary.get("inserted_rows", inserted_rows),
            "success_codes": int(
                summary.get("success_codes") or (summary.get("stats") or {}).get("success_codes") or 0
            ),
        }

        dataset = _infer_dataset(summary)
        mode = _infer_mode(job.get("job_type"), summary)
        source = _infer_source(dataset)
        date_range = _infer_date_range(summary)
        meta = {
            "dataset": dataset,
            "mode": mode,
            "type": job.get("job_type"),
            "source": source,
            "start_date": date_range["start_date"],
            "end_date": date_range["end_date"],
            "exchanges": summary.get("exchanges"),
            "freq_minutes": summary.get("freq_minutes"),
            "symbols_scope": summary.get("symbols_scope"),
        }

        items.append({
            "job_id": str(job.get("job_id")),
            "job_type": job.get("job_type"),
            "status": job.get("status"),
            "created_at": _isoformat(job.get("created_at")),
            "started_at": _isoformat(job.get("started_at")),
            "finished_at": _isoformat(job.get("finished_at")),
            "summary": summary,
            "progress": percent,
            "counters": counters,
            "logs": log_map.get(jid_str, []),
            "error_samples": error_map.get(jid_str, []),
            "meta": meta,
        })

    return items


def _upsert_ingestion_schedule_entry(
    dataset: str,
    mode: str,
    frequency: str,
    enabled: bool = True,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT schedule_id
          FROM market.ingestion_schedules
         WHERE dataset=%s AND mode=%s
        """,
        (dataset, mode),
    )
    schedule_id = rows[0]["schedule_id"] if rows else uuid.uuid4()
    sql = """
        INSERT INTO market.ingestion_schedules (
            schedule_id, dataset, mode, enabled, frequency, options, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (schedule_id)
        DO UPDATE SET enabled=EXCLUDED.enabled,
                      frequency=EXCLUDED.frequency,
                      options=EXCLUDED.options,
                      dataset=EXCLUDED.dataset,
                      mode=EXCLUDED.mode,
                      updated_at=NOW()
    """
    _execute(
        sql,
        (
            schedule_id,
            dataset,
            mode,
            enabled,
            frequency,
            _json_dump(options or {}),
        ),
    )
    return _ensure_ingestion_schedule(schedule_id)


def _ensure_default_ingestion_schedules() -> List[Dict[str, Any]]:
    defaults = [
        # 说明：
        # - kline_daily_raw / kline_minute_raw 的初始化和增量
        #   已统一切换为 Go 实现（init: /api/ingestion/init，incremental: /api/ingestion/incremental），
        #   不再通过 Python 调度器执行，因此这里不再为它们创建默认 schedule，
        #   以避免误触发 Python 版脚本。
        ("stock_moneyflow_ts", "incremental", "daily", True, {}),
        ("kline_weekly", "incremental", "daily", True, {}),
        ("anns_metadata", "incremental", "1h", True, _anns_metadata_incremental_options()),
    ]
    items: List[Dict[str, Any]] = []
    for ds, md, freq, en, opts in defaults:
        items.append(_upsert_ingestion_schedule_entry(ds, md, freq, en, opts))
    scheduler.refresh_schedules()
    return items


@router.post("/testing/run")
def trigger_testing_run(payload: TestingRunRequest) -> Dict[str, Any]:
    run_id = scheduler.run_testing_now(triggered_by=payload.triggered_by, options=payload.options)
    return {"run_id": str(run_id)}


@router.get("/testing/runs")
def list_testing_runs(limit: int = Query(20), offset: int = Query(0)) -> Dict[str, Any]:
    total_rows = _fetchall(
        """
        SELECT COUNT(*) AS cnt
          FROM market.testing_runs
        """,
    )
    total = int(total_rows[0].get("cnt") or 0) if total_rows else 0

    rows = _fetchall(
        """
        SELECT run_id, schedule_id, triggered_by, status, started_at, finished_at, summary, detail
          FROM market.testing_runs
         ORDER BY started_at DESC
         LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )
    return {
        "items": [_serialize_testing_run(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/testing/schedule")
def list_testing_schedules() -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT schedule_id, enabled, frequency, options, last_run_at, next_run_at,
               last_status, last_error, created_at, updated_at
          FROM market.testing_schedules
         ORDER BY created_at ASC
        """,
    )
    return {"items": [_serialize_schedule(row) for row in rows]}


@router.post("/testing/schedule")
def upsert_testing_schedule(payload: TestingScheduleUpsertRequest) -> Dict[str, Any]:
    schedule_id = payload.schedule_id or uuid.uuid4()
    sql = """
        INSERT INTO market.testing_schedules (
            schedule_id, enabled, frequency, options, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (schedule_id)
        DO UPDATE SET enabled=EXCLUDED.enabled,
                      frequency=EXCLUDED.frequency,
                      options=EXCLUDED.options,
                      updated_at=NOW()
    """
    _execute(sql, (schedule_id, payload.enabled, payload.frequency, _json_dump(payload.options)))
    scheduler.refresh_schedules()
    data = _ensure_testing_schedule(schedule_id)
    return _serialize_schedule(data)


@router.post("/testing/schedule/{schedule_id}/toggle")
def toggle_testing_schedule(
    payload: ToggleRequest,
    schedule_id: uuid.UUID = Path(..., description="Testing schedule identifier"),
) -> Dict[str, Any]:
    _ensure_testing_schedule(schedule_id)
    sql = """
        UPDATE market.testing_schedules
           SET enabled=%s, updated_at=NOW()
         WHERE schedule_id=%s
    """
    _execute(sql, (payload.enabled, schedule_id))
    scheduler.refresh_schedules()
    data = _ensure_testing_schedule(schedule_id)
    return _serialize_schedule(data)


@router.post("/testing/schedule/{schedule_id}/run")
def run_testing_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    data = _ensure_testing_schedule(schedule_id)
    run_id = scheduler.run_testing_for_schedule(schedule_id)
    data["last_status"] = "queued"
    return {"run_id": str(run_id), "schedule": _serialize_schedule(data)}


# ---------------------------------------------------------------------------
# Ingestion API endpoints（路径与 tdx_backend 完全一致）
# ---------------------------------------------------------------------------


@router.post("/ingestion/init")
def start_ingestion_init(payload: IngestionInitRequest) -> Dict[str, Any]:
    dataset = (payload.dataset or "").strip().lower()
    # 目前仅支持通过 Go 服务执行以下初始化：
    # - kline_minute_raw: 分钟线 RAW，全量 COPY 入库
    # - kline_daily_raw_go: 未复权日线 RAW（Go 直连版），全量 COPY 入库
    if dataset not in {"kline_minute_raw", "kline_daily_raw_go"}:
        raise HTTPException(status_code=400, detail="unsupported dataset for init")
    options = dict(payload.options or {})
    summary = {"datasets": [dataset], **options}
    job_id = _create_init_job(summary)

    # 对分钟线初始化：直接调用新的 TDX Go API，由 Go 负责高性能 COPY 入库
    if dataset == "kline_minute_raw":
        # 将前端传入的起始日期转换为 start_time（东八区），结束时间由 Go 端自行扩展到“最新可用”
        start_date_str = str(options.get("start_date") or "1990-01-01")
        try:
            start_date = dt.date.fromisoformat(start_date_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid start_date for minute init")

        tz = dt.timezone(dt.timedelta(hours=8))
        start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=tz)

        workers = int(options.get("workers") or 1)
        truncate_before = bool(options.get("truncate"))
        max_rows_per_chunk = int(options.get("max_rows_per_chunk") or 500_000)
        codes = options.get("codes") or []

        go_payload: Dict[str, Any] = {
            "job_id": str(job_id),
            "codes": codes,
            "start_time": start_dt.isoformat(),
            "workers": workers,
            "options": {
                "truncate_before": truncate_before,
                "max_rows_per_chunk": max_rows_per_chunk,
                "source": "tdx_api",
            },
        }

        base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
        url = f"{base}/api/tasks/ingest-minute-raw-init"

        try:
            resp = requests.post(url, json=go_payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            # 若任务创建失败，直接将 job 标记为 failed，避免长时间停留在 queued
            err_summary = {**summary, "error": str(exc), "phase": "create_go_task"}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET status='failed', finished_at=NOW(), summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(err_summary), job_id),
            )
            raise HTTPException(status_code=502, detail=f"failed to start TDX minute init task: {exc}")

        if isinstance(data, dict) and data.get("code") not in (0, None):
            msg = str(data)
            err_summary = {**summary, "error": msg, "phase": "create_go_task"}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET status='failed', finished_at=NOW(), summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(err_summary), job_id),
            )
            raise HTTPException(status_code=502, detail=f"TDX minute init task error: {msg}")

        task_id: Optional[str] = None
        payload_data = data.get("data") if isinstance(data, dict) else None
        if isinstance(payload_data, dict):
            raw_tid = payload_data.get("task_id")
            if raw_tid is not None:
                task_id = str(raw_tid)

        # 将 Go 侧 task_id 持久化到 ingestion_jobs.summary 中，方便后续在任务监视器中执行取消操作
        if task_id is not None:
            summary_with_task = {**summary, "go_task_id": task_id}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(summary_with_task), job_id),
            )

        # Go 任务会自行更新 ingestion_jobs.status / summary 以及 ingestion_logs
        return {"job_id": str(job_id), "task_id": task_id}

    # 未复权日线（Go 直连版）初始化：调用新的 TDX Go API，将结果 COPY 至 kline_daily_raw
    if dataset == "kline_daily_raw_go":
        start_date_str = str(options.get("start_date") or "1990-01-01")
        try:
            start_date = dt.date.fromisoformat(start_date_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid start_date for daily raw init")

        tz = dt.timezone(dt.timedelta(hours=8))
        start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=tz)

        workers = int(options.get("workers") or 1)
        truncate_before = bool(options.get("truncate"))
        max_rows_per_chunk = int(options.get("max_rows_per_chunk") or 500_000)
        codes = options.get("codes") or []

        go_payload: Dict[str, Any] = {
            "job_id": str(job_id),
            "codes": codes,
            "start_time": start_dt.isoformat(),
            "workers": workers,
            "options": {
                "truncate_before": truncate_before,
                "max_rows_per_chunk": max_rows_per_chunk,
                "source": "tdx_api",
            },
        }

        base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
        url = f"{base}/api/tasks/ingest-daily-raw-init"

        try:
            resp = requests.post(url, json=go_payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            err_summary = {**summary, "error": str(exc), "phase": "create_go_task"}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET status='failed', finished_at=NOW(), summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(err_summary), job_id),
            )
            raise HTTPException(status_code=502, detail=f"failed to start TDX daily raw init task: {exc}")

        if isinstance(data, dict) and data.get("code") not in (0, None):
            msg = str(data)
            err_summary = {**summary, "error": msg, "phase": "create_go_task"}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET status='failed', finished_at=NOW(), summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(err_summary), job_id),
            )
            raise HTTPException(status_code=502, detail=f"TDX daily raw init task error: {msg}")

        task_id: Optional[str] = None
        payload_data = data.get("data") if isinstance(data, dict) else None
        if isinstance(payload_data, dict):
            raw_tid = payload_data.get("task_id")
            if raw_tid is not None:
                task_id = str(raw_tid)

        if task_id is not None:
            summary_with_task = {**summary, "go_task_id": task_id}
            _execute(
                """
                UPDATE market.ingestion_jobs
                   SET summary=%s
                 WHERE job_id=%s
                """,
                (_json_dump(summary_with_task), job_id),
            )

        return {"job_id": str(job_id), "task_id": task_id}

    # 其它历史 init 任务路径（如旧版 kline_daily_raw Python 版）已关闭。
    raise HTTPException(status_code=400, detail="init path not implemented for this dataset")


@router.get("/ingestion/job/{job_id}")
def get_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    return _job_status(job_id)


@router.post("/ingestion/job/{job_id}/cancel")
def cancel_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    """取消正在运行的 Go 驱动的 ingestion 任务（目前主要用于 kline_minute_raw init）。

    - 从 ingestion_jobs.summary 中读取 go_task_id
    - 调用 TDX Go API /api/tasks/{go_task_id}/cancel
    - 将 ingestion_jobs.status 标记为 cancelled
    """

    row = _fetchone(
        "SELECT status, summary FROM market.ingestion_jobs WHERE job_id=%s",
        (job_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="ingestion job not found")

    status = str(row.get("status") or "").lower()
    if status in {"success", "failed", "cancelled", "canceled"}:
        raise HTTPException(status_code=400, detail="job already finished")

    summary_raw = row.get("summary") or {}
    try:
        summary_obj = json.loads(summary_raw) if isinstance(summary_raw, str) else dict(summary_raw)
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("cancel_ingestion_job: failed to parse summary JSON for job %s: %s", job_id, exc)
        summary_obj = {}

    go_task_id = summary_obj.get("go_task_id")
    if not go_task_id:
        raise HTTPException(status_code=400, detail="go_task_id not found for this job")

    base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
    url = f"{base}/api/tasks/{go_task_id}/cancel"

    try:
        resp = requests.post(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("code") not in (0, None):
            raise HTTPException(status_code=502, detail=f"TDX cancel task error: {data}")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"failed to cancel Go task: {exc}")

    # 将作业状态标记为 cancelled
    summary_obj["cancelled"] = True
    _execute(
        """
        UPDATE market.ingestion_jobs
           SET status='cancelled', finished_at=NOW(), summary=%s
         WHERE job_id=%s
        """,
        (_json_dump(summary_obj), job_id),
    )

    return {"job_id": str(job_id), "go_task_id": go_task_id, "status": "cancelled"}


@router.get("/ingestion/jobs")
def list_ingestion_jobs(limit: int = Query(50), active_only: bool = Query(False)) -> Dict[str, Any]:
    base_sql = (
        "SELECT job_id FROM market.ingestion_jobs "
        + ("WHERE status IN ('running','queued','pending') " if active_only else "")
        + "ORDER BY created_at DESC LIMIT %s"
    )
    rows = _fetchall(base_sql, (limit,))
    job_ids = [uuid.UUID(str(r["job_id"])) for r in rows]
    items = _batch_job_statuses(job_ids)
    return {"items": items}


@router.post("/ingestion/schedule/defaults")
def create_default_ingestion_schedules() -> Dict[str, Any]:
    items = _ensure_default_ingestion_schedules()
    return {"items": [_serialize_ingestion_schedule(row) for row in items]}


class BatchScheduleItem(BaseModel):
    dataset: str
    mode: str = "incremental"
    frequency: str = "daily"
    enabled: bool = True
    at: Optional[str] = None  # HH:MM format for daily schedules
    workers: Optional[int] = None  # concurrency for TDX datasets


class BatchCreateSchedulesRequest(BaseModel):
    items: List[BatchScheduleItem]


def _suspend_d_refresh_options() -> Dict[str, Any]:
    return {
        "date_strategy": "current_and_next_trading_day",
        "skip_auto_range": True,
    }


def _anns_metadata_incremental_options() -> Dict[str, Any]:
    return {
        "lookback_days": 2,
        "source": "eastmoney",
        "workers": 1,
        "request_sleep": 0.05,
        "skip_auto_range": True,
    }


@router.post("/ingestion/schedule/batch-create")
def batch_create_ingestion_schedules(payload: BatchCreateSchedulesRequest) -> Dict[str, Any]:
    """Batch create/update daily ingestion schedules for multiple datasets."""
    results: List[Dict[str, Any]] = []
    for item in payload.items:
        if item.mode not in SUPPORTED_INGESTION_MODES:
            results.append({"dataset": item.dataset, "error": f"invalid mode: {item.mode}"})
            continue
        options: Dict[str, Any] = {}
        if item.at:
            options["at"] = item.at
        if item.workers and item.workers > 0:
            options["workers"] = item.workers
        frequency = item.frequency
        if item.dataset == "suspend_d":
            frequency = "1h"
            options.update(_suspend_d_refresh_options())
            options.pop("at", None)
        elif item.dataset == "anns_metadata":
            frequency = "1h"
            options.update(_anns_metadata_incremental_options())
            options.pop("at", None)

        # upsert: find existing or create new
        rows = _fetchall(
            "SELECT schedule_id FROM market.ingestion_schedules WHERE dataset=%s AND mode=%s",
            (item.dataset, item.mode),
        )
        schedule_id = rows[0]["schedule_id"] if rows else uuid.uuid4()
        _execute(
            """INSERT INTO market.ingestion_schedules
                   (schedule_id, dataset, mode, enabled, frequency, options, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
               ON CONFLICT (schedule_id)
               DO UPDATE SET enabled=EXCLUDED.enabled, frequency=EXCLUDED.frequency,
                             options=EXCLUDED.options, updated_at=NOW()""",
            (schedule_id, item.dataset, item.mode, item.enabled, frequency, _json_dump(options)),
        )
        data = _ensure_ingestion_schedule(schedule_id)
        results.append(_serialize_ingestion_schedule(data))

    scheduler.refresh_schedules()
    return {"created": len(results), "items": results}


# 每日定时调度目标数据集
_DAILY_PRESETS = [
    ("kline_daily_raw", "incremental"),
    ("kline_minute_raw", "incremental"),
    ("daily_basic", "incremental"),
    ("stock_basic", "init"),
    ("stock_moneyflow_ts", "incremental"),
    ("adj_factor", "incremental"),
    ("index_daily", "incremental"),
    ("stock_st", "incremental"),
    ("stock_st_events", "incremental"),
    ("bak_basic", "incremental"),
    ("stk_limit", "incremental"),
    ("suspend_d", "incremental"),
    ("anns_metadata", "incremental"),
    ("tushare_forecast_raw", "incremental"),
    ("tushare_express_raw", "incremental"),
    ("tushare_fina_indicator_raw", "incremental"),
    ("margin_detail", "incremental"),
    ("sw_sector", "incremental"),
    ("sector_data", "incremental"),
    ("cyq_perf", "incremental"),
]


def _create_sw_sector_jobs(
    mode: str, options: Dict[str, Any], triggered_by: str,
) -> tuple:
    """Create 3 child jobs for sw_sector composite sync.

    Returns (sub_job_ids: List[str], run_id: str).
    Each child job appears individually in the task monitor.
    """
    sub_specs = [
        ("sw_index_classify", "init"),
        ("sw_index_member", "init"),
        ("sw_daily", mode),
    ]
    sub_job_ids: List[str] = []
    for ds_name, ds_mode in sub_specs:
        summary: Dict[str, Any] = {
            "dataset": ds_name, "mode": ds_mode,
            "triggered_by": triggered_by, "composite": "sw_sector",
        }
        for k in ("start_date", "end_date", "exchanges"):
            if k in options:
                summary[k] = options[k]
        jid = _create_job(ds_mode, summary)
        sub_job_ids.append(str(jid))
    opts = dict(options)
    opts["sub_job_ids"] = sub_job_ids
    run_id = scheduler.run_ingestion_now(
        dataset="sw_sector", mode=mode, triggered_by=triggered_by, options=opts,
    )
    return sub_job_ids, str(run_id)


class RunSinglePresetRequest(BaseModel):
    dataset: str
    workers: Optional[int] = None


def _run_preset(dataset: str, mode: str, triggered_by: str, workers: Optional[int] = None) -> Dict[str, Any]:
    if dataset == "sw_sector":
        sub_job_ids, run_id = _create_sw_sector_jobs(mode, {}, triggered_by)
        return {"dataset": dataset, "mode": mode, "job_id": sub_job_ids[-1],
                "run_id": run_id, "sub_job_ids": sub_job_ids}
    summary: Dict[str, Any] = {"dataset": dataset, "mode": mode, "triggered_by": triggered_by}
    if workers:
        summary["workers"] = workers
    job_id = _create_job(mode, summary)
    opts: Dict[str, Any] = {"job_id": str(job_id)}
    if workers:
        opts["workers"] = workers
    if dataset == "suspend_d":
        opts.update(_suspend_d_refresh_options())
    elif dataset == "anns_metadata":
        opts.update(_anns_metadata_incremental_options())
    run_id = scheduler.run_ingestion_now(
        dataset=dataset, mode=mode, triggered_by=triggered_by, options=opts,
    )
    return {"dataset": dataset, "mode": mode, "job_id": str(job_id), "run_id": str(run_id)}


@router.post("/ingestion/schedule/run-single-preset")
def run_single_preset(req: RunSinglePresetRequest) -> Dict[str, Any]:
    preset_map = {ds: m for ds, m in _DAILY_PRESETS}
    mode = preset_map.get(req.dataset)
    if not mode:
        raise HTTPException(status_code=400, detail=f"Unknown preset dataset: {req.dataset}")
    return _run_preset(req.dataset, mode, "preset-run-single", req.workers)


@router.post("/ingestion/schedule/run-all-presets")
def run_all_preset_schedules() -> Dict[str, Any]:
    jobs = [_run_preset(ds, m, "preset-run-all") for ds, m in _DAILY_PRESETS]
    return {"triggered": len(jobs), "jobs": jobs}


@router.get("/ingestion/schedule/preset-stats")
def get_preset_stats() -> Dict[str, Any]:
    """Batch query latest data dates for the 9 preset datasets."""
    results: List[Dict[str, Any]] = []
    for dataset, mode in _DAILY_PRESETS:
        entry: Dict[str, Any] = {"dataset": dataset, "mode": mode}
        # sw_sector composite: use sw_daily stats as representative
        stats_key = "sw_daily" if dataset == "sw_sector" else dataset
        # query data_stats_config for table/column
        cfg = _fetchone(
            "SELECT table_name, date_column FROM market.data_stats_config WHERE data_kind = %s AND enabled",
            (stats_key,),
        )
        if not cfg:
            entry["current_max_date"] = None
            entry["ready_date"] = None
            entry["readiness_source"] = "dataset_date_refresh_audit"
            entry["audit_missing"] = True
            entry["stats_source"] = "none"
            entry["error"] = "no config"
            results.append(entry)
            continue
        audit_row = _fetchone(
            """
            SELECT trade_date, refreshed_at, row_count, written_rows,
                   expected_rows, coverage_ratio, quality_status,
                   failure_category, data_source
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s AND status = 'success'
            ORDER BY trade_date DESC, refreshed_at DESC
            LIMIT 1
            """,
            (stats_key,),
        )
        if audit_row:
            audit_trade_date = audit_row.get("trade_date")
            entry["current_max_date"] = audit_trade_date.isoformat() if audit_trade_date else None
            entry["ready_date"] = entry["current_max_date"]
            entry["stats_source"] = "refresh_audit"
            entry["readiness_source"] = "dataset_date_refresh_audit"
            entry["audit_missing"] = False
            entry["audit_refreshed_at"] = _isoformat(audit_row.get("refreshed_at"))
            entry["row_count"] = audit_row.get("row_count")
            entry["written_rows"] = audit_row.get("written_rows")
            entry["expected_rows"] = audit_row.get("expected_rows")
            entry["coverage_ratio"] = float(audit_row["coverage_ratio"]) if audit_row.get("coverage_ratio") is not None else None
            entry["quality_status"] = audit_row.get("quality_status")
            entry["failure_category"] = audit_row.get("failure_category")
            entry["data_source"] = audit_row.get("data_source")
            results.append(entry)
            continue
        table_name = str(cfg.get("table_name") or "").strip()
        date_column = str(cfg.get("date_column") or "trade_date").strip()
        # query max date
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '10s'")
                    cur.execute(f"SELECT MAX({date_column})::date FROM {table_name}")
                    row = cur.fetchone()
                    mx = row[0] if row else None
            entry["current_max_date"] = None
            entry["ready_date"] = None
            entry["physical_max_date"] = mx.isoformat() if mx else None
            entry["stats_source"] = "physical_fallback_display_only"
            entry["readiness_source"] = "dataset_date_refresh_audit"
            entry["audit_missing"] = True
            entry["cache_state"] = "audit_missing"
        except Exception as exc:
            entry["current_max_date"] = None
            entry["ready_date"] = None
            entry["readiness_source"] = "dataset_date_refresh_audit"
            entry["audit_missing"] = True
            entry["error"] = str(exc)
        results.append(entry)
    return {"items": results}


@router.get("/ingestion/schedule/preset-daily-status")
def get_preset_daily_status() -> Dict[str, Any]:
    """查询每个 preset 数据集当天的执行状态。

    返回每个 dataset 的状态: null(未执行) / queued / running / success / failed
    sw_sector 额外返回 3 个子任务的独立状态。
    """
    results: Dict[str, Any] = {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            for dataset, mode in _DAILY_PRESETS:
                if dataset == "sw_sector":
                    # sw_sector: 查 3 个子任务
                    for sub_ds in ["sw_index_classify", "sw_index_member", "sw_daily"]:
                        cur.execute("""
                            SELECT status, created_at, finished_at
                            FROM market.ingestion_jobs
                            WHERE summary->>'dataset' = %s
                              AND created_at::date = CURRENT_DATE
                            ORDER BY created_at DESC LIMIT 1
                        """, (sub_ds,))
                        row = cur.fetchone()
                        if row:
                            results[sub_ds] = {
                                "status": row[0],
                                "created_at": _isoformat(row[1]),
                                "finished_at": _isoformat(row[2]),
                            }
                    # sw_sector 本身取 3 个子任务的综合状态
                    sub_statuses = [results.get(s, {}).get("status") for s in ["sw_index_classify", "sw_index_member", "sw_daily"]]
                    if any(s == "failed" for s in sub_statuses):
                        results["sw_sector"] = {"status": "failed"}
                    elif any(s == "running" for s in sub_statuses):
                        results["sw_sector"] = {"status": "running"}
                    elif any(s == "queued" for s in sub_statuses):
                        results["sw_sector"] = {"status": "queued"}
                    elif all(s == "success" for s in sub_statuses):
                        results["sw_sector"] = {"status": "success"}
                    # else: 部分执行部分未执行, 不设置
                else:
                    cur.execute("""
                        SELECT status, created_at, finished_at
                        FROM market.ingestion_jobs
                        WHERE summary->>'dataset' = %s
                          AND created_at::date = CURRENT_DATE
                        ORDER BY created_at DESC LIMIT 1
                    """, (dataset,))
                    row = cur.fetchone()
                    if row:
                        results[dataset] = {
                            "status": row[0],
                            "created_at": _isoformat(row[1]),
                            "finished_at": _isoformat(row[2]),
                        }

    return {"items": results}


@router.get("/ingestion/tushare/datasets")
def list_tushare_datasets() -> Dict[str, Any]:
    """Return all engine-managed DatasetSpec definitions with latest sync status."""
    specs = []
    for name, spec in DATASET_REGISTRY.items():
        row = _fetchone(
            """SELECT job_id, status, finished_at, summary
                 FROM market.ingestion_jobs
                WHERE summary::text LIKE %s
                ORDER BY created_at DESC LIMIT 1""",
            (f'%"dataset": "{name}"%',),
        )
        entry: Dict[str, Any] = {
            "name": name,
            "tushare_api": spec.tushare_api,
            "target_table": spec.target_table,
            "query_mode": spec.query_mode.value,
            "supports_incremental": spec.supports_incremental,
        }
        if row:
            entry["last_job_id"] = str(row["job_id"])
            entry["last_status"] = row["status"]
            entry["last_finished"] = _isoformat(row.get("finished_at"))
        specs.append(entry)
    return {"datasets": specs}


@router.post("/ingestion/tushare/sync-all")
def tushare_sync_all() -> Dict[str, Any]:
    """Trigger incremental sync for all engine-managed datasets."""
    jobs: List[Dict[str, Any]] = []
    for name, spec in DATASET_REGISTRY.items():
        mode = "init" if not spec.supports_incremental else "incremental"
        summary = {"dataset": name, "mode": mode}
        job_id = _create_job(mode, summary)
        run_id = scheduler.run_ingestion_now(
            dataset=name, mode=mode,
            triggered_by="api-sync-all",
            options={"job_id": str(job_id)},
        )
        jobs.append({"dataset": name, "mode": mode,
                      "job_id": str(job_id), "run_id": str(run_id)})
    return {"triggered": len(jobs), "jobs": jobs}


@router.post("/ingestion/run")
def trigger_ingestion_run(payload: IngestionRunRequest) -> Dict[str, Any]:
    payload.validate_mode()
    dataset = (payload.dataset or "").strip().lower()
    mode = (payload.mode or "").strip().lower()
    options = dict(payload.options or {})

    # 前复权日线、未复权日线、分钟线的初始化和增量
    # 已统一为 Go 端实现：
    #   - init:        /api/ingestion/init
    #   - incremental: /api/ingestion/incremental
    # 这里显式禁止通过 Python 调度入口 /api/ingestion/run 触发，
    # 避免应用误走 Python 版脚本导致行为不一致或无法取消任务。
    if dataset in {"kline_daily_raw", "kline_minute_raw"} and mode in {"init", "incremental"}:
        raise HTTPException(
            status_code=400,
            detail="dataset must be ingested via Go APIs (use /api/ingestion/init or /api/ingestion/incremental)",
        )

    # 业务校验：Tushare 相关数据集
    if dataset == "stock_basic":
        if mode != "init":
            raise HTTPException(status_code=400, detail="stock_basic only supports init mode")
    elif dataset == "index_basic":
        # 指数基础信息 index_basic：仅支持 init 模式，不需要起止日期参数
        if mode != "init":
            raise HTTPException(status_code=400, detail="index_basic only supports init mode")
    elif dataset == "stock_st":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stock_st init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stock_st init requires end_date")
    elif dataset == "stock_st_events":
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stock_st_events init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stock_st_events init requires end_date")
    elif dataset == "bak_basic":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="bak_basic init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="bak_basic init requires end_date")
    elif dataset == "daily_basic":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="daily_basic init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="daily_basic init requires end_date")
    elif dataset == "stk_limit":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stk_limit init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stk_limit init requires end_date")
    elif dataset == "suspend_d":
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="suspend_d init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="suspend_d init requires end_date")
    elif dataset == "margin_detail":
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="margin_detail init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="margin_detail init requires end_date")
    elif dataset in FINANCIAL_EVENT_RAW_DATASETS:
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail=f"{dataset} init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail=f"{dataset} init requires end_date")
    elif dataset == "index_daily":
        # 指数日线行情 index_daily：
        # - init: 必须提供 start_date/end_date
        # - incremental: start_date/end_date 可选，由脚本内部根据历史数据自动推断起始位置
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="index_daily init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="index_daily init requires end_date")
    elif dataset == "anns_d":
        # 公告数据 anns_d：init 需要 start_date/end_date；incremental 时 start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="anns_d init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="anns_d init requires end_date")
    elif dataset == "anns_metadata":
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="anns_metadata init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="anns_metadata init requires end_date")
        if options.get("lookback_days") is not None:
            try:
                lookback_days = int(options.get("lookback_days"))
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=f"anns_metadata invalid lookback_days: {exc}")
            if lookback_days < 1 or lookback_days > 30:
                raise HTTPException(status_code=400, detail="anns_metadata lookback_days must be between 1 and 30")
            options["lookback_days"] = lookback_days
        if options.get("source") is not None:
            source = str(options.get("source")).strip().lower()
            if source not in {"eastmoney", "cninfo", "both"}:
                raise HTTPException(status_code=400, detail="anns_metadata source must be eastmoney, cninfo or both")
            options["source"] = source
    elif dataset == "anns_pdf":
        # 公告 PDF 下载任务：通过 download_anns_pdf.py 实现
        # 这里不强制参数，但可以对 limit 做一个简单的范围约束，避免一次性扫描过大规模
        raw_limit = options.get("limit")
        if raw_limit is not None:
            try:
                limit_val = int(raw_limit)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=f"anns_pdf invalid limit: {exc}")
            if limit_val <= 0 or limit_val > 5000:
                raise HTTPException(status_code=400, detail="anns_pdf limit must be between 1 and 5000")
            options["limit"] = limit_val
    elif dataset == "stock_moneyflow_ts":
        # init: 需要起止日期；incremental: start_date 可选；支持可选 truncate
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stock_moneyflow_ts init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stock_moneyflow_ts init requires end_date")
    elif dataset in {"cyq_perf", "cyq_chips"}:
        # cyq_perf / cyq_chips：init 需要 start_date/end_date；incremental 时 start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail=f"{dataset} init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail=f"{dataset} init requires end_date")
    elif dataset == "sw_index_classify":
        # 行业分类：仅 init（全量替换），无需日期参数
        if mode != "init":
            raise HTTPException(status_code=400, detail="sw_index_classify only supports init mode")
    elif dataset == "sw_index_member":
        # PIT 成分股映射：仅 init（全量替换），无需日期参数
        if mode != "init":
            raise HTTPException(status_code=400, detail="sw_index_member only supports init mode")
    elif dataset == "sw_daily":
        # 行业日线行情：init 需要 start_date/end_date；incremental 时自动推断
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="sw_daily init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="sw_daily init requires end_date")
    elif dataset == "sw_sector":
        # 复合数据集：classify(全量) + member(全量) + daily(按mode)
        # init 需要 start_date/end_date（用于 sw_daily 部分）
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="sw_sector init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="sw_sector init requires end_date")
        # 创建 3 个子 job（任务监视器分别显示）
        sub_job_ids, run_id = _create_sw_sector_jobs(mode, options, payload.triggered_by)
        return {"job_id": sub_job_ids[-1], "run_id": run_id, "sub_job_ids": sub_job_ids}
    elif dataset == "sector_data":
        # 后处理数据集：init 需要 start_date/end_date；incremental 自动推断日期范围
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="sector_data init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="sector_data init requires end_date")

    summary = {"dataset": payload.dataset, "mode": payload.mode, **(payload.options or {})}
    job_type = "init" if payload.mode == "init" else "incremental"
    job_id = _create_job(job_type, summary)
    options["job_id"] = str(job_id)

    # 为未复权日线增量任务提供默认并行度：当前端未显式传入 workers 时，使用一个适中的默认值。
    if payload.dataset == "kline_daily_raw" and payload.mode == "incremental" and "workers" not in options:
        options["workers"] = 4

    run_id = scheduler.run_ingestion_now(
        dataset=payload.dataset,
        mode=payload.mode,
        triggered_by=payload.triggered_by,
        options=options,
    )
    return {"job_id": str(job_id), "run_id": str(run_id)}


@router.get("/ingestion/schedule")
def list_ingestion_schedules() -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT s.schedule_id, s.dataset, s.mode, s.enabled, s.frequency, s.options,
               s.last_run_at, s.next_run_at, s.last_status, s.last_error,
               s.created_at, s.updated_at,
               j.summary AS last_job_summary
          FROM market.ingestion_schedules s
          LEFT JOIN LATERAL (
              SELECT summary
                FROM market.ingestion_jobs
               WHERE summary->>'schedule_id' = s.schedule_id::text
                  OR summary->>'dataset' = s.dataset
               ORDER BY created_at DESC
               LIMIT 1
          ) j ON TRUE
         ORDER BY s.dataset, s.mode
        """,
    )
    return {"items": [_serialize_ingestion_schedule(row) for row in rows]}


@router.post("/ingestion/schedule")
def upsert_ingestion_schedule(payload: IngestionScheduleUpsertRequest) -> Dict[str, Any]:
    payload.validate_mode()
    schedule_id = payload.schedule_id
    if schedule_id is None:
        rows = _fetchall(
            """
            SELECT schedule_id
              FROM market.ingestion_schedules
             WHERE dataset=%s AND mode=%s
            """,
            (payload.dataset, payload.mode),
        )
        schedule_id = uuid.uuid4() if not rows else rows[0]["schedule_id"]
    sql = """
        INSERT INTO market.ingestion_schedules (
            schedule_id, dataset, mode, enabled, frequency, options, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (schedule_id)
        DO UPDATE SET enabled=EXCLUDED.enabled,
                      frequency=EXCLUDED.frequency,
                      options=EXCLUDED.options,
                      dataset=EXCLUDED.dataset,
                      mode=EXCLUDED.mode,
                      updated_at=NOW()
    """
    _execute(
        sql,
        (
            schedule_id,
            payload.dataset,
            payload.mode,
            payload.enabled,
            payload.frequency,
            _json_dump(payload.options),
        ),
    )
    scheduler.refresh_schedules()
    data = _ensure_ingestion_schedule(schedule_id)
    return _serialize_ingestion_schedule(data)


@router.post("/ingestion/schedule/{schedule_id}/toggle")
def toggle_ingestion_schedule(
    payload: ToggleRequest,
    schedule_id: uuid.UUID = Path(..., description="Ingestion schedule identifier"),
) -> Dict[str, Any]:
    _ensure_ingestion_schedule(schedule_id)
    sql = """
        UPDATE market.ingestion_schedules
           SET enabled=%s, updated_at=NOW()
         WHERE schedule_id=%s
    """
    _execute(sql, (payload.enabled, schedule_id))
    scheduler.refresh_schedules()
    data = _ensure_ingestion_schedule(schedule_id)
    return _serialize_ingestion_schedule(data)


@router.post("/ingestion/schedule/{schedule_id}/run")
def run_ingestion_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    data = _ensure_ingestion_schedule(schedule_id)
    run_id = scheduler.run_ingestion_for_schedule(schedule_id, data["dataset"], data["mode"])
    data["last_status"] = "queued"
    return {"run_id": str(run_id), "schedule": _serialize_ingestion_schedule(data)}


@router.delete("/ingestion/schedule/{schedule_id}")
def delete_ingestion_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    """Delete a single ingestion schedule and remove it from in-memory scheduler.

    仅删除调度配置本身，不会删除历史任务或日志记录。
    """

    # Ensure it exists first (will raise 404 if not found)
    _ensure_ingestion_schedule(schedule_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM market.ingestion_schedules
                 WHERE schedule_id=%s
                """,
                (schedule_id,),
            )

    # Refresh in-memory schedules so background scheduler drops this job
    scheduler.refresh_schedules()

    return {"deleted": True, "schedule_id": str(schedule_id)}


@router.get("/ingestion/logs")
def list_ingestion_logs(
    limit: int = Query(50),
    job_id: Optional[uuid.UUID] = Query(None),
    offset: int = Query(0),
) -> Dict[str, Any]:
    if job_id is not None:
        total_rows = _fetchall(
            """
            SELECT COUNT(*) AS cnt
              FROM market.ingestion_logs
             WHERE job_id=%s
            """,
            (job_id,),
        )
        total = int(total_rows[0].get("cnt") or 0) if total_rows else 0

        rows = _fetchall(
            """
            SELECT l.job_id,
                   l.ts,
                   l.level,
                   l.message,
                   j.summary
              FROM market.ingestion_logs AS l
              LEFT JOIN market.ingestion_jobs AS j
                     ON j.job_id = l.job_id
             WHERE l.job_id=%s
             ORDER BY l.ts DESC
             LIMIT %s OFFSET %s
            """,
            (job_id, limit, offset),
        )
    else:
        total_rows = _fetchall(
            """
            SELECT COUNT(*) AS cnt
              FROM market.ingestion_logs
            """,
        )
        total = int(total_rows[0].get("cnt") or 0) if total_rows else 0

        rows = _fetchall(
            """
            SELECT l.job_id,
                   l.ts,
                   l.level,
                   l.message,
                   j.summary
              FROM market.ingestion_logs AS l
              LEFT JOIN market.ingestion_jobs AS j
                     ON j.job_id = l.job_id
             ORDER BY l.ts DESC
             LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
    return {
        "items": [_serialize_ingestion_log(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.delete("/ingestion/logs")
def bulk_delete_ingestion_logs(
    payload: BulkDeleteIngestionLogsRequest = Body(...),
) -> Dict[str, Any]:
    """Bulk delete ingestion logs by (job_id, ts) pairs or clear all.

    - 当 delete_all=True 时，直接清空 market.ingestion_logs 表；
    - 否则按 items 中提供的 (job_id, ts) 精确删除对应日志行。
    """

    deleted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if payload.delete_all:
                cur.execute("DELETE FROM market.ingestion_logs")
                deleted = cur.rowcount or 0
            else:
                for item in payload.items:
                    cur.execute(
                        "DELETE FROM market.ingestion_logs WHERE job_id=%s AND ts=%s",
                        (item.job_id, item.ts),
                    )
                    deleted += cur.rowcount or 0

    return {"deleted": int(deleted)}


@router.delete("/ingestion/jobs/queued")
def delete_queued_ingestion_jobs() -> Dict[str, Any]:
    """Bulk delete all queued/pending ingestion jobs and their tasks.

    仅清理队列中的待运行作业及子任务，不影响已完成/正在运行的任务。
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM market.ingestion_job_tasks
                 WHERE job_id IN (
                     SELECT job_id FROM market.ingestion_jobs WHERE status IN ('queued','pending')
                 )
                """
            )
            cur.execute(
                """
                DELETE FROM market.ingestion_jobs
                 WHERE status IN ('queued','pending')
                """
            )
            deleted = cur.rowcount
    return {"deleted": deleted}

@router.delete("/ingestion/job/{job_id}")
def delete_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    """Delete a historical ingestion job and its related records.

    仅删除数据库记录，不会取消正在运行的后台任务。
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) 确认 job 存在
            cur.execute(
                """
                SELECT job_id, status
                  FROM market.ingestion_jobs
                 WHERE job_id=%s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Job not found")

            # 2) 找出与该 job 关联的 run_id（通过 params->>'job_id' 反查）
            cur.execute(
                """
                SELECT run_id
                  FROM market.ingestion_runs
                 WHERE params->>'job_id' = %s
                """,
                (str(job_id),),
            )
            run_rows = cur.fetchall() or []
            run_ids = [r[0] for r in run_rows]

            # 3) 逐个删除 run 级别相关记录（checkpoints / errors / runs）
            for rid in run_ids:
                cur.execute(
                    "DELETE FROM market.ingestion_checkpoints WHERE run_id=%s",
                    (rid,),
                )
                cur.execute(
                    "DELETE FROM market.ingestion_errors WHERE run_id=%s",
                    (rid,),
                )
                cur.execute(
                    "DELETE FROM market.ingestion_runs WHERE run_id=%s",
                    (rid,),
                )

            # 4) 删除与 job 直接关联的 logs / tasks / job 本身
            cur.execute(
                "DELETE FROM market.ingestion_logs WHERE job_id=%s",
                (job_id,),
            )
            cur.execute(
                "DELETE FROM market.ingestion_job_tasks WHERE job_id=%s",
                (job_id,),
            )
            cur.execute(
                "DELETE FROM market.ingestion_jobs WHERE job_id=%s",
                (job_id,),
            )

    return {
        "deleted": True,
        "job_id": str(job_id),
        "deleted_runs": len(run_ids),
    }


@router.delete("/testing/runs")
def bulk_delete_testing_runs(
    payload: BulkDeleteTestingRunsRequest = Body(...),
) -> Dict[str, Any]:
    """Bulk delete testing runs or clear all testing history.

    - 当 delete_all=True 时，直接清空 market.testing_runs 表；
    - 否则按 run_ids 删除指定执行记录。
    """

    deleted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if payload.delete_all:
                cur.execute("DELETE FROM market.testing_runs")
                deleted = cur.rowcount or 0
            else:
                for rid in payload.run_ids:
                    cur.execute(
                        "DELETE FROM market.testing_runs WHERE run_id=%s",
                        (rid,),
                    )
                    deleted += cur.rowcount or 0

    return {"deleted": int(deleted)}


# ---------------------------------------------------------------------------
# Data statistics endpoints（数据看板）
# ---------------------------------------------------------------------------


@router.post("/data-stats/refresh")
def refresh_data_stats() -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT market.refresh_data_stats();")
        return {"success": True}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"refresh_data_stats failed: {exc}") from exc


@router.get("/data-stats")
def list_data_stats() -> Dict[str, Any]:
    rows = _fetchall(
        """
        WITH latest_audit AS (
            SELECT DISTINCT ON (dataset)
                   dataset,
                   trade_date AS audit_ready_date,
                   row_count AS audit_row_count,
                   refreshed_at AS audit_refreshed_at,
                   quality_status AS audit_quality_status
              FROM market.dataset_date_refresh_audit
             WHERE status = 'success'
             ORDER BY dataset, trade_date DESC, refreshed_at DESC
        )
        SELECT ds.data_kind,
               ds.table_name,
               ds.min_date,
               ds.max_date,
               ds.row_count,
               ds.table_bytes,
               ds.index_bytes,
               ds.last_updated_at,
               ds.stat_generated_at,
               ds.extra_info,
               la.audit_ready_date,
               la.audit_row_count,
               la.audit_refreshed_at,
               la.audit_quality_status
          FROM market.data_stats ds
          LEFT JOIN latest_audit la ON la.dataset = ds.data_kind
         ORDER BY ds.data_kind
        """,
    )
    for row in rows:
        audit_ready_date = row.get("audit_ready_date")
        stats_max_date = row.get("max_date")
        if audit_ready_date and stats_max_date:
            cache_state = "fresh" if stats_max_date >= audit_ready_date else "stale"
        elif audit_ready_date and not stats_max_date:
            cache_state = "stale"
        elif stats_max_date and not audit_ready_date:
            cache_state = "audit_missing"
        else:
            cache_state = "unknown"
        row["ready_date"] = _date_iso(audit_ready_date)
        row["audit_ready_date"] = _date_iso(audit_ready_date)
        row["stats_max_date"] = _date_iso(stats_max_date)
        row["physical_max_date"] = _date_iso(stats_max_date)
        row["cache_state"] = cache_state
        row["readiness_source"] = "dataset_date_refresh_audit"
        row["operator_action_required"] = False
        row["audit_refreshed_at"] = _isoformat(row.get("audit_refreshed_at"))
    try:
        target_rows = _fetchall(
            """
            SELECT DISTINCT ON (dataset)
                   dataset,
                   target_date,
                   target_status AS sync_status,
                   last_error_message AS failure_category,
                   next_retry_at,
                   required_before AS final_deadline_at,
                   updated_at AS target_updated_at
              FROM market.data_sync_targets
             WHERE target_status NOT IN ('reconciled')
             ORDER BY dataset, target_date DESC NULLS LAST, updated_at DESC
            """
        )
        targets = {str(r.get("dataset")): r for r in target_rows}
        for row in rows:
            target = targets.get(str(row.get("data_kind")))
            if not target:
                continue
            row["sync_target_date"] = _date_iso(target.get("target_date"))
            row["sync_status"] = target.get("sync_status")
            row["failure_category"] = target.get("failure_category")
            row["next_retry_at"] = _isoformat(target.get("next_retry_at"))
            row["final_deadline_at"] = _isoformat(target.get("final_deadline_at"))
            row["target_updated_at"] = _isoformat(target.get("target_updated_at"))
            target_sync_status = str(target.get("sync_status") or "").lower()
            target_failure_category = str(target.get("failure_category") or "").lower()
            row["operator_action_required"] = target_sync_status in {
                "final_blocked",
                "db_unavailable",
                "provider_contract_error",
            } or target_failure_category in {
                "db_unavailable",
                "provider_contract_error",
            }
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("data_stats target overlay unavailable: %s", exc)
    return {"items": rows}

@router.get("/data-stats/gaps")
def get_data_gaps(
    data_kind: str = Query(..., description="数据集标识，对应 market.data_stats_config.data_kind"),
    start_date: Optional[str] = Query(
        default=None,
        description="可选覆盖起始日期(YYYY-MM-DD)，默认使用 data_stats.min_date",
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="可选覆盖结束日期(YYYY-MM-DD)，默认使用 data_stats.max_date",
    ),
    refresh: bool = Query(
        default=False,
        description="如果为 true，则强制实时计算并更新缓存；否则优先返回上次缓存结果",
    ),
) -> Dict[str, Any]:
    """
    计算指定 data_kind 在本地交易日历上的缺失日期段，并压缩为连续区间返回。
    完全基于新程序的连接池 / 数据表，不依赖 tdx_backend 或 9000 端口。
    """

    # 0) 如果不强制刷新且没有指定日期范围（即查全量），尝试读缓存
    # 缓存有效期 1 小时，过期后重新计算
    _CACHE_TTL_SECONDS = 3600
    if not refresh and not start_date and not end_date:
        cached_rows = _fetchall(
            "SELECT last_check_result, last_check_at FROM market.data_stats WHERE data_kind=%s",
            (data_kind,),
        )
        if cached_rows:
            chk_at_raw = cached_rows[0].get("last_check_at")
            chk_at = _isoformat(chk_at_raw)
            # Check cache TTL
            cache_valid = False
            age: float = 0.0
            if chk_at_raw:
                try:
                    if isinstance(chk_at_raw, str):
                        cache_ts = dt.datetime.fromisoformat(chk_at_raw)
                    elif hasattr(chk_at_raw, "timestamp"):
                        cache_ts = chk_at_raw if isinstance(chk_at_raw, dt.datetime) else None
                    else:
                        cache_ts = None
                    if cache_ts and hasattr(cache_ts, "timestamp"):
                        age = (dt.datetime.now(dt.timezone.utc) - cache_ts).total_seconds()
                        cache_valid = age < _CACHE_TTL_SECONDS
                except Exception as exc:
                    import logging
                    logging.getLogger(__name__).warning("gap cache TTL: failed to parse last_check_at %r: %s", chk_at_raw, exc)
            if cache_valid:
                res = _json_load(cached_rows[0].get("last_check_result"))
                if isinstance(res, dict) and res:
                    res["last_check_at"] = chk_at
                    res["cache_age_seconds"] = int(age)
                    return res

    # 1) 从 data_stats_config 读取表名和日期列
    cfg_rows = _fetchall(
        """
        SELECT data_kind, table_name, date_column, extra_info
          FROM market.data_stats_config
         WHERE data_kind = %s AND enabled
        """,
        (data_kind,),
    )
    if not cfg_rows:
        raise HTTPException(status_code=404, detail="unknown or disabled data_kind")
    cfg = cfg_rows[0]
    table_name = str(cfg.get("table_name") or "").strip()
    date_column = str(cfg.get("date_column") or "").strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="invalid data_stats_config for this data_kind")

    # 非时序数据集（如 stock_basic）：返回行数统计而非交易日覆盖分析
    extra_info = cfg.get("extra_info") or {}
    if isinstance(extra_info, str):
        import json as _json
        extra_info = _json.loads(extra_info)
    if extra_info.get("is_timeseries") is False:
        count_rows = _fetchall(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        total_rows = count_rows[0]["cnt"] if count_rows else 0
        result_payload = {
            "data_kind": data_kind,
            "table_name": table_name,
            "is_timeseries": False,
            "total_rows": total_rows,
            "gap_check_supported": False,
            "reason": "静态参考表，不适用交易日覆盖检查",
        }
        if not start_date and not end_date:
            try:
                now_ts = dt.datetime.now(dt.timezone.utc).isoformat()
                result_payload["last_check_at"] = now_ts
                _execute(
                    """
                    INSERT INTO market.data_stats (data_kind, table_name, last_check_result, last_check_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (data_kind)
                    DO UPDATE SET
                        last_check_result = EXCLUDED.last_check_result,
                        last_check_at = EXCLUDED.last_check_at,
                        table_name = EXCLUDED.table_name
                    """,
                    (data_kind, table_name, _json_dump(result_payload), now_ts),
                )
            except Exception as e:
                print(f"Failed to update data_stats cache: {e}")
        return result_payload

    if not date_column:
        return {
            "data_kind": data_kind,
            "table_name": table_name,
            "gap_check_supported": False,
            "reason": "该数据集无日期列，不支持按交易日历检测缺失",
        }

    # 2) 确定检查区间：显式 start/end 优先，否则使用 data_stats 的 min/max
    start: Optional[dt.date]
    end: Optional[dt.date]
    if start_date and end_date:
        try:
            start = dt.date.fromisoformat(start_date)
            end = dt.date.fromisoformat(end_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid start_date or end_date format")
    elif start_date or end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date must be both provided or omitted")
    else:
        stats_rows = _fetchall(
            """
            SELECT min_date, max_date
              FROM market.data_stats
             WHERE data_kind = %s
            """,
            (data_kind,),
        )
        if not stats_rows:
            raise HTTPException(
                status_code=400,
                detail="no data_stats entry for this data_kind; run /api/data-stats/refresh first",
            )
        row = stats_rows[0]
        start = row.get("min_date")
        end = row.get("max_date")
    if start is None or end is None:
        raise HTTPException(status_code=400, detail="min_date/max_date is NULL for this data_kind; cannot check gaps")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date is after end_date")

    # 3) 读取交易日历上的所有交易日
    cal_rows = _fetchall(
        """
        SELECT cal_date
          FROM market.trading_calendar
         WHERE is_trading = TRUE
           AND cal_date BETWEEN %s AND %s
         ORDER BY cal_date
        """,
        (start, end),
    )
    if not cal_rows:
        raise HTTPException(
            status_code=400,
            detail="no trading_calendar rows in range; please sync calendar via /api/calendar/sync first",
        )
    trading_days: List[dt.date] = [r["cal_date"] for r in cal_rows]

    # 4) 统计业务表中实际出现过数据的交易日期集合
    # OPTIMIZATION: Use "Driver Table + EXISTS" strategy.
    # Instead of scanning the huge data table, we iterate the small trading_calendar 
    # and check existence in the data table using the index.
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")
            
            # Efficiently find which trading days have data
            # 注意：date_column 在分钟/日线表中通常是 timestamp/timestamptz，需要按日期比较。
            # 因此前端看到的 data_kind=kline_minute_raw 等，需要使用 {date_column}::date = cal_date，
            # 否则严格相等比较会导致始终匹配不到任何行，从而错误地认为所有交易日都缺失。
            sql = f"""
                SELECT cal_date AS d
                  FROM market.trading_calendar
                 WHERE is_trading = TRUE
                   AND cal_date >= %s AND cal_date <= %s
                   AND EXISTS (
                       SELECT 1 FROM {table_name}
                        WHERE {date_column}::date = cal_date
                   )
                 ORDER BY cal_date
            """
            cur.execute(sql, (start, end))
            data_rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    
    data_days = {r["d"] for r in data_rows}

    # 5) 求差集并压缩为连续缺失区间
    missing_days: List[dt.date] = [d for d in trading_days if d not in data_days]
    missing_ranges: List[Dict[str, Any]] = []
    cur_start: Optional[dt.date] = None
    cur_end: Optional[dt.date] = None
    for d in missing_days:
        if cur_start is None:
            cur_start = d
            cur_end = d
        elif (d - cur_end).days == 1:
            cur_end = d
        else:
            days_span = (cur_end - cur_start).days + 1
            missing_ranges.append(
                {"start": cur_start.isoformat(), "end": cur_end.isoformat(), "days": days_span}
            )
            cur_start = d
            cur_end = d
    if cur_start is not None and cur_end is not None:
        days_span = (cur_end - cur_start).days + 1
        missing_ranges.append(
            {"start": cur_start.isoformat(), "end": cur_end.isoformat(), "days": days_span}
        )

    # 6) 针对特定数据集统计覆盖的股票数量
    symbol_count: Optional[int] = None
    
    # 确定代码列名
    code_col = None
    if data_kind in (
        "kline_daily_raw",
        "kline_minute_raw", "kline_weekly",
        "stock_moneyflow_ts", "minute_1m",
        "stock_st", "bak_basic", "suspend_d",
        "xtquant_pershare_index",
        "sw_daily", "sector_data",
        "index_daily", "index_basic",
    ):
        code_col = "ts_code"

    if code_col:
        # OPTIMIZATION: Combined strategy for symbol count
        # 1. Increase work_mem to avoid OOM/disk spill on complex queries.
        # 2. Use direct COUNT(DISTINCT) as requested by user to ensure accuracy against actual data,
        #    ignoring market.stock_info which might be incomplete.
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Increase memory for this session to handle HashAggregate in memory
                    cur.execute("SET work_mem = '256MB'")
                    # User requested long timeout. Set DB timeout to 20 minutes (1200s) 
                    # to be safer than the frontend 10 min timeout.
                    cur.execute("SET statement_timeout = '1200s'")
                    
                    symbol_sql = f"""
                        SELECT COUNT(DISTINCT {code_col}) AS c
                          FROM {table_name}
                         WHERE {date_column} >= %s AND {date_column} <= %s
                    """
                    cur.execute(symbol_sql, (start, end))
                    sc_rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
                    if sc_rows:
                        symbol_count = int(sc_rows[0].get("c") or 0)
                            
        except Exception as e: 
            print(f"Error calculating symbol_count for {data_kind}: {e}")
            symbol_count = None

    total_trading = len(trading_days)
    total_missing = len(missing_days)
    
    result_payload = {
        "data_kind": data_kind,
        "table_name": table_name,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "total_trading_days": total_trading,
        "covered_days": total_trading - total_missing,
        "missing_days": total_missing,
        "missing_ranges": missing_ranges,
        "symbol_count": symbol_count,
    }

    # 7) 如果是全量检查（未指定日期范围），更新缓存
    if not start_date and not end_date:
        try:
            now_ts = dt.datetime.now(dt.timezone.utc).isoformat()
            result_payload["last_check_at"] = now_ts
            # Use UPSERT (Insert on conflict update) to ensure row exists even if not previously in stats
            _execute(
                """
                INSERT INTO market.data_stats (data_kind, table_name, last_check_result, last_check_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (data_kind) 
                DO UPDATE SET 
                    last_check_result = EXCLUDED.last_check_result,
                    last_check_at = EXCLUDED.last_check_at,
                    table_name = EXCLUDED.table_name
                """,
                (data_kind, table_name, _json_dump(result_payload), now_ts),
            )
        except Exception as e:
            print(f"Failed to update data_stats cache: {e}")
            pass

    return result_payload


@router.get("/ingestion/auto-range")
def get_ingestion_auto_range(
    data_kind: str = Query(..., description=" data_stats_config.data_kind"),
) -> Dict[str, Any]:
    """Calculate start_date and latest date for incremental catch-up.

    Most market datasets advance by trading day. Event datasets such as
    stock_st_events use calendar-day pub_date and opt in through
    data_stats_config.extra_info.date_sequence = 'calendar'.
    """

    cfg_rows = _fetchall(
        """
        SELECT data_kind, table_name, date_column, extra_info
          FROM market.data_stats_config
         WHERE data_kind = %s AND enabled
        """,
        (data_kind,),
    )
    if not cfg_rows:
        raise HTTPException(status_code=404, detail="unknown or disabled data_kind")
    cfg = cfg_rows[0]
    table_name = str(cfg.get("table_name") or "").strip()
    date_column = str(cfg.get("date_column") or "trade_date").strip()
    extra_info = cfg.get("extra_info")
    use_calendar_dates = _uses_calendar_date_sequence(extra_info)
    use_refresh_audit_cursor = _uses_refresh_audit_cursor(extra_info)

    table_max_date: Optional[dt.date] = None
    max_query_failed = False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '10s'")
                cur.execute(f"SELECT MAX({date_column})::date FROM {table_name}")
                row = cur.fetchone()
                if row and row[0]:
                    table_max_date = row[0]
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "[%s] query MAX(%s) from %s failed: %s",
            data_kind,
            date_column,
            table_name,
            exc,
        )
        max_query_failed = True

    audit_max_date: Optional[dt.date] = None
    if use_refresh_audit_cursor:
        audit_rows = _fetchall(
            """
            SELECT MAX(trade_date)::date AS mx
              FROM market.dataset_date_refresh_audit
             WHERE dataset = %s
               AND status = 'success'
            """,
            (data_kind,),
        )
        audit_max_date = audit_rows[0].get("mx") if audit_rows else None

    audit_required = use_refresh_audit_cursor or data_kind in DATASET_REGISTRY
    if audit_required and audit_max_date is None and table_max_date is not None:
        return {
            "data_kind": data_kind,
            "table_name": table_name,
            "start_date": None,
            "latest_date": None,
            "latest_trading_date": None,
            "latest_date_kind": "calendar" if use_calendar_dates else "trading",
            "current_max_date": None,
            "data_max_date": table_max_date.isoformat() if isinstance(table_max_date, dt.date) else None,
            "cursor_source": "refresh_audit_missing",
            "readiness_source": "dataset_date_refresh_audit",
            "audit_missing": True,
            "needs_reconcile": True,
            "has_data": True,
            "has_cursor": False,
            "up_to_date": False,
        }

    current_max_date = audit_max_date or (None if audit_required else table_max_date)

    if use_calendar_dates:
        latest_date: Optional[dt.date] = dt.date.today()
    else:
        try:
            latest_date = _latest_trading_day_on_or_before()
        except DataUnavailableError as exc:
            _raise_trading_calendar_unavailable(exc)
        if latest_date is None:
            raise HTTPException(status_code=400, detail="no trading_calendar rows; please sync calendar first")

    up_to_date = False
    if current_max_date is None:
        if max_query_failed:
            raise HTTPException(
                status_code=503,
                detail=f"[{data_kind}] cannot query MAX date; check database before retrying auto catch-up",
            )
        initial_start = None
        if data_kind in DATASET_REGISTRY:
            initial_start = DATASET_REGISTRY[data_kind].initial_start_date
        start_date = dt.date.fromisoformat(initial_start) if initial_start else dt.date(1990, 1, 1)
        has_data = False
    else:
        up_to_date = current_max_date >= latest_date
        if up_to_date:
            start_date = latest_date
        elif use_calendar_dates:
            start_date = current_max_date + dt.timedelta(days=1)
        else:
            try:
                start_date = _next_trading_day_after(current_max_date)
            except DataUnavailableError as exc:
                _raise_trading_calendar_unavailable(exc)
        has_data = True

    return {
        "data_kind": data_kind,
        "table_name": table_name,
        "start_date": start_date.isoformat(),
        "latest_date": latest_date.isoformat(),
        "latest_trading_date": latest_date.isoformat(),
        "latest_date_kind": "calendar" if use_calendar_dates else "trading",
        "current_max_date": current_max_date.isoformat() if isinstance(current_max_date, dt.date) else None,
        "data_max_date": table_max_date.isoformat() if isinstance(table_max_date, dt.date) else None,
        "cursor_source": "refresh_audit" if audit_max_date else ("none" if audit_required else "table"),
        "readiness_source": "dataset_date_refresh_audit" if audit_required else "table",
        "audit_missing": audit_required and audit_max_date is None,
        "needs_reconcile": audit_required and audit_max_date is None and table_max_date is not None,
        "has_data": has_data,
        "has_cursor": current_max_date is not None,
        "up_to_date": up_to_date,
    }


class GoIncrementalRequest(BaseModel):
    data_kind: str
    start_date: str
    workers: int = 1


@router.post("/ingestion/incremental")
def trigger_go_incremental(payload: GoIncrementalRequest) -> Dict[str, Any]:
    """For specific TDX datasets, reuse Go init handlers as incremental tasks.

    - data_kind: kline_daily_raw_go / kline_minute_raw
    - start_date
    - end_date / target_date latest_trading_date
    - truncate_before false
    """

    data_kind = (payload.data_kind or "").strip()
    if data_kind not in {"kline_daily_raw_go", "kline_minute_raw"}:
        raise HTTPException(status_code=400, detail="unsupported data_kind for Go incremental")
    
    try:
        start_date = dt.date.fromisoformat(payload.start_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid start_date format, expected YYYY-MM-DD")
    
    # latest_trading_date：同样仅取当前日期及以前的交易日
    rows = _fetchall(
        """
        SELECT MAX(cal_date) AS latest
          FROM market.trading_calendar
         WHERE is_trading = TRUE
           AND cal_date <= CURRENT_DATE
        """,
    )
    latest = rows[0].get("latest") if rows else None
    if latest is None:
        raise HTTPException(status_code=400, detail="no trading_calendar rows; please sync calendar first")
    if not isinstance(latest, dt.date):
        try:
            latest_trading_date = dt.date.fromisoformat(str(latest))
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=500, detail="invalid latest_trading_date in DB")
    else:
        latest_trading_date = latest
    
    workers = payload.workers if payload.workers and payload.workers > 0 else 1
    
    summary: Dict[str, Any] = {
        "data_kind": data_kind,
        "mode": "incremental",
        "via": "go_init",
        "start_date": start_date.isoformat(),
        "end_date": latest_trading_date.isoformat(),
        "workers": workers,
    }
    job_id = _create_job("incremental", summary)
    
    base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
    tz = dt.timezone(dt.timedelta(hours=8))
    
    if data_kind == "kline_minute_raw":
        # Go start_time 
        start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=tz)
        go_payload: Dict[str, Any] = {
            "job_id": str(job_id),
            "codes": [],
            "start_time": start_dt.isoformat(),
            "workers": workers,
            "options": {
                "truncate_before": False,
                "max_rows_per_chunk": 500_000,
                "source": "tdx_api",
            },
        }
        url = f"{base}/api/tasks/ingest-minute-raw-init"
    else:  # kline_daily_raw_go
        start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=tz)
        go_payload = {
            "job_id": str(job_id),
            "codes": [],
            "start_time": start_dt.isoformat(),
            "workers": workers,
            "options": {
                "truncate_before": False,
                "max_rows_per_chunk": 500_000,
                "source": "tdx_api",
            },
        }
        url = f"{base}/api/tasks/ingest-daily-raw-init"

    try:
        resp = requests.post(url, json=go_payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        err_summary = {**summary, "error": str(exc), "phase": "create_go_task"}
        _execute(
            """
            UPDATE market.ingestion_jobs
               SET status='failed', finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (_json_dump(err_summary), job_id),
        )
        raise HTTPException(status_code=502, detail=f"failed to start Go incremental task: {exc}")
    
    if isinstance(data, dict) and data.get("code") not in (0, None):
        msg = str(data)
        err_summary = {**summary, "error": msg, "phase": "create_go_task"}
        _execute(
            """
            UPDATE market.ingestion_jobs
               SET status='failed', finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (_json_dump(err_summary), job_id),
        )
        raise HTTPException(status_code=502, detail=f"Go incremental task error: {msg}")
    
    task_id: Optional[str] = None
    payload_data = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload_data, dict):
        raw_tid = payload_data.get("task_id")
        if raw_tid is not None:
            task_id = str(raw_tid)
    
    if task_id is not None:
        summary_with_task = {**summary, "go_task_id": task_id}
        _execute(
            """
            UPDATE market.ingestion_jobs
               SET summary=%s
             WHERE job_id=%s
            """,
            (_json_dump(summary_with_task), job_id),
        )
    
    return {
        "job_id": str(job_id),
        "task_id": task_id,
        "data_kind": data_kind,
        "start_date": start_date.isoformat(),
        "end_date": latest_trading_date.isoformat(),
    }


# ---------------------------------------------------------------------------
# Trading calendar helper
# ---------------------------------------------------------------------------

@router.get("/trading/latest-day")
def get_latest_trading_day() -> Dict[str, Any]:
    """Return the latest trading day from market.trading_calendar.

    tdx_backend /api/trading/latest-day 
    """
    
    rows = _fetchall(
        """
        SELECT MAX(cal_date) AS latest
          FROM market.trading_calendar
         WHERE is_trading = TRUE
        """,
    )
    latest = rows[0].get("latest") if rows else None
    if latest is None:
        return {"latest_trading_day": None}
    if isinstance(latest, dt.date):
        return {"latest_trading_day": latest.isoformat()}
    return {"latest_trading_day": str(latest)}

class CalendarSyncRequest(BaseModel):
    start_date: str
    end_date: str
    exchange: str = "SSE"


@router.post("/calendar/sync")
def calendar_sync(
    payload: Optional[CalendarSyncRequest] = Body(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    exchange: str = Query(default="SSE"),
) -> Dict[str, Any]:
    """Sync trading calendar from Tushare trade_cal into market.trading_calendar.

    与 tdx_backend 中的 /api/calendar/sync 语义保持一致，
    供“交易日历初始化/同步”页面调用。
    """

    try:
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise HTTPException(status_code=500, detail="TUSHARE_TOKEN not set")
        ts = importlib.import_module("tushare")
        pro = ts.pro_api(token)

        # 允许通过 JSON body 或 query 传参，保持兼容性
        if payload is None:
            if not start_date or not end_date:
                raise HTTPException(status_code=400, detail="start_date and end_date are required")
            payload = CalendarSyncRequest(
                start_date=start_date,
                end_date=end_date,
                exchange=exchange or "SSE",
            )

        df = pro.trade_cal(
            exchange=payload.exchange,
            start_date=payload.start_date.replace("-", ""),
            end_date=payload.end_date.replace("-", ""),
        )

        rows: List[tuple] = []
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                d = str(r.get("cal_date"))
                if len(d) == 8:
                    d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                is_open = bool(int(r.get("is_open") or 0))
                rows.append((d, is_open))

        if rows:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    pgx.execute_values(
                        cur,
                        "INSERT INTO market.trading_calendar(cal_date, is_trading) VALUES %s "
                        "ON CONFLICT (cal_date) DO UPDATE SET is_trading=EXCLUDED.is_trading",
                        rows,
                    )

        calendar_status_cache = None
        if rows:
            try:
                refreshed = _trading_calendar_service()._refresh_cache("calendar_sync")
                calendar_status_cache = {
                    "generated_at": refreshed.get("generated_at"),
                    "coverage_start": refreshed.get("coverage_start"),
                    "coverage_end": refreshed.get("coverage_end"),
                    "calendar_row_count": len(refreshed.get("calendar") or []),
                    "checksum": refreshed.get("checksum"),
                    "refresh_reason": refreshed.get("_refresh_reason"),
                }
            except DataUnavailableError as exc:
                _raise_trading_calendar_unavailable(exc)

        return {"inserted_or_updated": len(rows), "calendar_status_cache": calendar_status_cache}
    except HTTPException:
        # 直接透传业务性错误
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Sector Data: build + export
# ---------------------------------------------------------------------------

@router.post("/sector-data/build")
def build_sector_data(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """预计算 sector_data（PIT映射 + 资金流聚合 + 展开到个股）。"""
    from ..services.sector_data_builder import SectorDataBuilder

    try:
        builder = SectorDataBuilder()
        rows = builder.build_range(
            dt.date.fromisoformat(start_date),
            dt.date.fromisoformat(end_date),
        )
        return {"rows": rows, "start_date": start_date, "end_date": end_date}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# 数据健康报警 API
# ---------------------------------------------------------------------------


@router.get("/ingestion/alerts/active")
def get_active_alerts(
    severity_min: str = Query("warning", description="Minimum severity: info|warning|error|critical"),
    limit: int = Query(50, ge=1, le=500, description="Max alerts to return"),
) -> Dict[str, Any]:
    """获取未确认的活跃报警（severity >= severity_min）."""
    sev_order = {"info": 0, "warning": 1, "error": 2, "critical": 3}
    min_level = sev_order.get(severity_min, 1)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """SELECT alert_id, created_at, severity, dataset, alert_type,
                          title, message, details, acknowledged
                   FROM market.data_alerts
                   WHERE acknowledged = FALSE
                     AND severity IN %s
                   ORDER BY created_at DESC
                   LIMIT %s""",
                (tuple(severity for severity, level in sev_order.items() if level >= min_level), limit),
            )
            rows = cur.fetchall()
            return {"alerts": [_serialize_alert(r) for r in rows], "count": len(rows)}


@router.get("/ingestion/alerts/unack-count")
def get_unack_alert_count() -> Dict[str, int]:
    """获取未确认报警数量."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM market.data_alerts
                   WHERE acknowledged = FALSE AND severity IN ('warning','error','critical')"""
            )
            count = cur.fetchone()[0]
            return {"count": count}


@router.post("/ingestion/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str) -> Dict[str, Any]:
    """标记报警为已确认."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """UPDATE market.data_alerts
                   SET acknowledged = TRUE, ack_at = NOW()
                   WHERE alert_id = %s
                   RETURNING alert_id, acknowledged, ack_at""",
                (alert_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
            conn.commit()
            return _serialize_alert(row)


def _serialize_alert(row: Any) -> Dict[str, Any]:
    return {
        "alert_id": str(row.get("alert_id")),
        "created_at": _isoformat(row.get("created_at")),
        "severity": row.get("severity"),
        "dataset": row.get("dataset"),
        "alert_type": row.get("alert_type"),
        "title": row.get("title"),
        "message": row.get("message"),
        "details": _json_load(row.get("details")),
        "acknowledged": row.get("acknowledged"),
        "ack_at": _isoformat(row.get("ack_at")),
    }


@router.post("/sector-data/export")
def export_sector_data(
    snapshot_id: str = Query(..., description="Snapshot ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """导出 sector_data.h5 到指定 snapshot。"""
    from ..qlib_exporter.exporter import QlibSectorDataExporter

    try:
        exporter = QlibSectorDataExporter()
        result = exporter.export_full(
            snapshot_id=snapshot_id,
            start=dt.date.fromisoformat(start_date),
            end=dt.date.fromisoformat(end_date),
        )
        return {
            "snapshot_id": result.snapshot_id,
            "freq": result.freq,
            "start": str(result.start),
            "end": str(result.end),
            "rows": result.rows,
            "instruments": len(result.ts_codes),
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
