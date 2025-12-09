from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from typing import Any, Dict, List, Optional

import psycopg2.extras as pgx
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field
import requests
from requests import exceptions as req_exc

from ..db.pg_pool import get_conn
from ..ingestion.tdx_scheduler import scheduler  # 1:1 复用现有调度器实现


router = APIRouter(prefix="/api", tags=["ingestion"])


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
    except Exception:  # noqa: BLE001 - fallback raw
        return value


def _isoformat(value: Optional[dt.datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc).isoformat()
    return value.astimezone(dt.timezone.utc).isoformat()


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
    if ds in {"kline_daily_qfq", "kline_daily_raw", "kline_minute_raw"}:
        return "tdx_api"
    if ds.startswith("tdx_board_"):
        return "tushare"
    if ds in {"stock_moneyflow", "stock_moneyflow_ts", "stock_basic", "stock_st", "bak_basic"}:
        return "tushare"
    if ds in {"kline_weekly"}:
        return "derived_from_kline_daily_qfq"
    if ds in {"trade_agg_5m"}:
        return "tdx_api_minute_trade_all"
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
    inserted_rows = int(summary.get("inserted_rows") or stats.get("inserted_rows") or 0)

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
        # - kline_daily_qfq / kline_daily_raw / kline_minute_raw 的初始化和增量
        #   已统一切换为 Go 实现（init: /api/ingestion/init，incremental: /api/ingestion/incremental），
        #   不再通过 Python 调度器执行，因此这里不再为它们创建默认 schedule，
        #   以避免误触发 Python 版脚本。
        ("stock_moneyflow", "incremental", "daily", True, {}),
        ("stock_moneyflow_ts", "incremental", "daily", True, {}),
        ("kline_weekly", "incremental", "daily", True, {}),
        ("trade_agg_5m", "incremental", "10m", True, {"freq_minutes": 5, "symbols_scope": "watchlist"}),
    ]
    items: List[Dict[str, Any]] = []
    for ds, md, freq, en, opts in defaults:
        items.append(_upsert_ingestion_schedule_entry(ds, md, freq, en, opts))
    scheduler.refresh_schedules()
    return items


@router.post("/testing/run")
async def trigger_testing_run(payload: TestingRunRequest) -> Dict[str, Any]:
    run_id = scheduler.run_testing_now(triggered_by=payload.triggered_by, options=payload.options)
    return {"run_id": str(run_id)}


@router.get("/testing/runs")
async def list_testing_runs(limit: int = Query(20), offset: int = Query(0)) -> Dict[str, Any]:
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
async def list_testing_schedules() -> Dict[str, Any]:
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
async def upsert_testing_schedule(payload: TestingScheduleUpsertRequest) -> Dict[str, Any]:
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
async def toggle_testing_schedule(
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
async def run_testing_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    data = _ensure_testing_schedule(schedule_id)
    run_id = scheduler.run_testing_for_schedule(schedule_id)
    data["last_status"] = "queued"
    return {"run_id": str(run_id), "schedule": _serialize_schedule(data)}


# ---------------------------------------------------------------------------
# Ingestion API endpoints（路径与 tdx_backend 完全一致）
# ---------------------------------------------------------------------------


@router.post("/ingestion/init")
async def start_ingestion_init(payload: IngestionInitRequest) -> Dict[str, Any]:
    dataset = (payload.dataset or "").strip().lower()
    # 目前仅支持通过 Go 服务执行以下初始化：
    # - kline_minute_raw: 分钟线 RAW，全量 COPY 入库
    # - kline_daily_raw_go: 未复权日线 RAW（Go 直连版），全量 COPY 入库
    # - kline_daily_qfq_go: 前复权日线 QFQ（Go 直连版），全量 COPY 入库
    if dataset not in {"kline_minute_raw", "kline_daily_raw_go", "kline_daily_qfq_go"}:
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

    # 前复权日线（Go 直连版）初始化：调用新的 TDX Go API，将结果 COPY 至 kline_daily_qfq
    if dataset == "kline_daily_qfq_go":
        workers = int(options.get("workers") or 1)
        truncate_before = bool(options.get("truncate"))
        max_rows_per_chunk = int(options.get("max_rows_per_chunk") or 500_000)
        codes = options.get("codes") or []

        go_payload: Dict[str, Any] = {
            "job_id": str(job_id),
            "codes": codes,
            "workers": workers,
            "options": {
                "truncate_before": truncate_before,
                "max_rows_per_chunk": max_rows_per_chunk,
                "source": "tdx_api",
            },
        }

        base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
        url = f"{base}/api/tasks/ingest-daily-qfq-init"

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
            raise HTTPException(status_code=502, detail=f"failed to start TDX daily qfq init task: {exc}")

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
            raise HTTPException(status_code=502, detail=f"TDX daily qfq init task error: {msg}")

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
async def get_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    return _job_status(job_id)


@router.post("/ingestion/job/{job_id}/cancel")
async def cancel_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
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
    except Exception:  # noqa: BLE001
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
async def list_ingestion_jobs(limit: int = Query(50), active_only: bool = Query(False)) -> Dict[str, Any]:
    base_sql = (
        "SELECT job_id, status, created_at FROM market.ingestion_jobs "
        + ("WHERE status IN ('running','queued','pending') " if active_only else "")
        + "ORDER BY created_at DESC LIMIT %s"
    )
    rows = _fetchall(base_sql, (limit,))
    items: List[Dict[str, Any]] = []
    for r in rows:
        jid = r.get("job_id")
        try:
            items.append(_job_status(uuid.UUID(str(jid))))
        except Exception:  # noqa: BLE001
            continue
    return {"items": items}


@router.post("/ingestion/schedule/defaults")
async def create_default_ingestion_schedules() -> Dict[str, Any]:
    items = _ensure_default_ingestion_schedules()
    return {"items": [_serialize_ingestion_schedule(row) for row in items]}


@router.post("/ingestion/run")
async def trigger_ingestion_run(payload: IngestionRunRequest) -> Dict[str, Any]:
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
    if dataset in {"kline_daily_qfq", "kline_daily_raw", "kline_minute_raw"} and mode in {"init", "incremental"}:
        raise HTTPException(
            status_code=400,
            detail="dataset must be ingested via Go APIs (use /api/ingestion/init or /api/ingestion/incremental)",
        )

    # 业务校验：三支新 Tushare 数据集
    if dataset == "stock_basic":
        if mode != "init":
            raise HTTPException(status_code=400, detail="stock_basic only supports init mode")
    elif dataset == "stock_st":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stock_st init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stock_st init requires end_date")
    elif dataset == "bak_basic":
        # init: 需要 start_date/end_date；incremental: start_date 可选
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="bak_basic init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="bak_basic init requires end_date")
    elif dataset == "stock_moneyflow_ts":
        # init: 需要起止日期；incremental: start_date 可选；支持可选 truncate
        if mode == "init" and not options.get("start_date"):
            raise HTTPException(status_code=400, detail="stock_moneyflow_ts init requires start_date")
        if mode == "init" and not options.get("end_date"):
            raise HTTPException(status_code=400, detail="stock_moneyflow_ts init requires end_date")

    summary = {"dataset": payload.dataset, "mode": payload.mode, **(payload.options or {})}
    job_type = "init" if payload.mode == "init" else "incremental"
    job_id = _create_job(job_type, summary)
    options["job_id"] = str(job_id)

    # 为未复权日线增量任务提供默认并行度：当前端未显式传入 workers 时，使用一个适中的默认值。
    if payload.dataset == "kline_daily_raw" and payload.mode == "incremental" and "workers" not in options:
        options["workers"] = 4

    # 对 trade_agg_5m 增量任务：前端通过 options.args 传入完整命令行片段，
    # 其中可能已经包含一个前端生成的 --job-id。这里需要将其替换为我们刚刚
    # 在 ingestion_jobs 表中创建的 job_id，确保脚本在写入 ingestion_job_tasks
    # 时不会触发外键约束错误。
    if payload.dataset == "trade_agg_5m" and "args" in options and isinstance(options["args"], str):
        raw_args = options["args"].strip()
        if raw_args:
            tokens = [tok for tok in raw_args.split(" ") if tok]
            new_tokens: list[str] = []
            skip_next = False
            for tok in tokens:
                if skip_next:
                    skip_next = False
                    continue
                if tok == "--job-id":
                    # 跳过旧的 --job-id 及其参数
                    skip_next = True
                    continue
                new_tokens.append(tok)
            new_tokens.extend(["--job-id", str(job_id)])
            options["args"] = " ".join(new_tokens)
    run_id = scheduler.run_ingestion_now(
        dataset=payload.dataset,
        mode=payload.mode,
        triggered_by=payload.triggered_by,
        options=options,
    )
    return {"job_id": str(job_id), "run_id": str(run_id)}


@router.get("/ingestion/schedule")
async def list_ingestion_schedules() -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT schedule_id, dataset, mode, enabled, frequency, options,
               last_run_at, next_run_at, last_status, last_error,
               created_at, updated_at
          FROM market.ingestion_schedules
         ORDER BY dataset, mode
        """,
    )
    return {"items": [_serialize_ingestion_schedule(row) for row in rows]}


@router.post("/ingestion/schedule")
async def upsert_ingestion_schedule(payload: IngestionScheduleUpsertRequest) -> Dict[str, Any]:
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
async def toggle_ingestion_schedule(
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
async def run_ingestion_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    data = _ensure_ingestion_schedule(schedule_id)
    run_id = scheduler.run_ingestion_for_schedule(schedule_id, data["dataset"], data["mode"])
    data["last_status"] = "queued"
    return {"run_id": str(run_id), "schedule": _serialize_ingestion_schedule(data)}


@router.delete("/ingestion/schedule/{schedule_id}")
async def delete_ingestion_schedule(schedule_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
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
async def list_ingestion_logs(
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
async def bulk_delete_ingestion_logs(
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
async def delete_queued_ingestion_jobs() -> Dict[str, Any]:
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

async def delete_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
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
async def bulk_delete_testing_runs(
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
async def refresh_data_stats() -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT market.refresh_data_stats();")
        return {"success": True}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"refresh_data_stats failed: {exc}") from exc


@router.get("/data-stats")
async def list_data_stats() -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT data_kind,
               table_name,
               min_date,
               max_date,
               row_count,
               table_bytes,
               index_bytes,
               last_updated_at,
               stat_generated_at,
               extra_info
          FROM market.data_stats
         ORDER BY data_kind
        """,
    )
    return {"items": rows}

@router.get("/data-stats/gaps")
async def get_data_gaps(
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
    if not refresh and not start_date and not end_date:
        cached_rows = _fetchall(
            "SELECT last_check_result, last_check_at FROM market.data_stats WHERE data_kind=%s",
            (data_kind,),
        )
        if cached_rows:
            res = _json_load(cached_rows[0].get("last_check_result"))
            chk_at = _isoformat(cached_rows[0].get("last_check_at"))
            if isinstance(res, dict) and res:
                # 将 last_check_at 注入返回结果
                res["last_check_at"] = chk_at
                return res

    # 1) 从 data_stats_config 读取表名和日期列
    cfg_rows = _fetchall(
        """
        SELECT data_kind, table_name, date_column
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
    if not table_name or not date_column:
        raise HTTPException(status_code=400, detail="invalid data_stats_config for this data_kind")

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
    if data_kind == "trade_agg_5m":
        code_col = "symbol"
    elif data_kind in (
        "kline_daily_qfq", "kline_daily_raw", 
        "kline_minute_raw", "kline_weekly", 
        "stock_moneyflow", "stock_moneyflow_ts", "minute_1m",
        "stock_st", "bak_basic"
    ):
        code_col = "ts_code"
    elif data_kind.startswith("tdx_board_"):
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
async def get_ingestion_auto_range(
    data_kind: str = Query(..., description=" data_stats_config.data_kind"),
) -> Dict[str, Any]:
    """Calculate start_date and latest_trading_date for incremental catch-up.

    -  data_kind
    -  start_date: next trading day after max_date, or 1990-01-01 if no data
    -  latest_trading_date: MAX(cal_date WHERE is_trading)
    """

    # 1)  data_stats
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT market.refresh_data_stats();")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"refresh_data_stats failed: {exc}") from exc

    # 2)  data_stats_config  table_name
    cfg_rows = _fetchall(
        """
        SELECT data_kind, table_name
          FROM market.data_stats_config
         WHERE data_kind = %s AND enabled
        """,
        (data_kind,),
    )
    if not cfg_rows:
        raise HTTPException(status_code=404, detail="unknown or disabled data_kind")
    cfg = cfg_rows[0]
    table_name = str(cfg.get("table_name") or "").strip()

    # 3)  data_stats  max_date
    stats_row = _fetchone(
        """
        SELECT max_date
          FROM market.data_stats
         WHERE data_kind = %s
        """,
        (data_kind,),
    )
    current_max_date: Optional[dt.date] = None
    if stats_row is not None:
        current_max_date = stats_row.get("max_date")

    # 4) latest_trading_date：仅考虑当前日期及之前的交易日，避免拿到未来计划交易日
    latest_rows = _fetchall(
        """
        SELECT MAX(cal_date) AS latest
          FROM market.trading_calendar
         WHERE is_trading = TRUE
           AND cal_date <= CURRENT_DATE
        """,
    )
    latest_trading_date: Optional[dt.date] = None
    if latest_rows:
        latest_trading_date = latest_rows[0].get("latest")
    if latest_trading_date is None:
        raise HTTPException(status_code=400, detail="no trading_calendar rows; please sync calendar first")

    # 5)  start_date
    if current_max_date is None:
        start_date = dt.date(1990, 1, 1)
        has_data = False
    else:
        next_rows = _fetchall(
            """
            SELECT MIN(cal_date) AS next_trading
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date > %s
            """,
            (current_max_date,),
        )
        next_trading: Optional[dt.date] = None
        if next_rows:
            next_trading = next_rows[0].get("next_trading")
        if next_trading is None:
            start_date = latest_trading_date
        else:
            start_date = next_trading
        has_data = True

    return {
        "data_kind": data_kind,
        "table_name": table_name,
        "start_date": start_date.isoformat(),
        "latest_trading_date": latest_trading_date.isoformat(),
        "current_max_date": current_max_date.isoformat() if isinstance(current_max_date, dt.date) else None,
        "has_data": has_data,
    }


class GoIncrementalRequest(BaseModel):
    data_kind: str
    start_date: str
    workers: int = 1


@router.post("/ingestion/incremental")
async def trigger_go_incremental(payload: GoIncrementalRequest) -> Dict[str, Any]:
    """For specific TDX datasets, reuse Go init handlers as incremental tasks.

    - data_kind: kline_daily_raw_go / kline_daily_qfq_go / kline_minute_raw
    - start_date 
    - end_date / target_date latest_trading_date
    - truncate_before false
    """
    
    data_kind = (payload.data_kind or "").strip()
    if data_kind not in {"kline_daily_raw_go", "kline_daily_qfq_go", "kline_minute_raw"}:
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
    elif data_kind == "kline_daily_raw_go":
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
    else:  # kline_daily_qfq_go
        go_payload = {
            "job_id": str(job_id),
            "codes": [],
            "workers": workers,
            "options": {
                "truncate_before": False,
                "max_rows_per_chunk": 500_000,
                "source": "tdx_api",
            },
        }
        url = f"{base}/api/tasks/ingest-daily-qfq-init"
    
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
async def get_latest_trading_day() -> Dict[str, Any]:
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
async def calendar_sync(
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
        import importlib

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

        return {"inserted_or_updated": len(rows)}
    except HTTPException:
        # 直接透传业务性错误
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
