from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from typing import Any, Dict, List, Optional

import psycopg2.extras as pgx
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field

from next_app.backend.db.pg_pool import get_conn
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
    if ds in {"stock_moneyflow"}:
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
    counters = {
        "total": total,
        "done": done,
        "running": running,
        "pending": pending,
        "failed": failed,
        "success": success,
        "inserted_rows": inserted_rows,
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
        ("kline_daily_qfq", "incremental", "daily", True, {}),
        ("kline_daily_raw", "incremental", "daily", True, {}),
        ("kline_minute_raw", "incremental", "10m", True, {}),
        ("stock_moneyflow", "incremental", "daily", True, {}),
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
    if dataset not in {"kline_daily_raw", "kline_minute_raw"}:
        raise HTTPException(status_code=400, detail="unsupported dataset for init")
    options = dict(payload.options or {})
    summary = {"datasets": [dataset], **options}
    job_id = _create_init_job(summary)
    options["job_id"] = str(job_id)
    run_id = scheduler.run_ingestion_now(dataset=dataset, mode="init", triggered_by="api", options=options)
    return {"job_id": str(job_id), "run_id": str(run_id)}


@router.get("/ingestion/job/{job_id}")
async def get_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    return _job_status(job_id)


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
    summary = {"dataset": payload.dataset, "mode": payload.mode, **(payload.options or {})}
    job_type = "init" if payload.mode == "init" else "incremental"
    job_id = _create_job(job_type, summary)
    options = dict(payload.options or {})
    options["job_id"] = str(job_id)
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


@router.delete("/ingestion/job/{job_id}")
async def delete_ingestion_job(job_id: uuid.UUID = Path(...)) -> Dict[str, Any]:
    """Delete a historical ingestion job and its related records.

    仅允许删除非运行中的任务（状态非 running/queued/pending），
    防止误删调度中的任务。主要用于清理调试期间产生的失败/测试任务。
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) 确认 job 存在并检查状态
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
            status = (row[1] or "").lower()
            if status in {"running", "queued", "pending"}:
                raise HTTPException(status_code=400, detail="Cannot delete a running or pending job")

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
) -> Dict[str, Any]:
    """
    计算指定 data_kind 在本地交易日历上的缺失日期段，并压缩为连续区间返回。
    完全基于新程序的连接池 / 数据表，不依赖 tdx_backend 或 9000 端口。
    """

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
    sql = f"""
        SELECT DISTINCT {date_column}::date AS d
          FROM {table_name}
         WHERE {date_column} >= %s AND {date_column} <= %s
         ORDER BY d
    """
    data_rows = _fetchall(sql, (start, end))
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

    total_trading = len(trading_days)
    total_missing = len(missing_days)
    return {
        "data_kind": data_kind,
        "table_name": table_name,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "total_trading_days": total_trading,
        "covered_days": total_trading - total_missing,
        "missing_days": total_missing,
        "missing_ranges": missing_ranges,
    }
# ---------------------------------------------------------------------------
# Trading calendar helper（供“补齐到最新交易日”使用）
# ---------------------------------------------------------------------------


@router.get("/trading/latest-day")
async def get_latest_trading_day() -> Dict[str, Any]:
    """Return the latest trading day from market.trading_calendar.

    与 tdx_backend 中新增的 /api/trading/latest-day 语义保持一致，
    供前端“补齐到最新交易日”按钮使用。
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
