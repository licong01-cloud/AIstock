"""Factor independent metrics facade for MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.mcp_payload_budget import artifact_ref, clamp_limit, clamp_offset, strip_forbidden_fields, summary_envelope
from backend.services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
from backend.services.quantevolver.factor_metrics_scheduler import OFFICIAL_FACTOR_WINDOW_END
from backend.services.quantevolver.factor_compute_preflight import build_official_cache_preflight

router = APIRouter(prefix="/factor-metrics", tags=["factor-metrics"])
SUBMIT_FACTOR_METRICS_CONFIRM = "SUBMIT_FACTOR_METRICS"
CALC_ENGINE = "qe_eval_v2"


class FactorMetricsPlanRequest(BaseModel):
    factor_names: list[str] | None = None
    dataset: str = "factor_metrics_compute"
    options: dict[str, Any] = Field(default_factory=dict)


class FactorMetricsSubmitRequest(FactorMetricsPlanRequest):
    confirm: str | None = None
    schedule_id: str | None = None


def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def _one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    rows = _rows(sql, params)
    return rows[0] if rows else None


def _job(job_id: str) -> dict[str, Any] | None:
    return _one(
        """
        SELECT job_id::text, job_type, status, created_at, started_at, completed_at, summary
        FROM market.ingestion_jobs
        WHERE job_id::text = %s
        """,
        (job_id,),
    )


@router.post("/plan")
def plan_metrics(req: FactorMetricsPlanRequest) -> dict[str, Any]:
    names = [str(name).strip() for name in (req.factor_names or []) if str(name).strip()]
    if names:
        found = _rows("SELECT factor_name, source, is_available FROM aistock_factor_catalog WHERE factor_name = ANY(%s) ORDER BY factor_name", (names,))
        eligible_count = sum(bool(row.get("is_available")) for row in found)
    else:
        found = _rows("SELECT factor_name, source, is_available FROM aistock_factor_catalog WHERE is_available = TRUE ORDER BY factor_name LIMIT 20")
        count_row = _one("SELECT COUNT(*) AS total FROM aistock_factor_catalog WHERE is_available = TRUE") or {}
        eligible_count = int(count_row.get("total") or 0)
    target_end = str(
        req.options.get("end_date")
        or req.options.get("window_backtest_end")
        or req.options.get("data_date")
        or OFFICIAL_FACTOR_WINDOW_END
    )
    cache_preflight = build_official_cache_preflight(
        target_end=target_end,
        eligible_factor_count=eligible_count,
    )
    return {
        "ok": True,
        "domain": "factor_metrics",
        "plan_type": "submit_factor_metrics",
        "dataset": req.dataset,
        "requested_factor_count": len(names) if names else "all_available",
        "eligible_factor_count": eligible_count,
        "eligible_preview": strip_forbidden_fields(found[:20]),
        "eligible_preview_count": min(len(found), 20),
        "target_end": target_end,
        "cache_preflight": cache_preflight,
        "cache_rebuild_required": not cache_preflight["ok"],
        "required_confirmation": SUBMIT_FACTOR_METRICS_CONFIRM,
        "async_job": True,
        "submit_tool": "aistock-factor-metrics/factor_metrics_submit_confirmed",
        "omitted_sections": ["factor_values", "full_universe_rows", "ic_time_series"],
    }


@router.post("/validate-inputs")
def validate_inputs(req: FactorMetricsPlanRequest) -> dict[str, Any]:
    plan = plan_metrics(req)
    blockers: list[str] = []
    if req.factor_names is not None and not plan["eligible_preview"]:
        blockers.append("no_requested_factors_found")
    return {
        "ok": not blockers,
        "domain": "factor_metrics",
        "blockers": blockers,
        "warnings": list(plan["cache_preflight"].get("blockers") or []),
        "plan": plan,
    }


@router.post("/submit-confirmed")
def submit_confirmed(req: FactorMetricsSubmitRequest) -> dict[str, Any]:
    if req.confirm != SUBMIT_FACTOR_METRICS_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": SUBMIT_FACTOR_METRICS_CONFIRM})
    options = dict(req.options or {})
    if req.factor_names is not None:
        options["factor_names"] = [str(name).strip() for name in req.factor_names if str(name).strip()]
    options["one_shot"] = True
    job_id = factor_metrics_scheduler.submit_job(req.schedule_id, req.dataset, options, triggered_by="mcp")
    return {"ok": True, "domain": "factor_metrics", "job_id": str(job_id), "status": "submitted", "confirmation": SUBMIT_FACTOR_METRICS_CONFIRM, "detail_tool": "aistock-factor-metrics/factor_metrics_get_job"}


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    row = _job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail={"error": "job_not_found", "job_id": job_id})
    summary = row.get("summary") or {}
    if isinstance(summary, str):
        summary = {"raw_summary_ref": artifact_ref("ingestion_job_summary", f"market.ingestion_jobs:{job_id}:summary")}
    row["summary"] = strip_forbidden_fields(summary)
    return {"ok": True, "domain": "factor_metrics", "job": strip_forbidden_fields(row), "omitted_sections": ["dispatch_full_log", "raw_result_payload"]}


@router.get("/results")
def get_result(
    factor_name: str | None = None,
    calc_batch_id: str | None = None,
    eval_window: str | None = None,
    limit: int | None = Query(None, ge=1),
    offset: int | None = Query(0, ge=0),
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    clauses = ["calc_engine = %s"]
    params: list[Any] = [CALC_ENGINE]
    if factor_name:
        clauses.append("factor_name = %s")
        params.append(factor_name)
    if calc_batch_id:
        clauses.append("calc_batch_id = %s")
        params.append(calc_batch_id)
    if eval_window:
        clauses.append("eval_window = %s")
        params.append(eval_window)
    where = " AND ".join(clauses)
    rows = _rows(
        f"""
        SELECT id, factor_name, eval_window, return_horizon, universe, ic_mean, rank_ic_mean,
               icir, rank_icir, ic_positive_ratio, top_excess_sharpe, top_excess_annual_return,
               top_max_drawdown, group_return_monotonicity, turnover, coverage, n_trading_days,
               h20_return_horizon, h20_ic_mean, h20_ic_std,
               h20_rank_ic_mean, h20_rank_ic_std, h20_icir, h20_rank_icir,
               h20_icir_hac, h20_rank_icir_hac, h20_ic_positive_ratio,
               h20_n_obs, h20_hac_lag,
               snapshot_date, calc_batch_id, calculated_at
        FROM aistock_factor_metrics
        WHERE {where}
        ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST, factor_name ASC
        LIMIT %s OFFSET %s
        """,
        tuple([*params, safe_limit, safe_offset]),
    )
    total = _one(f"SELECT COUNT(*) AS total FROM aistock_factor_metrics WHERE {where}", tuple(params))
    return summary_envelope(
        domain="factor_metrics.result",
        items=rows,
        total=int((total or {}).get("total") or len(rows)),
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["ic_time_series", "monthly_ic_rows", "group_return_details", "raw_factor_values"],
        detail_tool="aistock-factor-library/factor_library_get_metric_summary",
        detail_args_hint={"factor_name": factor_name or "<factor_name>"},
    )


@router.get("/compare-versions")
def compare_versions(factor_name: str, limit: int | None = Query(20, ge=1)) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    rows = _rows(
        """
        SELECT factor_name, eval_window, snapshot_date, calc_batch_id, ic_mean, rank_ic_mean,
               icir, rank_icir, coverage, n_trading_days,
               h20_return_horizon, h20_ic_mean, h20_ic_std,
               h20_rank_ic_mean, h20_rank_ic_std, h20_icir, h20_rank_icir,
               h20_icir_hac, h20_rank_icir_hac, h20_ic_positive_ratio,
               h20_n_obs, h20_hac_lag, calculated_at
        FROM aistock_factor_metrics
        WHERE factor_name = %s AND calc_engine = %s
        ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST
        LIMIT %s
        """,
        (factor_name, CALC_ENGINE, safe_limit),
    )
    return summary_envelope(domain="factor_metrics.compare_versions", items=rows, total=len(rows), limit=safe_limit, omitted_sections=["full_metric_json", "time_series"])


@router.get("/export-result-ref")
def export_result_ref(factor_name: str | None = None, calc_batch_id: str | None = None) -> dict[str, Any]:
    key = calc_batch_id or factor_name or "latest"
    return {
        "ok": True,
        "domain": "factor_metrics.export_ref",
        "artifact_ref": artifact_ref("factor_metrics_result", f"aistock_factor_metrics:{key}", {"factor_name": factor_name, "calc_batch_id": calc_batch_id, "inline": False}),
        "omitted_sections": ["full_result_rows", "ic_time_series", "group_return_details"],
    }
