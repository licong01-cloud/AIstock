"""Factor correlation facade for MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.mcp_payload_budget import artifact_ref, clamp_limit, clamp_offset, strip_forbidden_fields, summary_envelope
from backend.services.quantevolver.correlation_scheduler import correlation_scheduler
from backend.services.quantevolver.factor_compute_preflight import build_official_cache_preflight
from backend.services.quantevolver.factor_metrics_scheduler import OFFICIAL_FACTOR_WINDOW_END

router = APIRouter(prefix="/factor-correlation", tags=["factor-correlation"])
SUBMIT_FACTOR_CORRELATION_CONFIRM = "SUBMIT_FACTOR_CORRELATION"


class FactorCorrelationPlanRequest(BaseModel):
    factor_names: list[str] | None = None
    dataset: str = "correlation_full"
    method: str = "spearman_ewma"
    options: dict[str, Any] = Field(default_factory=dict)


class FactorCorrelationSubmitRequest(FactorCorrelationPlanRequest):
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


@router.post("/plan")
def plan_correlation(req: FactorCorrelationPlanRequest) -> dict[str, Any]:
    names = [str(name).strip() for name in (req.factor_names or []) if str(name).strip()]
    if names:
        found = _rows("SELECT factor_name, source, is_available FROM aistock_factor_catalog WHERE factor_name = ANY(%s) ORDER BY factor_name", (names,))
        count = sum(bool(row.get("is_available")) for row in found)
    else:
        found = _rows("SELECT factor_name, source, is_available FROM aistock_factor_catalog WHERE is_available = TRUE ORDER BY factor_name LIMIT 20")
        count_row = _one("SELECT COUNT(*) AS total FROM aistock_factor_catalog WHERE is_available = TRUE") or {}
        count = int(count_row.get("total") or 0)
    target_end = str(
        req.options.get("as_of_date")
        or req.options.get("end_date")
        or req.options.get("data_date")
        or OFFICIAL_FACTOR_WINDOW_END
    )
    cache_preflight = build_official_cache_preflight(
        target_end=target_end,
        eligible_factor_count=count,
    )
    return {
        "ok": True,
        "domain": "factor_correlation",
        "plan_type": "submit_factor_correlation",
        "dataset": req.dataset,
        "method": req.method,
        "eligible_preview": strip_forbidden_fields(found[:20]),
        "eligible_factor_count": count,
        "eligible_factor_count_preview": min(len(found), 20),
        "estimated_pair_count": max(0, count * (count - 1) // 2),
        "target_end": target_end,
        "cache_preflight": cache_preflight,
        "required_confirmation": SUBMIT_FACTOR_CORRELATION_CONFIRM,
        "async_job": True,
        "matrix_inline_allowed": False,
        "omitted_sections": ["full_correlation_matrix", "factor_value_panel", "daily_pair_correlations"],
    }


@router.post("/validate-inputs")
def validate_inputs(req: FactorCorrelationPlanRequest) -> dict[str, Any]:
    plan = plan_correlation(req)
    blockers: list[str] = []
    if int(plan["eligible_factor_count"] or 0) < 2:
        blockers.append("at_least_two_factors_required")
    blockers.extend(plan["cache_preflight"].get("blockers") or [])
    return {"ok": not blockers, "domain": "factor_correlation", "blockers": blockers, "plan": plan}


@router.post("/submit-confirmed")
def submit_confirmed(req: FactorCorrelationSubmitRequest) -> dict[str, Any]:
    if req.confirm != SUBMIT_FACTOR_CORRELATION_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": SUBMIT_FACTOR_CORRELATION_CONFIRM})
    options = dict(req.options or {})
    if req.factor_names is not None:
        options["factor_names"] = [str(name).strip() for name in req.factor_names if str(name).strip()]
    options["method"] = req.method
    options["one_shot"] = True
    job_id = correlation_scheduler.submit_job(req.schedule_id, req.dataset, options, triggered_by="mcp")
    return {"ok": True, "domain": "factor_correlation", "job_id": str(job_id), "status": "submitted", "confirmation": SUBMIT_FACTOR_CORRELATION_CONFIRM, "detail_tool": "aistock-factor-correlation/factor_corr_get_job"}


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    row = _one(
        """
        SELECT job_id::text, job_type, status, created_at, started_at, completed_at, summary
        FROM market.ingestion_jobs
        WHERE job_id::text = %s
        """,
        (job_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail={"error": "job_not_found", "job_id": job_id})
    row["summary"] = strip_forbidden_fields(row.get("summary") or {})
    return {"ok": True, "domain": "factor_correlation", "job": strip_forbidden_fields(row), "omitted_sections": ["full_dispatch_log", "full_matrix"]}


@router.get("/top-pairs")
def get_top_pairs(
    min_abs_corr: float = Query(0.7, ge=0, le=1),
    method: str | None = None,
    limit: int | None = Query(None, ge=1),
    offset: int | None = Query(0, ge=0),
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    clauses = ["ABS(q.correlation) >= %s"]
    params: list[Any] = [min_abs_corr]
    if method:
        clauses.append("q.method = %s")
        params.append(method)
    where = " AND ".join(clauses)
    rows = _rows(
        f"""
        SELECT q.id, ca.factor_name AS factor_a, cb.factor_name AS factor_b, q.correlation,
               q.method, q.as_of_date, q.data_window_days, q.computed_at, q.universe,
               q.universe_rule_version
        FROM qe_factor_correlations q
        JOIN aistock_factor_catalog ca ON ca.id = q.factor_a_id
        JOIN aistock_factor_catalog cb ON cb.id = q.factor_b_id
        WHERE {where}
        ORDER BY ABS(q.correlation) DESC, q.computed_at DESC NULLS LAST
        LIMIT %s OFFSET %s
        """,
        tuple([*params, safe_limit, safe_offset]),
    )
    total = _one(
        f"SELECT COUNT(*) AS total FROM qe_factor_correlations q WHERE {where}",
        tuple(params),
    )
    return summary_envelope(
        domain="factor_correlation.top_pairs",
        items=rows,
        total=int((total or {}).get("total") or len(rows)),
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["daily_correlations", "full_matrix"],
        detail_tool="aistock-factor-correlation/factor_corr_get_matrix_ref",
        detail_args_hint={"method": method, "as_of_date": "<as_of_date>"},
    )


@router.get("/clusters")
def get_clusters(min_abs_corr: float = Query(0.7, ge=0, le=1), limit: int | None = Query(None, ge=1)) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    pairs = _rows(
        """
        SELECT ca.factor_name AS factor_a, cb.factor_name AS factor_b, q.correlation, q.as_of_date, q.method
        FROM qe_factor_correlations q
        JOIN aistock_factor_catalog ca ON ca.id = q.factor_a_id
        JOIN aistock_factor_catalog cb ON cb.id = q.factor_b_id
        WHERE ABS(q.correlation) >= %s
        ORDER BY ABS(q.correlation) DESC
        LIMIT %s
        """,
        (min_abs_corr, safe_limit * 5),
    )
    clusters: dict[str, set[str]] = {}
    for row in pairs:
        anchor = row["factor_a"]
        clusters.setdefault(anchor, set()).update([row["factor_a"], row["factor_b"]])
    items = [{"cluster_id": key, "factor_count": len(values), "factors_preview": sorted(values)[:20]} for key, values in list(clusters.items())[:safe_limit]]
    return summary_envelope(domain="factor_correlation.clusters", items=items, total=len(items), limit=safe_limit, omitted_sections=["full_graph", "full_matrix"])


@router.get("/suggest-replacements")
def suggest_replacements(factor_name: str, max_abs_corr: float = Query(0.4, ge=0, le=1), limit: int | None = Query(None, ge=1)) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    rows = _rows(
        """
        WITH target AS (SELECT id FROM aistock_factor_catalog WHERE factor_name = %s LIMIT 1),
        correlated AS (
            SELECT CASE WHEN factor_a_id = target.id THEN factor_b_id ELSE factor_a_id END AS other_id,
                   ABS(correlation) AS abs_corr
            FROM qe_factor_correlations, target
            WHERE factor_a_id = target.id OR factor_b_id = target.id
        )
        SELECT c.factor_name, c.source, c.factor_type, c.data_source, COALESCE(corr.abs_corr, 0) AS abs_corr_to_target,
               m.rank_ic_mean, m.icir, m.coverage, m.snapshot_date
        FROM aistock_factor_catalog c
        LEFT JOIN correlated corr ON corr.other_id = c.id
        LEFT JOIN LATERAL (
            SELECT rank_ic_mean, icir, coverage, snapshot_date
            FROM aistock_factor_metrics m
            WHERE m.factor_name = c.factor_name AND m.calc_engine = 'qe_eval_v2'
            ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST
            LIMIT 1
        ) m ON TRUE
        WHERE c.factor_name <> %s AND COALESCE(corr.abs_corr, 0) <= %s AND COALESCE(c.is_available, TRUE) = TRUE
        ORDER BY m.rank_ic_mean DESC NULLS LAST, m.icir DESC NULLS LAST, c.factor_name ASC
        LIMIT %s
        """,
        (factor_name, factor_name, max_abs_corr, safe_limit),
    )
    return summary_envelope(domain="factor_correlation.replacements", items=rows, total=len(rows), limit=safe_limit, omitted_sections=["full_factor_metrics", "factor_values"])


@router.get("/matrix-ref")
def get_matrix_ref(as_of_date: str | None = None) -> dict[str, Any]:
    if as_of_date:
        row = _one("SELECT * FROM qe_correlation_metadata WHERE as_of_date = %s ORDER BY created_at DESC LIMIT 1", (as_of_date,))
    else:
        row = _one("SELECT * FROM qe_correlation_metadata ORDER BY as_of_date DESC NULLS LAST, created_at DESC LIMIT 1")
    if not row:
        return {"ok": True, "domain": "factor_correlation.matrix_ref", "artifact_ref": None, "message": "no correlation metadata found", "matrix_inline_allowed": False}
    return {
        "ok": True,
        "domain": "factor_correlation.matrix_ref",
        "matrix_inline_allowed": False,
        "metadata": strip_forbidden_fields(row),
        "artifact_ref": artifact_ref("factor_correlation_matrix", row.get("hdf5_path") or f"qe_correlation_metadata:{row.get('id')}", {"as_of_date": row.get("as_of_date"), "num_factors": row.get("num_factors")}),
        "omitted_sections": ["full_correlation_matrix"],
    }
