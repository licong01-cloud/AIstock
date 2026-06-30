"""Summary-first factor library facade for MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.mcp_payload_budget import artifact_ref, clamp_limit, clamp_offset, detail_ref, strip_forbidden_fields, summary_envelope
from backend.services.strategy_package.factor_usage import (
    STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
    StrategyPackageFactorUsageQueryError,
    find_strategy_package_factor_usage,
)

router = APIRouter(prefix="/factor-library", tags=["factor-library"])
REGISTER_FACTOR_CONFIRM = "REGISTER_FACTOR"
DEPRECATE_FACTOR_CONFIRM = "DEPRECATE_FACTOR"
CALC_ENGINE = "qe_eval_v2"

SUMMARY_FIELDS = [
    "id", "factor_name", "source", "region", "tags", "description_cn", "freq", "align", "nan_policy",
    "created_at_utc", "factor_type", "data_source", "is_available", "transformation_status",
]
METRIC_FIELDS = [
    "ic_mean", "rank_ic_mean", "icir", "rank_icir", "top_excess_sharpe", "top_excess_annual_return",
    "top_max_drawdown", "coverage", "n_trading_days", "snapshot_date", "calc_batch_id",
]


class FactorRegisterPlanRequest(BaseModel):
    factor_name: str = Field(min_length=1)
    source: str = "manual"
    metadata: dict[str, Any] = Field(default_factory=dict)


class FactorRegisterConfirmedRequest(FactorRegisterPlanRequest):
    confirm: str | None = None
    operator: str = "mcp_factor_library"


class FactorDeprecatePlanRequest(BaseModel):
    factor_name: str = Field(min_length=1)
    source: str | None = None
    reason: str = Field(min_length=1)


class FactorDeprecateConfirmedRequest(FactorDeprecatePlanRequest):
    confirm: str | None = None
    operator: str = "mcp_factor_library"


def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def _one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def _summary_select(where: str, params: list[Any], *, limit: int, offset: int) -> list[dict[str, Any]]:
    sql = f"""
        WITH latest_metrics AS (
            SELECT DISTINCT ON (factor_name) factor_name,
                   ic_mean, rank_ic_mean, icir, rank_icir, top_excess_sharpe,
                   top_excess_annual_return, top_max_drawdown, coverage,
                   n_trading_days, snapshot_date, calc_batch_id
            FROM aistock_factor_metrics
            WHERE calc_engine = %s
            ORDER BY factor_name, snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST, id DESC
        ), corr AS (
            SELECT factor_id, COUNT(*)::int AS correlation_pair_count, MAX(computed_at) AS correlation_computed_at
            FROM (
                SELECT factor_a_id AS factor_id, computed_at FROM qe_factor_correlations
                UNION ALL
                SELECT factor_b_id AS factor_id, computed_at FROM qe_factor_correlations
            ) q
            GROUP BY factor_id
        )
        SELECT c.id, c.factor_name, c.source, c.region, c.tags, c.description_cn, c.freq, c.align, c.nan_policy,
               c.created_at_utc, c.factor_type, c.data_source, c.is_available, c.transformation_status,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir, m.top_excess_sharpe,
               m.top_excess_annual_return, m.top_max_drawdown, m.coverage, m.n_trading_days,
               m.snapshot_date, m.calc_batch_id,
               corr.correlation_pair_count, corr.correlation_computed_at
        FROM aistock_factor_catalog c
        LEFT JOIN latest_metrics m ON m.factor_name = c.factor_name
        LEFT JOIN corr ON corr.factor_id = c.id
        {where}
        ORDER BY c.is_available DESC NULLS LAST, c.factor_name ASC, c.source ASC
        LIMIT %s OFFSET %s
    """
    return _rows(sql, (CALC_ENGINE, *params, limit, offset))


def _count(where: str, params: list[Any]) -> int:
    row = _one(f"SELECT COUNT(*) AS total FROM aistock_factor_catalog c {where}", tuple(params))
    return int((row or {}).get("total") or 0)


def _where(*, search: str | None = None, source: str | None = None, is_available: bool | None = None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if search:
        clauses.append("(c.factor_name ILIKE %s OR c.description_cn ILIKE %s OR c.source ILIKE %s)")
        like = f"%{search}%"
        params.extend([like, like, like])
    if source:
        clauses.append("c.source = %s")
        params.append(source)
    if is_available is not None:
        clauses.append("c.is_available = %s")
        params.append(is_available)
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def _strategy_package_usage_or_http(factor_name: str, *, limit: int = 20) -> dict[str, Any]:
    try:
        return find_strategy_package_factor_usage(factor_name, limit=limit)
    except StrategyPackageFactorUsageQueryError as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
                "reason_code": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
                "message": "strategy package factor usage lookup failed",
                "factor_name": factor_name,
                "context": exc.context,
            },
        ) from exc


@router.get("/factors")
def list_factors(
    search: str | None = None,
    source: str | None = None,
    is_available: bool | None = None,
    limit: int | None = Query(None, ge=1),
    offset: int | None = Query(0, ge=0),
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    where, params = _where(search=search, source=source, is_available=is_available)
    items = _summary_select(where, params, limit=safe_limit, offset=safe_offset)
    total = _count(where, params)
    return summary_envelope(
        domain="factor_library",
        items=items,
        total=total,
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["raw_payload", "performance_metrics", "interface_info", "full_metric_series", "factor_values"],
        detail_tool="aistock-factor-library/factor_library_get",
        detail_args_hint={"factor_name": "<factor_name>", "source": "<source>"},
    )


@router.get("/factors/search")
def search_factors(q: str, limit: int | None = Query(None, ge=1), offset: int | None = Query(0, ge=0)) -> dict[str, Any]:
    return list_factors(search=q, limit=limit, offset=offset)


@router.get("/factors/{factor_name}")
def get_factor(factor_name: str, source: str | None = None) -> dict[str, Any]:
    where, params = _where(search=None, source=source)
    suffix = " AND c.factor_name = %s" if where else "WHERE c.factor_name = %s"
    row = _one(f"SELECT * FROM aistock_factor_catalog c {where}{suffix} ORDER BY c.source ASC LIMIT 1", tuple([*params, factor_name]))
    if not row:
        raise HTTPException(status_code=404, detail={"error": "factor_not_found", "factor_name": factor_name, "source": source})
    metric_rows = _rows(
        """
        SELECT eval_window, return_horizon, universe, ic_mean, rank_ic_mean, icir, rank_icir,
               top_excess_sharpe, top_excess_annual_return, top_max_drawdown, coverage,
               n_trading_days, snapshot_date, calc_batch_id, calculated_at
        FROM aistock_factor_metrics
        WHERE factor_name = %s AND calc_engine = %s
        ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST
        LIMIT 20
        """,
        (factor_name, CALC_ENGINE),
    )
    corr_count = _one(
        """
        SELECT COUNT(*) AS pair_count, MAX(computed_at) AS computed_at
        FROM qe_factor_correlations q
        JOIN aistock_factor_catalog c ON c.id IN (q.factor_a_id, q.factor_b_id)
        WHERE c.factor_name = %s
        """,
        (factor_name,),
    ) or {}
    heavy_refs = []
    if row.get("asset_bundle_id"):
        heavy_refs.append(artifact_ref("factor_asset_bundle", row.get("asset_bundle_id"), {"factor_name": factor_name}))
    if row.get("raw_payload"):
        heavy_refs.append(artifact_ref("factor_raw_payload", f"aistock_factor_catalog:{row.get('id')}:raw_payload", {"detail_endpoint": "factor_library_get"}))
    safe_detail = strip_forbidden_fields(row)
    return {
        "ok": True,
        "domain": "factor_library",
        "factor": safe_detail,
        "metric_summary": strip_forbidden_fields(metric_rows),
        "correlation_summary": strip_forbidden_fields(corr_count),
        "artifact_refs": heavy_refs,
        "omitted_sections": ["raw_payload", "performance_metrics", "interface_info", "full_metric_series", "factor_values"],
        "detail_refs": [detail_ref("aistock-factor-metrics", "factor_metrics_get_result", {"factor_name": factor_name})],
    }


@router.get("/factors/{factor_name}/coverage")
def get_coverage(factor_name: str) -> dict[str, Any]:
    rows = _rows(
        """
        SELECT eval_window, coverage, coverage_numerator, coverage_denominator, coverage_semantics,
               universe_rule_version, universe_fingerprint_sha256, snapshot_date, calculated_at
        FROM aistock_factor_metrics
        WHERE factor_name = %s AND calc_engine = %s
        ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST
        LIMIT 20
        """,
        (factor_name, CALC_ENGINE),
    )
    return summary_envelope(
        domain="factor_library.coverage",
        items=rows,
        total=len(rows),
        omitted_sections=["factor_value_rows", "coverage_denominator_members"],
        detail_tool="aistock-factor-library/factor_library_get",
        detail_args_hint={"factor_name": factor_name},
    )


@router.get("/factors/{factor_name}/metric-summary")
def get_metric_summary(factor_name: str) -> dict[str, Any]:
    rows = _rows(
        """
        SELECT eval_window, return_horizon, universe, ic_mean, rank_ic_mean, icir, rank_icir,
               ic_positive_ratio, top_excess_sharpe, top_excess_annual_return, top_max_drawdown,
               group_return_monotonicity, turnover, coverage, n_trading_days, snapshot_date,
               calc_batch_id, calculated_at
        FROM aistock_factor_metrics
        WHERE factor_name = %s AND calc_engine = %s
        ORDER BY snapshot_date DESC NULLS LAST, calculated_at DESC NULLS LAST
        LIMIT 20
        """,
        (factor_name, CALC_ENGINE),
    )
    return summary_envelope(domain="factor_library.metric_summary", items=rows, total=len(rows), omitted_sections=["ic_time_series", "group_return_rows"])


@router.get("/factors/{factor_name}/usage-summary")
def get_usage_summary(factor_name: str, limit: int | None = Query(None, ge=1)) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    metric_rows = _rows(
        """
        SELECT factor_name, COUNT(*)::int AS metric_version_count, MAX(calculated_at) AS latest_metric_at,
               COUNT(DISTINCT calc_batch_id)::int AS calc_batch_count
        FROM aistock_factor_metrics
        WHERE factor_name = %s AND calc_engine = %s
        GROUP BY factor_name
        """,
        (factor_name, CALC_ENGINE),
    )
    package_usage = _strategy_package_usage_or_http(factor_name, limit=safe_limit)
    return summary_envelope(
        domain="factor_library.usage_summary",
        items=metric_rows[:safe_limit],
        total=len(metric_rows),
        limit=safe_limit,
        omitted_sections=["qe_archive_full_usage", "strategy_package_full_usage"],
        detail_tool="aistock-qe-archive/qe_archive_query_factor_usage",
        detail_args_hint={"factor_name": factor_name},
        extra={
            "strategy_package_usage": {
                "factor_name": factor_name,
                "protected": package_usage["protected"],
                "reason_code": package_usage["reason_code"],
                "package_count": package_usage["package_count"],
                "reference_count": package_usage["reference_count"],
                "sample_references": package_usage["references"][:safe_limit],
                "query_sources": package_usage["query_sources"],
            }
        },
    )


@router.post("/register-plan")
def plan_register(req: FactorRegisterPlanRequest) -> dict[str, Any]:
    existing = _one("SELECT factor_name, source, is_available FROM aistock_factor_catalog WHERE factor_name=%s AND source=%s", (req.factor_name, req.source))
    return {
        "ok": True,
        "domain": "factor_library",
        "plan_type": "register_factor",
        "factor_name": req.factor_name,
        "source": req.source,
        "existing": strip_forbidden_fields(existing or {}),
        "will_write": existing is None,
        "required_confirmation": REGISTER_FACTOR_CONFIRM,
        "preflight": {"duplicate_check": "found" if existing else "clear", "metadata_summary": strip_forbidden_fields(req.metadata)},
    }


@router.post("/register-confirmed")
def register_confirmed(req: FactorRegisterConfirmedRequest) -> dict[str, Any]:
    if req.confirm != REGISTER_FACTOR_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": REGISTER_FACTOR_CONFIRM})
    metadata = strip_forbidden_fields(req.metadata)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO aistock_factor_catalog (
                    factor_name, source, catalog_version, generated_at_utc, catalog_source,
                    region, tags, description_cn, freq, is_available, raw_payload
                ) VALUES (%s, %s, COALESCE(%s, 'mcp'), NOW()::text, 'mcp_factor_library', %s, %s, %s, %s, TRUE, %s)
                ON CONFLICT (factor_name, source) DO UPDATE
                SET is_available = TRUE, updated_at = NOW()
                RETURNING factor_name, source, is_available, updated_at
                """,
                (
                    req.factor_name,
                    req.source,
                    metadata.get("catalog_version"),
                    metadata.get("region"),
                    metadata.get("tags"),
                    metadata.get("description_cn"),
                    metadata.get("freq"),
                    Json(metadata),
                ),
            )
            row = dict(cur.fetchone())
        conn.commit()
    return {"ok": True, "registered": strip_forbidden_fields(row), "confirmation": REGISTER_FACTOR_CONFIRM}


@router.post("/deprecate-plan")
def plan_deprecate(req: FactorDeprecatePlanRequest) -> dict[str, Any]:
    where, params = _where(source=req.source)
    suffix = " AND c.factor_name = %s" if where else "WHERE c.factor_name = %s"
    row = _one(f"SELECT factor_name, source, is_available FROM aistock_factor_catalog c {where}{suffix} LIMIT 1", tuple([*params, req.factor_name]))
    package_usage = _strategy_package_usage_or_http(req.factor_name)
    return {
        "ok": True,
        "domain": "factor_library",
        "plan_type": "deprecate_factor",
        "factor": strip_forbidden_fields(row or {}),
        "reason": req.reason,
        "required_confirmation": DEPRECATE_FACTOR_CONFIRM,
        "will_write": row is not None,
        "strategy_package_usage": {
            "protected": package_usage["protected"],
            "reason_code": package_usage["reason_code"],
            "package_count": package_usage["package_count"],
            "reference_count": package_usage["reference_count"],
            "sample_references": package_usage["references"][:5],
        },
        "deprecate_policy": "allowed_even_when_referenced_by_strategy_package",
    }


@router.post("/deprecate-confirmed")
def deprecate_confirmed(req: FactorDeprecateConfirmedRequest) -> dict[str, Any]:
    if req.confirm != DEPRECATE_FACTOR_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": DEPRECATE_FACTOR_CONFIRM})
    where, params = _where(source=req.source)
    suffix = " AND factor_name = %s" if where else "WHERE factor_name = %s"
    sql_where = (where + suffix).replace("c.", "")
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"UPDATE aistock_factor_catalog SET is_available=FALSE, updated_at=NOW() {sql_where} RETURNING factor_name, source, is_available, updated_at", tuple([*params, req.factor_name]))
            rows = [dict(row) for row in cur.fetchall()]
        conn.commit()
    return {"ok": True, "deprecated": strip_forbidden_fields(rows), "reason": req.reason, "confirmation": DEPRECATE_FACTOR_CONFIRM}
