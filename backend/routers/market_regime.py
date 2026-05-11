"""Market regime label read-only APIs.

Surfaces ``market.regime_label`` rows for the frontend market-regime page.
Read-only by design: there is no INSERT/UPDATE/DELETE here. The daily
classification job lives in ``scripts/regime_label_daily.py``.
"""

from __future__ import annotations

from datetime import date as date_type
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db.pg_pool import get_conn

REGIME_VALUES = ("bull", "bear", "oscillation", "high_vol", "low_vol")
SOURCE_METHODS = ("simple_quadrant", "hmm_viterbi", "bbq", "ensemble")

router = APIRouter(prefix="/market/regime-label", tags=["market-regime"])


class RegimeLabel(BaseModel):
    trade_date: date_type
    regime: Literal["bull", "bear", "oscillation", "high_vol", "low_vol"]
    regime_confidence: float | None = None
    source_method: str
    source_signal_json: dict[str, Any] | None = None
    labeled_at: str | None = None


class RegimeDistributionItem(BaseModel):
    regime: str
    count: int
    pct: float = Field(..., description="Percentage of total in window, 0..1")


class RegimeDistributionResponse(BaseModel):
    source_method: str
    start_date: date_type | None
    end_date: date_type | None
    total: int
    items: list[RegimeDistributionItem]


class RegimeTimelineResponse(BaseModel):
    source_method: str
    items: list[RegimeLabel]


def _normalize_source_method(value: str) -> str:
    if value not in SOURCE_METHODS:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported source_method {value!r}; expected one of {SOURCE_METHODS}",
        )
    return value


@router.get("/methods")
def list_source_methods() -> dict[str, list[str]]:
    """Return the four supported source_method values plus the distinct
    methods currently present in ``market.regime_label`` so the UI can
    grey-out methods without any data."""
    available: list[str] = []
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT source_method FROM market.regime_label ORDER BY source_method"
        )
        available = [row[0] for row in cur.fetchall()]
    return {"supported": list(SOURCE_METHODS), "available": available}


@router.get("/timeline", response_model=RegimeTimelineResponse)
def get_timeline(
    source_method: str = Query("simple_quadrant"),
    start_date: date_type | None = Query(None),
    end_date: date_type | None = Query(None),
    limit: int = Query(2500, ge=1, le=10000),
) -> RegimeTimelineResponse:
    method = _normalize_source_method(source_method)
    clauses = ["source_method = %s"]
    params: list[Any] = [method]
    if start_date is not None:
        clauses.append("trade_date >= %s")
        params.append(start_date)
    if end_date is not None:
        clauses.append("trade_date <= %s")
        params.append(end_date)
    where = " AND ".join(clauses)
    sql = (
        "SELECT trade_date, regime, regime_confidence, source_method, "
        "source_signal_json, labeled_at "
        f"FROM market.regime_label WHERE {where} "
        "ORDER BY trade_date ASC LIMIT %s"
    )
    params.append(limit)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    items = [
        RegimeLabel(
            trade_date=row[0],
            regime=row[1],
            regime_confidence=float(row[2]) if row[2] is not None else None,
            source_method=row[3],
            source_signal_json=row[4],
            labeled_at=row[5].isoformat() if row[5] else None,
        )
        for row in rows
    ]
    return RegimeTimelineResponse(source_method=method, items=items)


@router.get("/distribution", response_model=RegimeDistributionResponse)
def get_distribution(
    source_method: str = Query("simple_quadrant"),
    start_date: date_type | None = Query(None),
    end_date: date_type | None = Query(None),
) -> RegimeDistributionResponse:
    method = _normalize_source_method(source_method)
    clauses = ["source_method = %s"]
    params: list[Any] = [method]
    if start_date is not None:
        clauses.append("trade_date >= %s")
        params.append(start_date)
    if end_date is not None:
        clauses.append("trade_date <= %s")
        params.append(end_date)
    where = " AND ".join(clauses)
    sql = (
        "SELECT regime, COUNT(*) "
        f"FROM market.regime_label WHERE {where} "
        "GROUP BY regime"
    )
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    counts: dict[str, int] = {regime: 0 for regime in REGIME_VALUES}
    total = 0
    for regime, count in rows:
        counts[regime] = int(count)
        total += int(count)
    items = [
        RegimeDistributionItem(
            regime=regime,
            count=counts[regime],
            pct=(counts[regime] / total) if total else 0.0,
        )
        for regime in REGIME_VALUES
    ]
    return RegimeDistributionResponse(
        source_method=method,
        start_date=start_date,
        end_date=end_date,
        total=total,
        items=items,
    )


@router.get("/current", response_model=RegimeLabel | None)
def get_current(source_method: str = Query("simple_quadrant")) -> RegimeLabel | None:
    method = _normalize_source_method(source_method)
    sql = (
        "SELECT trade_date, regime, regime_confidence, source_method, "
        "source_signal_json, labeled_at "
        "FROM market.regime_label WHERE source_method = %s "
        "ORDER BY trade_date DESC LIMIT 1"
    )
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (method,))
        row = cur.fetchone()
    if row is None:
        return None
    return RegimeLabel(
        trade_date=row[0],
        regime=row[1],
        regime_confidence=float(row[2]) if row[2] is not None else None,
        source_method=row[3],
        source_signal_json=row[4],
        labeled_at=row[5].isoformat() if row[5] else None,
    )
