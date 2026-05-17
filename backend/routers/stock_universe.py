from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_REFRESH_POLICY,
    DEFAULT_ST_PIT_RULE_VERSION,
    DEFAULT_ST_PIT_START_DATE,
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    StockUniversePitError,
    StockUniversePitService,
)


router = APIRouter(prefix="/stock-universe", tags=["stock-universe"])


class StPitEnsureRequest(BaseModel):
    universe_key: str = Field(DEFAULT_ST_PIT_UNIVERSE_KEY)
    start_date: dt.date = Field(DEFAULT_ST_PIT_START_DATE)
    end_date: Optional[dt.date] = Field(None)
    rule_version: str = Field(DEFAULT_ST_PIT_RULE_VERSION)
    force: bool = Field(False)
    rebuild_if_stale: bool = Field(True)
    refresh_policy: str = Field(
        DEFAULT_ST_PIT_REFRESH_POLICY,
        description="coverage reuses a valid PIT cache for the same covered date range; source_fingerprint forces source-change refresh.",
    )


class StPitRebuildRequest(BaseModel):
    universe_key: str = Field(DEFAULT_ST_PIT_UNIVERSE_KEY)
    start_date: dt.date = Field(DEFAULT_ST_PIT_START_DATE)
    end_date: Optional[dt.date] = Field(None)
    rule_version: str = Field(DEFAULT_ST_PIT_RULE_VERSION)


@router.get("/st-pit/status")
def st_pit_status(universe_key: str = Query(DEFAULT_ST_PIT_UNIVERSE_KEY)) -> dict[str, Any]:
    service = StockUniversePitService()
    return service.get_status(universe_key=universe_key)


@router.post("/st-pit/ensure")
def ensure_st_pit_universe(body: StPitEnsureRequest) -> dict[str, Any]:
    service = StockUniversePitService()
    try:
        return service.ensure_st_pit_universe(
            universe_key=body.universe_key,
            start_date=body.start_date,
            end_date=body.end_date,
            rule_version=body.rule_version,
            force=body.force,
            strict=True,
            rebuild_if_stale=body.rebuild_if_stale,
            refresh_policy=body.refresh_policy,
        )
    except StockUniversePitError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/st-pit/rebuild")
def rebuild_st_pit_universe(body: StPitRebuildRequest) -> dict[str, Any]:
    service = StockUniversePitService()
    try:
        return service.rebuild_st_pit_universe(
            universe_key=body.universe_key,
            start_date=body.start_date,
            end_date=body.end_date,
            rule_version=body.rule_version,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/st-pit/eligible-codes")
def st_pit_eligible_codes(
    trade_date: dt.date = Query(...),
    universe_key: str = Query(DEFAULT_ST_PIT_UNIVERSE_KEY),
    ensure: bool = Query(True),
) -> dict[str, Any]:
    service = StockUniversePitService()
    try:
        codes = service.get_eligible_codes(
            trade_date=trade_date,
            universe_key=universe_key,
            ensure=ensure,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "universe_key": universe_key,
        "trade_date": trade_date.isoformat(),
        "count": len(codes),
        "codes": codes,
    }
