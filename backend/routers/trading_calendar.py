"""Official AIstock trading calendar status API."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError

router = APIRouter(prefix="/trading-calendar", tags=["trading-calendar"])


def get_trading_calendar_status_service() -> TradingCalendarStatusService:
    return TradingCalendarStatusService()


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 404 if isinstance(exc, DataUnavailableError) else 400
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


@router.get("/status")
def get_trading_calendar_status(
    as_of_date: date | None = None,
    service: TradingCalendarStatusService = Depends(get_trading_calendar_status_service),
) -> dict[str, Any]:
    try:
        return service.status(as_of_date=as_of_date)
    except TradingCoreError as exc:
        _raise_http(exc)
