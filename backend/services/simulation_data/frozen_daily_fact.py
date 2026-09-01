"""Source-neutral parser for one frozen daily symbol fact reference."""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from backend.services.simulation_data.daily_context import (
    DailyTradingAuthorityStateV2,
    DailyTradingSymbolFactV1,
    DailyTradingSymbolFactV2,
)
from backend.services.trading_core.errors import DataUnavailableError


def parse_frozen_daily_symbol_fact(
    symbol: str,
    trade_date: date,
    reference: Mapping[str, Any],
) -> DailyTradingSymbolFactV1 | DailyTradingSymbolFactV2:
    raw = reference.get("symbol_fact")
    if not isinstance(raw, Mapping):
        raise DataUnavailableError(
            "minute data requires a frozen daily symbol fact",
            context={"symbol": symbol, "trade_date": trade_date.isoformat()},
        )
    try:
        fact = (
            DailyTradingSymbolFactV1.model_validate(dict(raw))
            if reference.get("schema_version") == "daily_trading_context_reference_v1"
            else DailyTradingSymbolFactV2.model_validate(dict(raw))
        )
    except Exception as exc:
        raise DataUnavailableError(
            "frozen daily symbol fact is invalid",
            context={"symbol": symbol, "trade_date": trade_date.isoformat()},
        ) from exc
    if fact.symbol != symbol or fact.trade_date != trade_date:
        raise DataUnavailableError("frozen daily symbol fact identity mismatch")
    if (
        isinstance(fact, DailyTradingSymbolFactV2)
        and fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
    ):
        raise DataUnavailableError(
            "frozen daily authority is unavailable",
            context={"symbol": symbol, "reason_code": fact.authority_reason_code},
        )
    return fact
