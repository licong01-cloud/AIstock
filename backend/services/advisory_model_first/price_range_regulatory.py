from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

import numpy as np

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
)


@dataclass(frozen=True)
class RegulatoryPriceRange:
    status: str
    low: float | None
    high: float | None
    rule_id: str
    source: str = "DECISION_TIME_BOARD_ST_RULE"

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "low": self.low,
            "high": self.high,
            "rule_id": self.rule_id,
            "source": self.source,
        }


def resolve_regulatory_price_range(
    context: PriceRangeRealtimeContext,
    *,
    target_trade_date: date,
) -> RegulatoryPriceRange:
    reference = context.decision_raw_close * context.target_raw_price_multiplier
    if not np.isfinite(reference) or reference <= 0 or context.tick_size <= 0:
        raise AdvisoryModelFirstError(
            "regulatory reference price is invalid",
            reason_code="ADVISORY_PRICE_RANGE_REGULATORY_BOUNDARY_UNAVAILABLE",
            context={"symbol": context.symbol},
        )

    if _has_no_daily_limit(context, target_trade_date=target_trade_date):
        return RegulatoryPriceRange(
            status="NO_DAILY_LIMIT",
            low=None,
            high=None,
            rule_id=_no_limit_rule_id(context),
        )

    low_rate, high_rate, rule_id = _limit_rates(
        context,
        target_trade_date=target_trade_date,
    )
    low = _limit_price(
        reference=reference,
        rate=low_rate,
        tick_size=context.tick_size,
        direction=-1,
    )
    high = _limit_price(
        reference=reference,
        rate=high_rate,
        tick_size=context.tick_size,
        direction=1,
    )
    if not (0 < low <= reference <= high):
        raise AdvisoryModelFirstError(
            "regulatory price range is invalid after tick rounding",
            reason_code="ADVISORY_PRICE_RANGE_REGULATORY_BOUNDARY_UNAVAILABLE",
            context={"symbol": context.symbol, "rule_id": rule_id},
        )
    return RegulatoryPriceRange(
        status="LIMITED",
        low=low,
        high=high,
        rule_id=rule_id,
    )


def _has_no_daily_limit(
    context: PriceRangeRealtimeContext,
    *,
    target_trade_date: date,
) -> bool:
    if context.board_type == "STAR":
        return context.listed_trading_days <= 5
    if context.board_type == "CHINEXT":
        return (
            target_trade_date >= date(2020, 8, 24)
            and context.listed_trading_days <= 5
        )
    if context.board_type == "BSE":
        return context.listed_trading_days <= 1
    return (
        context.board_type == "MAIN"
        and context.list_date >= date(2023, 4, 10)
        and context.listed_trading_days <= 5
    )


def _no_limit_rule_id(context: PriceRangeRealtimeContext) -> str:
    if context.board_type == "STAR":
        return "STAR_IPO_FIRST_5_NO_LIMIT_V1"
    if context.board_type == "CHINEXT":
        return "CHINEXT_IPO_FIRST_5_NO_LIMIT_V1"
    if context.board_type == "BSE":
        return "BSE_IPO_FIRST_DAY_NO_LIMIT_V1"
    return "REGISTRATION_MAIN_IPO_FIRST_5_NO_LIMIT_V1"


def _limit_rates(
    context: PriceRangeRealtimeContext,
    *,
    target_trade_date: date,
) -> tuple[float, float, str]:
    if (
        context.board_type in {"MAIN", "CHINEXT"}
        and context.listed_trading_days <= 1
        and (
            context.board_type == "CHINEXT"
            or target_trade_date < date(2023, 4, 10)
        )
    ):
        return 0.36, 0.44, f"LEGACY_{context.board_type}_IPO_FIRST_DAY_44_36_V1"
    if context.board_type == "BSE":
        return 0.30, 0.30, "BSE_30PCT_V1"
    if context.board_type == "STAR":
        return 0.20, 0.20, "STAR_20PCT_V1"
    if context.board_type == "CHINEXT":
        if target_trade_date < date(2020, 8, 24):
            rate = 0.05 if context.target_is_st else 0.10
            return rate, rate, "CHINEXT_PRE_REFORM_ST_5PCT_V1" if context.target_is_st else "CHINEXT_PRE_REFORM_10PCT_V1"
        return 0.20, 0.20, "CHINEXT_20PCT_V1"
    if context.board_type == "MAIN":
        rate = 0.05 if context.target_is_st else 0.10
        return rate, rate, "MAIN_ST_5PCT_V1" if context.target_is_st else "MAIN_10PCT_V1"
    raise AdvisoryModelFirstError(
        "board type does not have a deterministic price-limit rule",
        reason_code="ADVISORY_PRICE_RANGE_REGULATORY_BOUNDARY_UNAVAILABLE",
        context={"symbol": context.symbol, "board_type": context.board_type},
    )


def _round_nearest_tick(value: float, tick_size: float) -> float:
    tick = Decimal(str(tick_size))
    units = (Decimal(str(round(value, 12))) / tick).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return float(units * tick)


def _limit_price(
    *,
    reference: float,
    rate: float,
    tick_size: float,
    direction: int,
) -> float:
    if direction not in {-1, 1}:
        raise ValueError("limit-price direction must be -1 or 1")
    result = _round_nearest_tick(reference * (1.0 + direction * rate), tick_size)
    if abs(result - reference) < tick_size:
        result = _round_nearest_tick(reference + direction * tick_size, tick_size)
    return max(tick_size, result)
