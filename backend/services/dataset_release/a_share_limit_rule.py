"""Versioned A-share daily price-limit rules for candidate dataset overlays.

This module is deliberately pure: it has no database, provider, filesystem or
runtime trading dependency.  Historical candidate builders must supply PIT
state and an adjustment-aware reference-price input explicitly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from typing import Any


PRICE_LIMIT_RULE_VERSION = "cn_a_share_price_limit_v2_20260706"
PRICE_TICK = Decimal("0.01")
MAIN_BOARD_ST_TEN_PERCENT_FROM = date(2026, 7, 6)
CHINEXT_TWENTY_PERCENT_FROM = date(2020, 8, 24)

_TEN_PERCENT = Decimal("0.10")
_FIVE_PERCENT = Decimal("0.05")
_TWENTY_PERCENT = Decimal("0.20")
_CODE = re.compile(r"^(?P<digits>[0-9]{6})\.(?P<exchange>SH|SZ)$")


class AShareLimitRuleError(ValueError):
    """Raised when a stock-day cannot be safely classified or calculated."""

    code = "BLOCKED_A_SHARE_LIMIT_RULE_INVALID"


class AShareBoard(str, Enum):
    SH_MAIN = "SH_MAIN"
    SZ_MAIN = "SZ_MAIN"
    CHINEXT = "CHINEXT"
    STAR = "STAR"


@dataclass(frozen=True, slots=True)
class LimitRateDecision:
    ts_code: str
    trade_date: date
    board: AShareBoard
    is_st: bool
    has_daily_limit: bool
    limit_rate: Decimal | None
    rule_version: str = PRICE_LIMIT_RULE_VERSION


@dataclass(frozen=True, slots=True)
class DerivedLimitPrices:
    ts_code: str
    trade_date: date
    board: AShareBoard
    is_st: bool
    pre_close: Decimal
    up_limit: Decimal
    down_limit: Decimal
    limit_rate: Decimal
    rule_version: str = PRICE_LIMIT_RULE_VERSION

    def as_source_row(self) -> dict[str, str]:
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date.isoformat(),
            "pre_close": format(self.pre_close, "f"),
            "up_limit": format(self.up_limit, "f"),
            "down_limit": format(self.down_limit, "f"),
        }


def classify_a_share_board(ts_code: str) -> AShareBoard:
    code = str(ts_code or "").strip().upper()
    match = _CODE.fullmatch(code)
    if match is None:
        raise AShareLimitRuleError(f"unsupported A-share board: {code}")
    digits = match.group("digits")
    exchange = match.group("exchange")
    if exchange == "SH":
        if digits.startswith(("688", "689")):
            return AShareBoard.STAR
        if digits.startswith(("600", "601", "603", "605")):
            return AShareBoard.SH_MAIN
    elif digits.startswith(("300", "301", "302")):
        return AShareBoard.CHINEXT
    elif digits.startswith(("000", "001", "002", "003")):
        return AShareBoard.SZ_MAIN
    raise AShareLimitRuleError(f"unsupported A-share board: {code}")


def resolve_limit_rate(
    *,
    ts_code: str,
    trade_date: date,
    is_st: bool,
    no_daily_limit: bool = False,
) -> LimitRateDecision:
    if not isinstance(trade_date, date):
        raise AShareLimitRuleError("trade_date is invalid")
    if type(is_st) is not bool or type(no_daily_limit) is not bool:
        raise AShareLimitRuleError("PIT ST/no-limit state is invalid")
    code = str(ts_code or "").strip().upper()
    board = classify_a_share_board(code)
    if no_daily_limit:
        return LimitRateDecision(
            ts_code=code,
            trade_date=trade_date,
            board=board,
            is_st=is_st,
            has_daily_limit=False,
            limit_rate=None,
        )
    if board in {AShareBoard.SH_MAIN, AShareBoard.SZ_MAIN}:
        rate = _FIVE_PERCENT if is_st and trade_date < MAIN_BOARD_ST_TEN_PERCENT_FROM else _TEN_PERCENT
    elif board is AShareBoard.CHINEXT:
        if trade_date < CHINEXT_TWENTY_PERCENT_FROM:
            rate = _FIVE_PERCENT if is_st else _TEN_PERCENT
        else:
            rate = _TWENTY_PERCENT
    else:
        rate = _TWENTY_PERCENT
    return LimitRateDecision(
        ts_code=code,
        trade_date=trade_date,
        board=board,
        is_st=is_st,
        has_daily_limit=True,
        limit_rate=rate,
    )


def derive_limit_prices(
    *,
    ts_code: str,
    trade_date: date,
    previous_close: Any,
    previous_adj_factor: Any,
    current_adj_factor: Any,
    is_st: bool,
    no_daily_limit: bool = False,
) -> DerivedLimitPrices:
    decision = resolve_limit_rate(
        ts_code=ts_code,
        trade_date=trade_date,
        is_st=is_st,
        no_daily_limit=no_daily_limit,
    )
    if not decision.has_daily_limit or decision.limit_rate is None:
        raise AShareLimitRuleError("stock-day has no daily price limit")
    close = _positive_decimal(previous_close, field="previous_close")
    previous_factor = _positive_decimal(previous_adj_factor, field="previous_adj_factor")
    current_factor = _positive_decimal(current_adj_factor, field="current_adj_factor")
    pre_close = _to_tick(close * previous_factor / current_factor)
    up_limit = _to_tick(pre_close * (Decimal("1") + decision.limit_rate))
    down_limit = _to_tick(pre_close * (Decimal("1") - decision.limit_rate))
    if up_limit - pre_close < PRICE_TICK:
        up_limit = pre_close + PRICE_TICK
    if pre_close - down_limit < PRICE_TICK:
        down_limit = max(PRICE_TICK, pre_close - PRICE_TICK)
    if not PRICE_TICK <= down_limit <= pre_close <= up_limit:
        raise AShareLimitRuleError("derived price bounds are invalid")
    return DerivedLimitPrices(
        ts_code=decision.ts_code,
        trade_date=decision.trade_date,
        board=decision.board,
        is_st=decision.is_st,
        pre_close=pre_close,
        up_limit=up_limit,
        down_limit=down_limit,
        limit_rate=decision.limit_rate,
    )


def _positive_decimal(value: Any, *, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise AShareLimitRuleError(f"{field} is invalid") from exc
    if not number.is_finite() or number <= 0:
        raise AShareLimitRuleError(f"{field} must be positive and finite")
    return number


def _to_tick(value: Decimal) -> Decimal:
    if not value.is_finite() or value <= 0:
        raise AShareLimitRuleError("derived price is non-positive or non-finite")
    return value.quantize(PRICE_TICK, rounding=ROUND_HALF_UP)


__all__ = [
    "PRICE_LIMIT_RULE_VERSION",
    "PRICE_TICK",
    "AShareBoard",
    "AShareLimitRuleError",
    "DerivedLimitPrices",
    "LimitRateDecision",
    "classify_a_share_board",
    "derive_limit_prices",
    "resolve_limit_rate",
]
