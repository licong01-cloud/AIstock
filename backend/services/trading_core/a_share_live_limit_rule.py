"""Pure A-share daily limits derived from an audited live reference close."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from backend.services.dataset_release.a_share_limit_rule import (
    PRICE_LIMIT_RULE_VERSION,
    AShareBoard,
    AShareLimitRuleError,
    resolve_limit_rate,
)


LIVE_REFERENCE_LIMIT_RULE_VERSION = PRICE_LIMIT_RULE_VERSION


class LiveReferenceLimitRuleError(ValueError):
    """Raised when a live reference cannot safely produce daily limits."""

    code = "DAILY_LIMIT_TDX_REFERENCE_RULE_INVALID"


@dataclass(frozen=True, slots=True)
class LiveReferenceLimitResult:
    ts_code: str
    trade_date: date
    board: AShareBoard
    is_st: bool
    has_daily_limit: bool
    reference_pre_close: Decimal
    price_tick: Decimal
    up_limit: Decimal | None
    down_limit: Decimal | None
    limit_rate: Decimal | None
    reference_evidence_hash: str
    rule_version: str
    no_daily_limit_reason: str | None
    derivation_hash: str | None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date.isoformat(),
            "board": self.board.value,
            "is_st": self.is_st,
            "has_daily_limit": self.has_daily_limit,
            "reference_pre_close": format(self.reference_pre_close, "f"),
            "price_tick": format(self.price_tick, "f"),
            "up_limit": format(self.up_limit, "f") if self.up_limit is not None else None,
            "down_limit": format(self.down_limit, "f") if self.down_limit is not None else None,
            "limit_rate": format(self.limit_rate, "f") if self.limit_rate is not None else None,
            "reference_evidence_hash": self.reference_evidence_hash,
            "rule_version": self.rule_version,
            "no_daily_limit_reason": self.no_daily_limit_reason,
        }


def derive_live_reference_limit_prices(
    *,
    ts_code: str,
    trade_date: date,
    reference_pre_close: Any,
    reference_evidence_hash: str,
    price_tick: Any,
    is_st: bool,
    no_daily_limit: bool = False,
    no_daily_limit_reason: str | None = None,
) -> LiveReferenceLimitResult:
    """Derive raw CNY/share limits without historical prices or adjustment factors."""

    if type(trade_date) is not date:
        raise LiveReferenceLimitRuleError("trade_date must be an exact date")
    if type(is_st) is not bool or type(no_daily_limit) is not bool:
        raise LiveReferenceLimitRuleError("PIT ST/no-limit state requires exact booleans")
    evidence_hash = _sha256_text(reference_evidence_hash, field="reference_evidence_hash")
    pre_close = _positive_decimal(reference_pre_close, field="reference_pre_close")
    tick = _positive_decimal(price_tick, field="price_tick")
    if pre_close % tick != 0:
        raise LiveReferenceLimitRuleError("reference_pre_close must align to price_tick")

    reason = str(no_daily_limit_reason or "").strip() or None
    if no_daily_limit and reason is None:
        raise LiveReferenceLimitRuleError("no_daily_limit requires a versioned reason")
    if not no_daily_limit and reason is not None:
        raise LiveReferenceLimitRuleError("no_daily_limit_reason is forbidden for a limited stock-day")

    try:
        decision = resolve_limit_rate(
            ts_code=ts_code,
            trade_date=trade_date,
            is_st=is_st,
            no_daily_limit=no_daily_limit,
        )
    except AShareLimitRuleError as exc:
        raise LiveReferenceLimitRuleError(str(exc)) from exc

    if not decision.has_daily_limit:
        return LiveReferenceLimitResult(
            ts_code=decision.ts_code,
            trade_date=decision.trade_date,
            board=decision.board,
            is_st=decision.is_st,
            has_daily_limit=False,
            reference_pre_close=pre_close,
            price_tick=tick,
            up_limit=None,
            down_limit=None,
            limit_rate=None,
            reference_evidence_hash=evidence_hash,
            rule_version=LIVE_REFERENCE_LIMIT_RULE_VERSION,
            no_daily_limit_reason=reason,
            derivation_hash=None,
        )

    if decision.limit_rate is None:
        raise LiveReferenceLimitRuleError("limited stock-day is missing a limit rate")
    up_limit = _round_to_tick(pre_close * (Decimal("1") + decision.limit_rate), tick)
    down_limit = _round_to_tick(pre_close * (Decimal("1") - decision.limit_rate), tick)
    if not down_limit < pre_close < up_limit:
        raise LiveReferenceLimitRuleError("derived daily limit bounds are invalid")
    hash_payload = {
        "schema_version": "tdx_reference_daily_limit_derivation_v1",
        "ts_code": decision.ts_code,
        "trade_date": decision.trade_date.isoformat(),
        "board": decision.board.value,
        "is_st": decision.is_st,
        "reference_pre_close": format(pre_close, "f"),
        "price_tick": format(tick, "f"),
        "up_limit": format(up_limit, "f"),
        "down_limit": format(down_limit, "f"),
        "limit_rate": format(decision.limit_rate, "f"),
        "reference_evidence_hash": evidence_hash,
        "rule_version": LIVE_REFERENCE_LIMIT_RULE_VERSION,
    }
    derivation_hash = hashlib.sha256(
        json.dumps(hash_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return LiveReferenceLimitResult(
        ts_code=decision.ts_code,
        trade_date=decision.trade_date,
        board=decision.board,
        is_st=decision.is_st,
        has_daily_limit=True,
        reference_pre_close=pre_close,
        price_tick=tick,
        up_limit=up_limit,
        down_limit=down_limit,
        limit_rate=decision.limit_rate,
        reference_evidence_hash=evidence_hash,
        rule_version=LIVE_REFERENCE_LIMIT_RULE_VERSION,
        no_daily_limit_reason=None,
        derivation_hash=derivation_hash,
    )


def _positive_decimal(value: Any, *, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveReferenceLimitRuleError(f"{field} is invalid") from exc
    if not number.is_finite() or number <= 0:
        raise LiveReferenceLimitRuleError(f"{field} must be positive and finite")
    return number


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if not value.is_finite() or value <= 0:
        raise LiveReferenceLimitRuleError("derived price must be positive and finite")
    return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


def _sha256_text(value: str, *, field: str) -> str:
    if type(value) is not str:
        raise LiveReferenceLimitRuleError(f"{field} must be a lowercase SHA-256 hex digest")
    text = value.strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise LiveReferenceLimitRuleError(f"{field} must be a lowercase SHA-256 hex digest")
    return text


__all__ = [
    "LIVE_REFERENCE_LIMIT_RULE_VERSION",
    "LiveReferenceLimitResult",
    "LiveReferenceLimitRuleError",
    "derive_live_reference_limit_prices",
]
