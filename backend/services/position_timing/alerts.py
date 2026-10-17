"""Read-only quote evaluation for human position-timing reminders.

The module deliberately owns no scheduler, background poller, notification
transport, or order path.  The browser calls the read endpoint while visible;
the service only grants one append-only authorization after revalidation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from backend.services.simulation_data.contracts import (
    TDX_REALTIME_BATCH_QUOTE_LIMIT,
    TDX_REALTIME_QUOTE_MAX_AGE,
    TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW,
)

from .contracts import PositionTimingCardV1, TriggerOperator, TriggerSide, TriggerV1, canonical_sha256


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUOTE_SOURCE = "TDX_REALTIME.batch_quote"


@dataclass(frozen=True)
class ParsedAlertQuote:
    symbol: str
    price_raw: Decimal
    open_raw: Decimal
    observed_at: datetime
    evaluated_at: datetime
    source: str = QUOTE_SOURCE

    @property
    def age_seconds(self) -> Decimal:
        return max(
            Decimal("0"),
            Decimal(str((self.evaluated_at - self.observed_at).total_seconds())),
        )


def fetch_quotes_in_contract_chunks(
    *,
    symbols: list[str],
    quote_loader: Callable[[list[str]], dict[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Call the injected batch authority in explicit <=50-symbol chunks."""

    unique = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    result: dict[str, dict[str, Any]] = {}
    for offset in range(0, len(unique), TDX_REALTIME_BATCH_QUOTE_LIMIT):
        chunk = unique[offset : offset + TDX_REALTIME_BATCH_QUOTE_LIMIT]
        payload = quote_loader(chunk)
        if not isinstance(payload, dict):
            raise TypeError("TDX realtime quote loader must return a symbol-keyed dictionary")
        for symbol, quote in payload.items():
            canonical = str(symbol).strip().upper()
            if canonical not in chunk or not isinstance(quote, dict):
                continue
            if canonical in result and canonical_sha256(result[canonical]) != canonical_sha256(quote):
                raise ValueError(f"TDX realtime quote identity conflict: {canonical}")
            result[canonical] = dict(quote)
    return result


def parse_alert_quote(
    *,
    symbol: str,
    raw_quote: Mapping[str, Any] | None,
    evaluated_at: datetime,
) -> tuple[ParsedAlertQuote | None, str, dict[str, Any]]:
    """Parse only the bounded raw fields needed by L1a and return typed failures."""

    if not isinstance(raw_quote, Mapping):
        return None, "QUOTE_UNAVAILABLE", {}
    raw = dict(raw_quote)
    try:
        price = _quote_price(raw, normalized_key="quote_price_raw", tdx_keys=("Close", "close"))
        open_price = _quote_price(raw, normalized_key="quote_open_raw", tdx_keys=("Open", "open"))
    except (InvalidOperation, TypeError, ValueError):
        return None, "QUOTE_UNAVAILABLE", {}
    if price is None or open_price is None or price <= 0 or open_price <= 0:
        return None, "QUOTE_UNAVAILABLE", {}
    try:
        observed_at = _quote_timestamp(raw, trade_date=evaluated_at.astimezone(CHINA_TZ).date())
    except (TypeError, ValueError):
        return None, "QUOTE_UNAVAILABLE", {}
    evaluated = evaluated_at.astimezone(CHINA_TZ)
    observed = observed_at.astimezone(CHINA_TZ)
    age = evaluated - observed
    details = {
        "quote_observed_at": observed,
        "alert_evaluated_at": evaluated,
        "quote_age_seconds": Decimal(str(max(0.0, age.total_seconds()))),
        "max_quote_age_seconds": Decimal(str(TDX_REALTIME_QUOTE_MAX_AGE.total_seconds())),
        "max_future_skew_seconds": Decimal(str(TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW.total_seconds())),
    }
    if age > TDX_REALTIME_QUOTE_MAX_AGE:
        return None, "QUOTE_STALE", details
    if observed - evaluated > TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW:
        return None, "QUOTE_FUTURE_SKEW", details
    return (
        ParsedAlertQuote(
            symbol=symbol,
            price_raw=price,
            open_raw=open_price,
            observed_at=observed,
            evaluated_at=evaluated,
        ),
        "FRESH",
        details,
    )


def evaluate_frozen_trigger(
    *, card: PositionTimingCardV1, quote: ParsedAlertQuote
) -> tuple[TriggerV1 | None, str]:
    """Select one already-frozen branch; never create a new direction or threshold."""

    if not card.triggers:
        return None, "NO_ACTIVE_TRIGGER"
    sides = {trigger.side for trigger in card.triggers if trigger.side is not TriggerSide.NONE}
    if len(sides) != 1:
        return None, "TRIGGER_DIRECTION_CONFLICT"
    side = next(iter(sides))
    if side is TriggerSide.BUY and _directionally_locked(
        quote.open_raw, quote.price_raw, card.limit_up_raw
    ):
        return None, "BUY_LIMIT_UP_UNAVAILABLE"
    if side is TriggerSide.SELL and _directionally_locked(
        quote.open_raw, quote.price_raw, card.limit_down_raw
    ):
        return None, "SELL_LIMIT_DOWN_UNAVAILABLE"

    always = [
        trigger
        for trigger in card.triggers
        if trigger.operator is TriggerOperator.ALWAYS and trigger.planned_delta_qty != 0
    ]
    if always:
        return always[0], "ELIGIBLE"
    eligible: list[TriggerV1] = []
    for trigger in card.triggers:
        threshold = trigger.trigger_price_raw
        if threshold is None or trigger.planned_delta_qty == 0:
            continue
        if trigger.operator is TriggerOperator.LTE and quote.price_raw <= threshold:
            eligible.append(trigger)
        elif trigger.operator is TriggerOperator.GTE and quote.price_raw >= threshold:
            eligible.append(trigger)
    if not eligible:
        return None, "PRICE_TRIGGER_NOT_MET"
    if side is TriggerSide.BUY:
        # Green is the lowest threshold.  When current price satisfies both
        # overlapping LTE bands, the frozen full-size green branch owns it.
        selected = min(eligible, key=lambda item: item.trigger_price_raw or Decimal("Infinity"))
    else:
        selected = max(eligible, key=lambda item: item.trigger_price_raw or Decimal("0"))
    return selected, "ELIGIBLE"


def eligibility_payload(
    *,
    card: PositionTimingCardV1,
    trigger: TriggerV1,
    quote: ParsedAlertQuote,
    position_snapshot_sha256: str,
    intent_snapshot_sha256: str,
) -> dict[str, Any]:
    return {
        "card_id": card.card_id,
        "card_artifact_sha256": canonical_sha256(card),
        "trigger_id": trigger.trigger_id,
        "quote_price_raw": quote.price_raw,
        "quote_open_raw": quote.open_raw,
        "quote_observed_at": quote.observed_at,
        "alert_evaluated_at": quote.evaluated_at,
        "quote_source": quote.source,
        "position_snapshot_sha256": position_snapshot_sha256,
        "intent_snapshot_sha256": intent_snapshot_sha256,
    }


def eligibility_identity(payload: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(payload))


def _directionally_locked(open_price: Decimal, current_price: Decimal, limit_price: Decimal | None) -> bool:
    if limit_price is None:
        return False
    tick = Decimal("0.01")
    return abs(open_price - limit_price) < tick / 2 and abs(current_price - limit_price) < tick / 2


def _quote_price(
    raw: dict[str, Any], *, normalized_key: str, tdx_keys: tuple[str, ...]
) -> Decimal | None:
    if raw.get(normalized_key) not in (None, ""):
        return Decimal(str(raw[normalized_key]))
    kline = raw.get("K") if isinstance(raw.get("K"), dict) else {}
    value: Any = None
    for key in tdx_keys:
        if kline.get(key) not in (None, ""):
            value = kline[key]
            break
    if value is None:
        fallback = {
            "Close": ("lastPrice", "last_price", "price", "close"),
            "Open": ("open", "open_price", "openPrice"),
        }
        for key in fallback.get(tdx_keys[0], ()):
            if raw.get(key) not in (None, ""):
                value = raw[key]
                break
    if value is None:
        return None
    parsed = Decimal(str(value))
    basis = str(raw.get("price_basis") or "raw_li").strip().lower()
    return parsed if basis in {"yuan", "raw_cny"} else parsed / Decimal("1000")


def _quote_timestamp(raw: dict[str, Any], *, trade_date: date) -> datetime:
    value: Any = raw.get("quote_observed_at")
    if value in (None, ""):
        keys = (
            "ServerTime",
            "serverTime",
            "server_time",
            "timestamp",
            "quote_timestamp",
            "quoteTime",
            "quote_time",
            "time",
            "Time",
            "datetime",
            "date_time",
            "update_time",
        )
        for key in keys:
            if raw.get(key) not in (None, ""):
                value = raw[key]
                break
        if value in (None, "") and isinstance(raw.get("K"), dict):
            for key in keys:
                if raw["K"].get(key) not in (None, ""):
                    value = raw["K"][key]
                    break
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("quote timestamp is missing")
        if text.isdigit() and len(text) == 14:
            parsed = datetime.strptime(text, "%Y%m%d%H%M%S")
        elif text.isdigit() and len(text) in {7, 8}:
            padded = text.zfill(8)
            parsed = datetime.combine(
                trade_date,
                datetime.strptime(padded[:6], "%H%M%S").time().replace(
                    microsecond=int(padded[6:8]) * 10_000
                ),
            )
        elif text.isdigit() and len(text) <= 6:
            padded = text.zfill(6)
            parsed = datetime.combine(
                trade_date,
                datetime.strptime(padded, "%H%M%S").time(),
            )
        elif text.isdigit() and len(text) >= 10:
            numeric = int(text)
            parsed = datetime.fromtimestamp(numeric / 1000 if numeric >= 10**12 else numeric, tz=CHINA_TZ)
        else:
            normalized = text.replace("/", "-")
            if normalized.endswith("Z"):
                normalized = f"{normalized[:-1]}+00:00"
            parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=CHINA_TZ)
    return parsed.astimezone(CHINA_TZ)


__all__ = [
    "ParsedAlertQuote",
    "QUOTE_SOURCE",
    "eligibility_identity",
    "eligibility_payload",
    "evaluate_frozen_trigger",
    "fetch_quotes_in_contract_chunks",
    "parse_alert_quote",
]
