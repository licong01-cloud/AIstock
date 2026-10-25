"""Public, rule-independent execution price preferences from explicit raw references."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal

from pydantic import model_validator

from .contracts import CHINA, Contract, PlanDraft, Positive, PriceConstraint, PriceInterval, Timestamp, payload, utc
from .replay import FileRef

Reason = Literal["REFERENCE_AVAILABLE", "REFERENCE_FILE_MISSING", "REFERENCE_OUTSIDE_FROZEN_SPAN",
                 "REFERENCE_NONFINITE", "REFERENCE_NONPOSITIVE"]


class PricePlanPolicy(Contract):
    schema_version: Literal["position_timing_shared_price_policy_v1"] = "position_timing_shared_price_policy_v1"
    price_policy_id: Literal["RAW_PREVCLOSE_PREFERRED_50BPS_V1"] = "RAW_PREVCLOSE_PREFERRED_50BPS_V1"
    offset_bps: Literal[50] = 50
    tick_cny: Literal["0.01"] = "0.01"
    price_basis: Literal["raw_cny"] = "raw_cny"


class PriceReference(Contract):
    symbol: str
    trade_date: date
    reference_price_raw: Positive | None
    source_file: FileRef
    source_available_at: Timestamp
    reason_code: Reason

    @model_validator(mode="after")
    def known_value(self):
        if (self.reference_price_raw is not None) != (self.reason_code == "REFERENCE_AVAILABLE"):
            raise ValueError("reference value and availability must agree")
        return self


class PricePlanResult(Contract):
    availability: Literal["AVAILABLE", "UNKNOWN"]
    reason_code: Reason
    event: dict


def build_price_plan(reference: PriceReference, side: Literal["BUY", "SELL"], policy: PricePlanPolicy,
                     available_at: datetime, *, revision: int = 1,
                     lifecycle: Literal["ACTIVE", "CANCELLED", "COMPLETED"] = "ACTIVE",
                     previous: dict | None = None) -> PricePlanResult:
    """Build one explicit preference; callers retain accounts and revision history.

    NONE for UNKNOWN carries no interval. It is not permission to run C1 without
    its preference, nor permission to carry a previous day's price forward.
    """
    available = utc(available_at)
    day = reference.trade_date
    start, end = (datetime.combine(day, time(9, 30), CHINA), datetime.combine(day, time(14, 57), CHINA))
    if side not in {"BUY", "SELL"} or lifecycle not in {"ACTIVE", "CANCELLED", "COMPLETED"}:
        raise ValueError("unsupported side or lifecycle")
    if (isinstance(revision, bool) or not isinstance(revision, int) or revision < 1
            or reference.source_available_at > available or available >= utc(end)
            or available.astimezone(CHINA).date() != day):
        raise ValueError("invalid revision or price availability clock")
    if previous is None:
        if revision != 1 or lifecycle != "ACTIVE" or available > utc(start):
            raise ValueError("first price event must be active before execution starts")
    elif ((previous["symbol"], previous["side"], previous["trade_date"]) != (reference.symbol, side, str(day))
          or previous["lifecycle"] != "ACTIVE" or revision != previous["revision"] + 1
          or available <= utc(previous["available_at"])):
        raise ValueError("price revision must follow the same active event series")
    known = reference.reference_price_raw is not None
    intervals = ()
    if known:
        value, offset, tick = reference.reference_price_raw, Decimal(policy.offset_bps) / 10000, Decimal(policy.tick_cny)
        lower, upper = (value * (1 - offset), value) if side == "BUY" else (value, value * (1 + offset))
        intervals = (PriceInterval(lower=lower.quantize(tick, rounding=ROUND_FLOOR),
                                   upper=upper.quantize(tick, rounding=ROUND_CEILING)),)
    price = PriceConstraint(mode="PREFERRED" if known else "NONE", intervals=intervals,
                            source_ref="candidate.daily.prev_close:raw_cny",
                            source_sha256=reference.source_file.sha256, valid_from=start, valid_until=end)
    # Validate shared symbol/date contracts without making C1 a global authority.
    PlanDraft(symbol=reference.symbol, side=side, target_trade_date=day, source_ref=policy.price_policy_id,
              source_revision=str(revision), valid_from=start, valid_until=end, price_constraint=price)
    event = payload({"symbol": reference.symbol, "side": side, "trade_date": day, "revision": revision,
                     "lifecycle": lifecycle, "available_at": available, "valid_from": start, "valid_until": end,
                     "source_file": reference.source_file, "source_available_at": reference.source_available_at,
                     "price_constraint": price})
    return PricePlanResult(availability="AVAILABLE" if known else "UNKNOWN",
                           reason_code=reference.reason_code, event=event)
