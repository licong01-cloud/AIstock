"""Read-only adapter over frozen policy episodes, not a new exit simulator.

Only actual next-open entries become observations. Price/risk paths are label
inputs, never decision-time features. Input rows and non-mature candidates stay
present; incompatible identities or contradictory economic values fail closed.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import (
    EconomicEntryInputIdentityV1,
    EconomicEntryLabelV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


KEY = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
NON_MATURE_STATUSES = {
    "NOT_ENTERED_MISSING_OPEN", "NOT_ENTERED_SUSPENDED", "NOT_ENTERED_LIMIT_UP",
    "CENSORED_RIGHT_BOUNDARY", "DATA_UNAVAILABLE",
}


def _fail(message: str, reason: str = "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH") -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason)


def _day(value: Any) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (ValueError, TypeError):
        _fail("label date is malformed")
    if pd.isna(result) or result.tzinfo is not None or result != result.normalize():
        _fail("label dates must be non-null, timezone-free trading dates")
    return result


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _positive(value: Any) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def _frame(frame: pd.DataFrame, keys: list[str], required: set[str]) -> pd.DataFrame:
    if not required.issubset(frame.columns):
        _fail(f"economic label source misses columns: {sorted(required - set(frame.columns))}")
    result = frame.copy()
    for name in keys:
        if name != "instrument":
            result[name] = result[name].map(_day)
    if not result["instrument"].map(lambda value: isinstance(value, str)).all():
        _fail("source instruments must be explicit strings")
    if result.duplicated(keys).any():
        _fail(f"economic label source has duplicate keys: {keys}")
    return result


def candidate_roster_sha256(candidates: pd.DataFrame) -> str:
    """Bind the exact D/T/symbol/rank roster; not a full-market universe receipt."""
    frame = _frame(candidates, KEY, set(KEY) | {"selection_effective_rank"})
    if frame.duplicated([KEY[0], "selection_effective_rank"]).any():
        _fail("candidate ranks are not unique within each decision")
    rows = []
    for row in frame.sort_values(KEY).to_dict("records"):
        rank = _positive(row["selection_effective_rank"])
        if rank is None or rank != int(rank):
            _fail("candidate rank must be a positive integer")
        rows.append({
            "decision": row[KEY[0]].date().isoformat(), "target": row[KEY[1]].date().isoformat(),
            "instrument": row["instrument"], "rank": int(rank),
        })
    return canonical_json_sha256(rows)


def build_economic_entry_labels(
    *,
    candidates: pd.DataFrame,
    episodes: pd.DataFrame,
    prices: pd.DataFrame,
    references: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    label_cutoff: pd.Timestamp,
    identity: EconomicEntryInputIdentityV1,
) -> tuple[EconomicEntryLabelV1, ...]:
    """Adapt caller-verified sources without changing candidates or old artifacts.

    prices: actual raw CNY open/close, policy_price_per_raw_cny, suspended,
    price_coordinate_sha256 and source_sha256, keyed by trade_date/instrument.
    references: D raw close, corporate-action projected T reference known by D,
    reference_visible_through and source_sha256, keyed by the candidate KEY.
    A missing price/reference row makes only that candidate unavailable. Missing
    schemas, duplicate rows and provenance contradictions invalidate the request.
    """
    # frozen=True does not recursively freeze embedded policy dictionaries.
    # Revalidate at the consumption boundary rather than trust a mutated object.
    identity = EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    identity_digest = identity.identity_sha256
    roster = _frame(candidates, KEY, set(KEY) | {"selection_effective_rank"})
    labels = _frame(episodes, KEY, set(KEY) | set(identity.episode_identity()) | {
        "selection_rank", "episode_label_id", "label_status", "label_information_end",
        "label_information_start",
        "entry_trade_date", "effective_exit_date", "entry_price", "exit_price", "net_return_bps",
    })
    if roster.empty or candidate_roster_sha256(roster) != identity.candidate_roster_sha256:
        _fail("frozen candidate roster hash mismatch or empty roster")
    if set(map(tuple, roster[KEY].to_numpy())) != set(map(tuple, labels[KEY].to_numpy())):
        _fail("episode roster differs from frozen candidates")
    if labels["episode_label_id"].isna().any() or labels["episode_label_id"].duplicated().any():
        _fail("frozen episode identities must be non-null and unique")
    for name, expected in identity.episode_identity().items():
        if not labels[name].eq(expected).all():
            _fail(f"episode identity mismatch: {name}")
    price_table = _frame(prices, ["trade_date", "instrument"], {
        "trade_date", "instrument", "raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny",
        "suspended", "price_coordinate_sha256", "source_sha256",
    })
    reference_table = _frame(references, KEY, set(KEY) | {
        "decision_raw_close_cny", "target_reference_raw_cny", "reference_visible_through", "source_sha256",
    })
    for column, expected in (
        ("price_coordinate_sha256", identity.price_coordinate_sha256),
        ("source_sha256", identity.price_source_sha256),
    ):
        if not price_table[column].eq(expected).all():
            _fail(f"price provenance mismatch: {column}")
    if not reference_table["source_sha256"].eq(identity.reference_source_sha256).all():
        _fail("reference provenance mismatch")
    if not price_table["suspended"].map(lambda value: isinstance(value, bool)).all():
        _fail("suspension state must be explicit boolean, not unknown or truthy string")
    calendar = pd.DatetimeIndex([_day(day) for day in trading_calendar])
    if calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing:
        _fail("trading calendar must be ordered and unique")
    cutoff = _day(label_cutoff)
    calendar_positions = {day: index for index, day in enumerate(calendar)}
    price_map = price_table.set_index(["trade_date", "instrument"]).to_dict("index")
    reference_map = reference_table.set_index(KEY).to_dict("index")
    episode_map = labels.set_index(KEY).to_dict("index")
    output = []
    for candidate in roster.to_dict("records"):
        decision, target, symbol = (candidate[name] for name in KEY)
        if decision not in calendar_positions or target not in calendar_positions:
            _fail("candidate dates missing from the bound calendar")
        if calendar_positions[target] != calendar_positions[decision] + 1:
            _fail("candidate target is not next trading day")
        episode = episode_map[(decision, target, symbol)]
        if _day(episode["label_information_start"]) != decision:
            _fail("episode information_start differs from the frozen decision")
        if candidate["selection_effective_rank"] != episode["selection_rank"]:
            _fail("episode selection rank differs from frozen candidate")
        information_end = _day(episode["label_information_end"])
        if information_end < target:
            _fail("episode information_end precedes entry observation")
        status = str(episode["label_status"])
        base = {
            "input_identity_sha256": identity_digest,
            "episode_label_id": episode["episode_label_id"], "decision_date": decision.date(),
            "target_date": target.date(), "instrument": symbol,
            "selection_rank": candidate["selection_effective_rank"],
            "original_label_status": status, "label_information_end": information_end.date(),
        }
        if status != "MATURED":
            if status not in NON_MATURE_STATUSES:
                _fail(f"unknown frozen label status: {status}")
            adapted = (
                "NOT_ENTERED" if status in {"NOT_ENTERED_SUSPENDED", "NOT_ENTERED_LIMIT_UP"}
                else "DATA_UNAVAILABLE" if status == "NOT_ENTERED_MISSING_OPEN" else status
            )
            observed_entry = price_map.get((target, symbol))
            if status == "NOT_ENTERED_SUSPENDED" and observed_entry is not None and not observed_entry["suspended"]:
                _fail("frozen suspension label contradicts the observed suspension state")
            output.append(EconomicEntryLabelV1(**base, status=adapted, reason_code=status))
            continue
        if information_end > cutoff:
            output.append(EconomicEntryLabelV1(
                **base, status="CENSORED_RIGHT_BOUNDARY", reason_code="LABEL_AFTER_CUTOFF",
            ))
            continue
        ref = reference_map.get((decision, target, symbol))
        if ref is None:
            output.append(EconomicEntryLabelV1(**base, status="DATA_UNAVAILABLE", reason_code="REFERENCE_MISSING"))
            continue
        if _day(ref["reference_visible_through"]) != decision:
            _fail("D-close reference must be exactly D-visible, never stale or future", "ADVISORY_ECONOMIC_REFERENCE_PIT")
        output.append(_mature_label(
            base=base, episode=episode, reference=ref, prices=price_map,
            calendar=calendar, positions=calendar_positions, identity=identity,
        ))
    return tuple(output)


def _mature_label(
    *, base: dict[str, Any], episode: dict[str, Any], reference: dict[str, Any],
    prices: dict[tuple[pd.Timestamp, str], dict[str, Any]], calendar: pd.DatetimeIndex,
    positions: dict[pd.Timestamp, int], identity: EconomicEntryInputIdentityV1,
) -> EconomicEntryLabelV1:
    def unavailable(reason: str) -> EconomicEntryLabelV1:
        return EconomicEntryLabelV1(**base, status="DATA_UNAVAILABLE", reason_code=reason)

    target, decision = _day(base["target_date"]), _day(base["decision_date"])
    symbol = base["instrument"]
    exit_date = _day(episode["effective_exit_date"])
    if (
        _day(episode["entry_trade_date"]) != target or exit_date <= target
        or exit_date.date() != base["label_information_end"] or exit_date not in positions
    ):
        _fail("mature label entry/exit/information clock mismatch")
    entry = prices.get((target, symbol))
    exit_row = prices.get((exit_date, symbol))
    decision_row = prices.get((decision, symbol))
    if entry is None or exit_row is None or decision_row is None:
        return unavailable("PRICE_ENDPOINT_MISSING")
    if entry["suspended"] or exit_row["suspended"]:
        _fail("mature episode claims an executable entry/exit on a suspended day")
    raw_open = _positive(entry["raw_open_cny"])
    dclose = _positive(reference["decision_raw_close_cny"])
    tref = _positive(reference["target_reference_raw_cny"])
    if raw_open is None or dclose is None or tref is None:
        return unavailable("REFERENCE_OR_OPEN_INVALID")
    observed_dclose = _positive(decision_row["raw_close_cny"])
    if observed_dclose is None:
        return unavailable("DECISION_CLOSE_MISSING")
    if not math.isclose(dclose, observed_dclose, rel_tol=1e-9, abs_tol=1e-9):
        _fail("reference raw close differs from actual D close")
    endpoints = []
    for row, field in ((entry, "entry_price"), (exit_row, "exit_price")):
        factor = _positive(row["policy_price_per_raw_cny"])
        raw = _positive(row["raw_open_cny"])
        recorded = _positive(episode[field])
        if factor is None or raw is None or recorded is None:
            return unavailable("PRICE_COORDINATE_ENDPOINT_INVALID")
        projected = raw * factor
        if not math.isclose(projected, recorded, rel_tol=1e-7, abs_tol=1e-8):
            _fail("raw-to-policy price parity failed", "ADVISORY_ECONOMIC_PRICE_PARITY")
        endpoints.append(recorded)
    cost = identity.cost_policy
    paid = endpoints[0] * (1 + cost.buy_cost_bps / 10000)
    sell_multiplier = 1 - cost.sell_cost_bps / 10000
    net_bps = (endpoints[1] * sell_multiplier / paid - 1) * 10000
    recorded_net = _number(episode["net_return_bps"])
    if recorded_net is None or not math.isclose(net_bps, recorded_net, rel_tol=1e-8, abs_tol=1e-6):
        _fail("frozen net return differs from cost-once policy return", "ADVISORY_ECONOMIC_COST_PARITY")
    peak, drawdown, carried = 1.0, 0.0, 0
    for day in calendar[positions[target]:positions[exit_date] + 1]:
        row = prices.get((day, symbol))
        if row is None:
            return unavailable("DAILY_MARK_MISSING")
        if row["suspended"]:
            carried += 1  # The previous adjusted mark is unchanged, not a made-up raw quote.
            continue
        factor = _positive(row["policy_price_per_raw_cny"])
        if factor is None:
            return unavailable("DAILY_MARK_COORDINATE_MISSING")
        fields = ("raw_open_cny",) if day == exit_date else ("raw_open_cny", "raw_close_cny")
        for field in fields:
            mark = _positive(row[field])
            if mark is None:
                return unavailable("DAILY_MARK_INVALID")
            wealth = mark * factor * sell_multiplier / paid
            peak = max(peak, wealth)
            drawdown = max(drawdown, (1 - wealth / peak) * 10000)
    return EconomicEntryLabelV1(
        **base, status="AVAILABLE", decision_raw_close_cny=dclose, target_reference_raw_cny=tref,
        actual_open_raw_cny=raw_open, actual_gap_bps=(raw_open / tref - 1) * 10000,
        enter_net_value_bps=recorded_net, skip_net_value_bps=0.0, entry_advantage_bps=recorded_net,
        daily_mark_max_drawdown_bps=drawdown, suspension_carried_mark_count=carried,
    )
