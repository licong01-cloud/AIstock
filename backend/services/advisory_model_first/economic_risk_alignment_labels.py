"""Same-policy entry-anchored losses, preserving every original candidate."""

from __future__ import annotations

import math
from collections.abc import Sequence

import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import (
    EconomicEntryInputIdentityV1, EconomicEntryLabelV1, POLICY_PRICE_RELATIVE_TOLERANCE,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _fail, _frame, _number, _positive
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2


def build_entry_loss_labels_v2(
    *, original_labels: tuple[EconomicEntryLabelV1, ...], episodes: pd.DataFrame,
    prices: pd.DataFrame, trading_calendar: Sequence, identity: EconomicEntryInputIdentityV1,
) -> tuple[EntryLossLabelV2, ...]:
    identity = EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    originals = tuple(EconomicEntryLabelV1.model_validate(value.model_dump()) for value in original_labels)
    if not originals or any(value.input_identity_sha256 != identity.identity_sha256 for value in originals):
        _fail("v2 original labels do not match the frozen input identity")
    labels = _frame(episodes, KEY, set(KEY) | set(identity.episode_identity()) | {
        "episode_label_id", "selection_rank", "entry_trade_date", "effective_exit_date", "label_information_end",
        "entry_price", "exit_price", "net_return_bps",
    })
    if labels.episode_label_id.isna().any() or labels.episode_label_id.duplicated().any():
        _fail("v2 original episode IDs must be unique")
    if len(originals) != len(labels) or {value.episode_label_id for value in originals} != set(labels.episode_label_id):
        _fail("v2 must retain the exact original episode roster")
    for field, expected in identity.episode_identity().items():
        if not labels[field].eq(expected).all():
            _fail(f"v2 episode identity differs: {field}")
    table = _frame(prices, ["trade_date", "instrument"], {
        "trade_date", "instrument", "raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny",
        "suspended", "tradability_unknown", "up_limit", "down_limit", "source_sha256", "price_coordinate_sha256",
    })
    for field, expected in (("source_sha256", identity.price_source_sha256),
                            ("price_coordinate_sha256", identity.price_coordinate_sha256)):
        if not table[field].eq(expected).all():
            _fail(f"v2 price provenance differs: {field}")
    for field in ("suspended", "tradability_unknown"):
        if not table[field].map(lambda value: isinstance(value, bool)).all():
            _fail("v2 trading state must be explicit boolean")
    calendar = pd.DatetimeIndex([_day(day) for day in trading_calendar])
    if calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing:
        _fail("v2 calendar must be ordered and unique")
    positions = {day: index for index, day in enumerate(calendar)}
    episode_map = labels.set_index("episode_label_id").to_dict("index")
    price_map = table.set_index(["trade_date", "instrument"]).to_dict("index")
    output = []
    for original in originals:
        episode = episode_map[original.episode_label_id]
        if (
            episode[KEY[0]].date() != original.decision_date or episode[KEY[1]].date() != original.target_date
            or episode["instrument"] != original.instrument or episode["selection_rank"] != original.selection_rank
            or _day(episode["label_information_end"]).date() != original.label_information_end
        ):
            _fail("v2 episode clock, rank or instrument differs from unchanged original")
        base = dict(original=original, original_label_sha256=original.label_sha256)
        if original.status != "AVAILABLE":
            status = "UNAVAILABLE" if original.status == "DATA_UNAVAILABLE" else original.status
            output.append(EntryLossLabelV2(**base, status=status, reason_code=original.reason_code))
            continue
        value, reason = _entry_risk_path(original, episode, price_map, calendar, positions, identity)
        output.append(EntryLossLabelV2(**base, status="UNAVAILABLE", reason_code=reason) if reason else
                      EntryLossLabelV2(**base, status="AVAILABLE", **value))
    return tuple(output)


def _entry_risk_path(original, episode, prices, calendar, positions, identity):
    start, end = _day(original.target_date), _day(original.label_information_end)
    if start not in positions or end not in positions or end <= start:
        _fail("v2 mature path dates missing from frozen calendar")
    if _day(episode["entry_trade_date"]) != start or _day(episode["effective_exit_date"]) != end:
        _fail("v2 original policy entry/exit clock differs")
    entry_price, exit_price = _positive(episode["entry_price"]), _positive(episode["exit_price"])
    if entry_price is None or exit_price is None:
        _fail("v2 original mature policy endpoints invalid")
    for day, recorded, side in ((start, entry_price, "ENTRY"), (end, exit_price, "EXIT")):
        row = prices.get((day, original.instrument))
        if row is None or row["tradability_unknown"]:
            return None, f"{side}_TRADABILITY_UNKNOWN"
        if row["suspended"]:
            _fail("v2 matured policy claims entry/exit on full suspension")
        raw, factor = _positive(row["raw_open_cny"]), _positive(row["policy_price_per_raw_cny"])
        upper, lower = _positive(row["up_limit"]), _positive(row["down_limit"])
        if raw is None or factor is None or upper is None or lower is None:
            return None, f"{side}_PRICE_OR_LIMIT_UNKNOWN"
        if lower > upper or raw < lower - 1e-9 or raw > upper + 1e-9:
            _fail("v2 observed raw endpoint contradicts daily CNY limits")
        if (side == "ENTRY" and raw >= upper - 1e-9) or (side == "EXIT" and raw <= lower + 1e-9):
            return None, f"{side}_OPEN_LIMIT_EXECUTION_UNPROVEN"
        if not math.isclose(raw * factor, recorded, rel_tol=POLICY_PRICE_RELATIVE_TOLERANCE, abs_tol=1e-8):
            _fail("v2 raw-to-policy endpoint parity failed")
    cost = identity.cost_policy
    paid = entry_price * (1 + cost.buy_cost_bps / 10000)
    proceeds_multiplier = 1 - cost.sell_cost_bps / 10000
    net = (exit_price * proceeds_multiplier / paid - 1) * 10000
    if not math.isclose(net, original.entry_advantage_bps, rel_tol=1e-8, abs_tol=1e-6):
        _fail("v2 return parity or cost-once check failed")
    recorded = _number(episode["net_return_bps"])
    if recorded is None or not math.isclose(net, recorded, rel_tol=1e-8, abs_tol=1e-6):
        _fail("v2 frozen episode return differs")
    peak, maximum_drawdown, minimum_wealth, mark_count = 1., 0., 1., 0
    for day in calendar[positions[start]:positions[end] + 1]:
        row = prices.get((day, original.instrument))
        if row is None or row["tradability_unknown"]:
            return None, "DAILY_MARK_OR_TRADING_STATE_UNKNOWN"
        if row["suspended"]:
            if mark_count == 0:
                return None, "SUSPENSION_HAS_NO_PREVIOUS_MARK"
            continue  # Previous policy-coordinate wealth remains unchanged.
        factor = _positive(row["policy_price_per_raw_cny"])
        if factor is None:
            return None, "DAILY_MARK_COORDINATE_UNKNOWN"
        for field in ("raw_open_cny",) if day == end else ("raw_open_cny", "raw_close_cny"):
            raw = _positive(row[field])
            if raw is None:
                return None, "DAILY_MARK_UNKNOWN"
            wealth = raw * factor * proceeds_multiplier / paid
            peak = max(peak, wealth)
            minimum_wealth = min(minimum_wealth, wealth)
            maximum_drawdown = max(maximum_drawdown, (1 - wealth / peak) * 10000)
            mark_count += 1
    if not math.isclose(maximum_drawdown, original.daily_mark_max_drawdown_bps, rel_tol=1e-8, abs_tol=1e-6):
        _fail("v2 path cannot reproduce the unchanged original peak risk")
    return {"entry_net_max_loss_bps": (1 - minimum_wealth) * 10000,
            "episode_peak_to_trough_drawdown_bps": maximum_drawdown}, None
