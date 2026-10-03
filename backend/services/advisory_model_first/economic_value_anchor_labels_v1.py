"""Fresh price-independent scene labels over read-only frozen Advisory inputs."""
import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame, candidate_roster_sha256
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import (
    COST, SCENARIO, value_anchor_policy_sha256_v1, value_anchor_policy_v1,
)
from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def value_anchor_shadow_inputs_v1(*, prices, calendar):
    """Preserve authentic bars; this adapter must not invent fills or prices."""
    fields = ["trade_date", "instrument", "suspended", "tradability_unknown", "policy_price_per_raw_cny",
              "raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny", "up_limit", "down_limit"]
    data = _frame(prices.loc[:, fields], fields[:2], set(fields))
    for flag in ("suspended", "tradability_unknown"):
        if not data[flag].map(lambda value: type(value) is bool).all():
            raise ValueError("value scene trading flags must be explicitly known booleans")
    market = data.rename(columns={"trade_date": "datetime"}).copy()
    for name in ("open", "high", "low", "close"):
        market[name] = pd.to_numeric(market[f"raw_{name}_cny"], errors="coerce")*pd.to_numeric(market.policy_price_per_raw_cny, errors="coerce")
    market["factor"] = market.policy_price_per_raw_cny
    # Limits in this frozen snapshot are raw yuan; the existing execution helper
    # converts policy OHLC through factor before checking the actual limit.
    market["up_limit_price"], market["down_limit_price"] = market.up_limit, market.down_limit
    market["limit_up"], market["limit_down"] = 1., 1.
    market = market.set_index(["datetime", "instrument"]).sort_index()
    days = pd.DatetimeIndex(pd.to_datetime(list(calendar))).normalize()
    if days.has_duplicates or not days.is_monotonic_increasing or days.hasnans:
        raise ValueError("value scene needs the original unique ordered calendar")
    cash = pd.DataFrame({"datetime": days, "open": 1.}).set_index("datetime")
    return market, cash, data.loc[data.suspended, ["trade_date", "instrument"]].copy(), days


def _positive(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if not isinstance(value, (bool, np.bool_)) and np.isfinite(result) and result > 0 else None


def build_value_anchor_labels_v1(*, candidates, rankings, prices, references, calendar, parent_identity):
    parent = EconomicEntryInputIdentityV1.model_validate(parent_identity)
    if parent.cost_policy != COST:
        raise ValueError("value scene must use the frozen cost-once contract")
    roster = _frame(candidates.loc[:, KEY+["selection_effective_rank"]], KEY, set(KEY)|{"selection_effective_rank"})
    ranked = _frame(rankings, KEY, set(KEY)|{"selection_effective_rank", "combined_score"})
    for frame in (roster, ranked):
        if (not frame.selection_effective_rank.map(lambda value: isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_))).all()
                or frame.duplicated([KEY[0], "instrument"]).any() or frame.groupby(KEY[0])[KEY[1]].nunique().ne(1).any()):
            raise ValueError("value scene has malformed rank/target/unique candidate identity")
    if candidate_roster_sha256(roster) != parent.candidate_roster_sha256:
        raise ValueError("value scene does not match the parent frozen candidate roster")
    if (not roster.groupby(KEY[0]).size().eq(20).all()
            or not roster.groupby(KEY[0]).selection_effective_rank.apply(lambda values: set(values) == set(range(1, 21))).all()
            or not ranked.groupby(KEY[0]).selection_effective_rank.apply(lambda values: len(values) == 40 and set(values) == set(range(1, 41))).all()):
        raise ValueError("value scene requires exact original Top20 and Top40 context")
    rank_map = ranked.set_index(KEY).selection_effective_rank.to_dict()
    if any(rank_map.get(tuple(row[name] for name in KEY)) != row["selection_effective_rank"] for row in roster.to_dict("records")):
        raise ValueError("value scene roster differs from frozen rankings")
    price = _frame(prices, ["trade_date", "instrument"], {"trade_date", "instrument", "source_sha256", "price_coordinate_sha256"})
    reference = _frame(references, KEY, set(KEY)|{"target_reference_raw_cny", "reference_visible_through", "source_sha256"})
    if (not price.source_sha256.eq(parent.price_source_sha256).all()
            or not price.price_coordinate_sha256.eq(parent.price_coordinate_sha256).all()
            or not reference.source_sha256.eq(parent.reference_source_sha256).all()):
        raise ValueError("value scene source or price-coordinate identity differs")
    visible = pd.to_datetime(reference.reference_visible_through)
    if visible.isna().any() or not (visible <= reference[KEY[0]]).all():
        raise ValueError("value scene reference sees after D")
    market, cash, suspend, days = value_anchor_shadow_inputs_v1(prices=price, calendar=calendar)
    scenario_hash = value_anchor_policy_sha256_v1()
    request_hash = canonical_json_sha256({"scenario": scenario_hash, "parent": parent.identity_sha256,
        "roster": [{name: str(value) for name, value in row.items()} for row in roster.to_dict("records")]})
    result = build_policy_episode_labels(rankings=ranked, daily=market, benchmark_daily=cash, suspend_rows=suspend,
        trading_calendar=days, policy=value_anchor_policy_v1(), policy_sha256=scenario_hash,
        cost_policy=COST, request_identity={"request_id": "advvalue_labels_"+request_hash[:24]},
        candidate_decision_dates=sorted(roster[KEY[0]].unique()))
    quotes, refs = price.set_index(["trade_date", "instrument"]).to_dict("index"), reference.set_index(KEY).to_dict("index")
    output = []
    for row in result.labels.to_dict("records"):
        row.update(value_scenario=SCENARIO, parent_input_identity_sha256=parent.identity_sha256,
            gross_value_ratio=None, path_min_value_ratio=None, value_label_status="UNKNOWN", value_label_reason=row["label_status"],
            source_evidence=parent.source_evidence, evidence_limitations=parent.evidence_limitations,
            native_identity="UNPROVEN", decision_use="NAVIGATION_ONLY")
        key = tuple(row[name] for name in KEY)
        target, symbol = row[KEY[1]], row[KEY[2]]
        ref, entry = refs.get(key), quotes.get((target, symbol))
        if row["label_status"] != "MATURED" or ref is None or entry is None:
            output.append(row)
            continue
        anchor, factor = _positive(ref["target_reference_raw_cny"]), _positive(entry["policy_price_per_raw_cny"])
        entry_open, upper = _positive(entry["raw_open_cny"]), _positive(entry["up_limit"])
        if None in (anchor, factor, entry_open, upper) or entry["tradability_unknown"] or entry_open >= upper:
            row["value_label_reason"] = "ENTRY_OPEN_EXECUTION_UNPROVEN"
            output.append(row)
            continue
        denominator = anchor*factor
        if not np.isfinite(denominator) or denominator <= 0:
            row["value_label_reason"] = "REFERENCE_COORDINATE_UNPROVEN"
            output.append(row)
            continue
        endpoint = pd.Timestamp(row["effective_exit_date"])
        last = entry_open*factor
        marks, reason = [], None
        for day in days[(days >= target) & (days <= endpoint)]:
            quote = quotes.get((day, symbol))
            if quote is None or quote["tradability_unknown"]:
                reason = "HOLDING_MARK_UNPROVEN"
                break
            if quote["suspended"]:
                marks.append(last)
                continue
            f = _positive(quote["policy_price_per_raw_cny"])
            opened = _positive(quote["raw_open_cny"])
            if None in (f, opened):
                reason = "HOLDING_COORDINATE_OR_OPEN_UNPROVEN"
                break
            if day == endpoint:
                lower = _positive(quote["down_limit"])
                if lower is None or opened <= lower:
                    reason = "EXIT_OPEN_EXECUTION_UNPROVEN"
                    break
                marks.append(opened*f)
                # Do not consume this day's close after an at-open exit.
                continue
            closed = _positive(quote["raw_close_cny"])
            if closed is None:
                reason = "HOLDING_CLOSE_UNPROVEN"
                break
            if day != target:
                marks.append(opened*f)
            last = closed*f
            marks.append(last)
        if reason is None and marks:
            exit_quote = quotes[(endpoint, symbol)]
            terminal = float(exit_quote["raw_open_cny"])*float(exit_quote["policy_price_per_raw_cny"])
            if not np.isclose(terminal, row["exit_price"], rtol=1e-10, atol=1e-10):
                raise ValueError("value scene endpoint coordinate contradicts the episode")
            row.update(gross_value_ratio=terminal/denominator, path_min_value_ratio=min(marks)/denominator,
                       value_label_status="AVAILABLE", value_label_reason=None)
        else:
            row["value_label_reason"] = reason or "PATH_EMPTY"
        output.append(row)
    return pd.DataFrame(output)
