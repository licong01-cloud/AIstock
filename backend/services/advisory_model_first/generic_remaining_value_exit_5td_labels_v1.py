"""Pure T+1 Exit advantage labels on one fixed original Top5 shadow policy."""
import math

import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import validate_roster
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import (
    GEOMETRY, KEY, POLICY, POLICY_SHA256, QUOTE_FIELDS, ordered_calendar,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

LABEL_FIELDS = ("held", "label_status", "reference_net_cny", "continue_net_cny", "sell_net_cny",
                "sell_executable", "hold_executable", "y_hold_bps", "sell_scenario_bps", "advantage_sell_bps",
                "baseline_return_bps", "sell_return_bps", "fee_schedule_sha256")


def _episode_identity(row):
    return sha(dict(key={KEY[0]: _day(row[KEY[0]]).isoformat(), KEY[1]: _day(row[KEY[1]]).isoformat(),
                         KEY[2]: row[KEY[2]]}, policy_sha256=POLICY_SHA256))


def exit_geometry_v1(*, candidates, decision_dates, calendar, development_cutoff):
    roster, _decisions, days = validate_roster(candidates, decision_dates, calendar)
    cutoff = _day(development_cutoff)
    if any(day.date() > cutoff for day in roster[KEY[0]]):
        raise ValueError("Exit5 geometry cannot consume test/holdout entry candidates")
    indexes, rows = {day: i for i, day in enumerate(days)}, []
    for row in roster.loc[roster.selection_effective_rank.le(5)].to_dict("records"):
        t = _day(row[KEY[1]])
        position = indexes[t]
        horizon = days[position:position+5]
        episode = _episode_identity(row)
        end = horizon[-1] if len(horizon) == 5 else None
        for offset in range(1, len(horizon)):
            rows.append({**row, "episode_id": episode, "entry_date": t, "endpoint_date": end,
                "s_date": horizon[offset-1], "u_date": horizon[offset], "remaining_sessions": 5-offset,
                "geometry_status": "MATURE" if end is not None and end <= cutoff else "UNSETTLED",
                "policy_sha256": POLICY_SHA256})
        if len(horizon) < 2:
            rows.append({**row, "episode_id": episode, "entry_date": t, "endpoint_date": end,
                "s_date": None, "u_date": None, "remaining_sessions": None, "geometry_status": "UNSETTLED",
                "policy_sha256": POLICY_SHA256})
    geometry = pd.DataFrame(rows, columns=GEOMETRY)
    receipt = dict(policy_sha256=POLICY_SHA256, original_candidate_count=len(roster),
        episodes=int(geometry.episode_id.nunique()), mature_episodes=int(geometry.loc[geometry.geometry_status.eq("MATURE"), "episode_id"].nunique()),
        decision_keys=len(geometry), development_cutoff=cutoff.isoformat(), future_values_read=False, fit_count=0)
    return geometry, receipt


def _fees(schedule):
    if schedule is None:
        return {}, sha(dict(default=POLICY["sell_bps"]))
    if not isinstance(schedule, dict) or len(schedule) > 5000:
        raise ValueError("Exit5 declared fee schedule exceeds its contract")
    values = {_day(day): _number(value, nonnegative=True) for day, value in schedule.items()}
    if any(value is None or value >= 10000 for value in values.values()):
        raise ValueError("Exit5 sell fees must be known fractions below 100 percent")
    return values, sha({day.isoformat(): value for day, value in sorted(values.items())})


def _executable(quote, side, price):
    if quote is None or quote["tradability_unknown"] or price is None:
        return None
    if quote["suspended"]:
        return False
    limit = quote["up_limit" if side == "buy" else "down_limit"]
    if limit is None:
        return None
    return price < limit if side == "buy" else price > limit


def build_exit_remaining_value_labels_v1(*, geometry, calendar, prices, development_cutoff, fee_schedule=None):
    days = ordered_calendar(calendar)
    indexes = {day: i for i, day in enumerate(days)}
    cutoff = _day(development_cutoff)
    if (not isinstance(geometry, pd.DataFrame) or set(geometry.columns) != set(GEOMETRY)
            or not geometry.columns.is_unique or len(geometry) > 15000):
        raise ValueError("Exit5 original geometry schema/budget differs")
    if (not isinstance(prices, pd.DataFrame) or set(prices.columns) != set(QUOTE_FIELDS)
            or not prices.columns.is_unique or len(prices) > 500000):
        raise ValueError("Exit5 label quote schema/budget differs")
    quote = prices.loc[:, QUOTE_FIELDS].copy(deep=True)
    quote["trade_date"] = quote.trade_date.map(_day)
    if quote.duplicated(["trade_date", "instrument"]).any() or quote.trade_date.gt(cutoff).any():
        raise ValueError("Exit5 labels cannot read duplicate/test/holdout quotes")
    for name in ("raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny", "up_limit", "down_limit"):
        values = quote[name].map(lambda value: _number(value, positive=True))
        quote[name] = values.astype(object).where(values.notna(), None)
    for name in ("suspended", "tradability_unknown"):
        if not quote[name].map(lambda value: type(value) is bool).all():
            raise ValueError("Exit5 tradability flags must be explicit")
    quotes = quote.set_index(["trade_date", "instrument"]).to_dict("index")
    fees, fee_identity = _fees(fee_schedule)
    def value(bar, price, day, quantity):
        if bar is None or price is None or bar["policy_price_per_raw_cny"] is None:
            return None
        amount = quantity*price*bar["policy_price_per_raw_cny"]*(1-fees.get(day, POLICY["sell_bps"])/10000.)
        return amount if math.isfinite(amount) and amount > 0 else None
    output, seen = [], set()
    for row in geometry.to_dict("records"):
        t, e = _day(row["entry_date"]), _day(row["endpoint_date"], nullable=True)
        s, u = _day(row["s_date"], nullable=True), _day(row["u_date"], nullable=True)
        symbol = row["instrument"]
        key = row["episode_id"], s
        if (key in seen or row["episode_id"] != _episode_identity(row) or row["policy_sha256"] != POLICY_SHA256
                or row["entry_date"] != _day(row[KEY[1]]) or _day(row[KEY[0]]) > cutoff
                or t not in indexes or indexes[t] < 1 or days[indexes[t]-1] != _day(row[KEY[0]])):
            raise ValueError("Exit5 episode duplicate/original policy identity differs")
        seen.add(key)
        if (s is not None and u is not None and (s not in indexes or u not in indexes or indexes[u] != indexes[s]+1
                or not 1 <= indexes[u]-indexes[t] <= 4 or not t <= s < u
                or row["remaining_sessions"] != 5-(indexes[u]-indexes[t]))) or (
                e is not None and (e not in indexes or indexes[e] != indexes[t]+4)):
            raise ValueError("Exit5 violates S-close/U-next/T+1/fixed-E/remaining geometry")
        if (s is None) != (u is None) or (e is not None and s is None):
            raise ValueError("Exit5 mature decisions need complete original S/U geometry")
        if row["geometry_status"] != ("MATURE" if e is not None and e <= cutoff else "UNSETTLED"):
            raise ValueError("Exit5 original maturity declaration differs from its fixed cutoff")
        labels = dict(held=None, label_status="UNKNOWN", reference_net_cny=None, continue_net_cny=None,
            sell_net_cny=None, sell_executable=None, hold_executable=None, y_hold_bps=None, sell_scenario_bps=None,
            advantage_sell_bps=None, baseline_return_bps=None, sell_return_bps=None, fee_schedule_sha256=fee_identity)
        if e is None or e > cutoff:
            labels["label_status"] = "UNSETTLED"
            output.append({**row, **labels})
            continue
        entry = quotes.get((t, symbol))
        entry_open = None if entry is None else entry["raw_open_cny"]
        held = _executable(entry, "buy", entry_open)
        labels["held"] = held
        if held is False:
            labels.update(label_status="NOT_HELD", baseline_return_bps=0.)
        elif held and entry["policy_price_per_raw_cny"] is not None:
            quantity = 1/(entry_open*entry["policy_price_per_raw_cny"]*(1+POLICY["buy_bps"]/10000.))
            current, sell, endpoint = (quotes.get((day, symbol)) for day in (s, u, e))
            close = None if current is None else current["raw_close_cny"]
            sell_open = None if sell is None else sell["raw_open_cny"]
            end_close = None if endpoint is None else endpoint["raw_close_cny"]
            ref = value(current, close, s, quantity)
            continuation = value(endpoint, end_close, e, quantity)
            sale = value(sell, sell_open, u, quantity)
            executable = _executable(sell, "sell", sell_open)
            end_executable = _executable(endpoint, "sell", end_close)
            labels.update(reference_net_cny=ref, continue_net_cny=continuation, sell_net_cny=sale,
                sell_executable=executable, hold_executable=end_executable)
            if continuation is not None and end_executable:
                # The fixed endpoint baseline does not depend on availability of a particular S mark.
                labels["baseline_return_bps"] = 10000*(continuation-1)
            if ref is not None and continuation is not None and end_executable:
                labels.update(label_status="AVAILABLE" if sale is not None and executable is not None else "CONTINUE_ONLY",
                    y_hold_bps=10000*(continuation/ref-1), baseline_return_bps=10000*(continuation-1))
                if sale is not None:
                    labels.update(sell_scenario_bps=10000*(sale/ref-1), advantage_sell_bps=10000*(sale-continuation)/ref,
                        sell_return_bps=10000*(sale-1))
        output.append({**row, **labels})
    result = pd.DataFrame(output, columns=(*GEOMETRY, *LABEL_FIELDS))
    receipt = dict(policy_sha256=POLICY_SHA256, fee_schedule_sha256=fee_identity,
        episode_count=int(geometry.episode_id.nunique()), decision_count=len(result),
        label_status_counts={str(key): int(value) for key, value in result.label_status.value_counts().items()},
        baseline_policy="ORIGINAL_TOP5_ENTRY_KNOWN_T_ONLY", quantity_semantics="FROZEN_POLICY_EQUIVALENT_NOT_ACTUAL_CASH_NAV",
        development_cutoff=cutoff.isoformat(), future_features_read=False, sealed_holdout_read=False,
        fit_count=0, economic_confirmation=False, deployable=False)
    return result, receipt
