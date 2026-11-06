"""Four fixed-slot overlays; typed financial unknowns are never cash or NAV."""
from collections import Counter

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    ARMS, CLOCK, COHORT_ARMS, CONDITION_KINDS, CONTEXT_COLUMNS, INTERVENTION, KEY,
    POLICY_SHA256, ROSTER_KEY, SCHEMA_SHA256, UNKNOWN, VALUATION_POLICY_SHA256,
)


def _validate(rows, days, predictions, plan, calendar):
    sessions = pd.to_datetime(calendar)
    expected = list(sessions[(sessions >= pd.Timestamp(plan.evaluation_start)) & (sessions <= pd.Timestamp(plan.evaluation_end))])
    if (list(sessions) != sorted(set(sessions)) or rows.duplicated(list(ROSTER_KEY)).any()
            or days.duplicated(["package_id", "decision_date"]).any() or set(predictions) != set(ARMS)
            or len(rows) > 860 or set(days.package_id) != {s.package_id for s in plan.sources}):
        raise ValueError("evaluation clocks, original identities or budget differ")
    for source in plan.sources:
        dates = days.loc[days.package_id.eq(source.package_id)]
        if dates.decision_date.tolist() != expected:
            raise ValueError("an original package/date was removed")
        own = rows.loc[rows.package_id.eq(source.package_id)]
        if (not own.manifest_sha256.eq(source.manifest_sha256).all() or not own.run_id.eq(source.run_id).all()
                or not own.source_id.eq(source.source_id).all()):
            raise ValueError("evaluation source package identity differs")
        for item in dates.itertuples(index=False):
            i = sessions.get_loc(item.decision_date)
            h = sessions[i+5] if i+5 < len(sessions) else pd.NaT
            group = own.loc[own[KEY[0]].eq(item.decision_date)]
            if (i+1 >= len(sessions) or item.target_date != sessions[i+1]
                    or not (item.horizon_end == h or pd.isna(item.horizon_end) and pd.isna(h))
                    or len(group) != min(5, item.original_candidates)
                    or group.selection_effective_rank.tolist() != list(range(1, len(group)+1))
                    or not group.candidate_group_size.eq(item.original_candidates).all()
                    or not group[KEY[1]].eq(item.target_date).all()
                    or not group.label_information_end.eq(h).all()):
                raise ValueError("five fixed original slots or T..T+4 clock changed")
    if not rows.package_id.isin(days.package_id).all() or not rows[KEY[0]].isin(expected).all():
        raise ValueError("evaluation includes a foreign original slot")
    maps = {}
    for arm, frame in predictions.items():
        if (frame.duplicated(list(ROSTER_KEY)).any() or len(frame) != len(rows)
                or not frame.arm.eq(arm).all() or not frame.information_clock.eq(CLOCK).all()
                or not frame.condition_kind.eq(CONDITION_KINDS[1]).all()
                or not frame.schema_sha256.eq(SCHEMA_SHA256).all() or not frame.policy_sha256.eq(POLICY_SHA256).all()
                or not frame.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
                or not frame.calibration_status.eq("UNCALIBRATED").all() or not frame.deployable.eq(False).all()
                or frame.bundle_sha256.nunique() != 1
                or not frame.status.isin({"ACCEPTABLE_VALUE_PREDICTED", "AVOID", *UNKNOWN}).all()):
            raise ValueError("prediction schema, role or policy differs")
        expected_keys = pd.MultiIndex.from_frame(rows[list(ROSTER_KEY)])
        indexed = frame.set_index(list(ROSTER_KEY))
        if set(indexed.index) != set(expected_keys):
            raise ValueError("prediction removed/added an original slot")
        aligned = indexed.reindex(expected_keys)
        if not np.array_equal(aligned.action, np.where(aligned.status.eq("ACCEPTABLE_VALUE_PREDICTED"), "TAKE",
                np.where(aligned.status.eq("AVOID"), "SKIP", "NO_ADVICE"))):
            raise ValueError("prediction action and status disagree")
        known = aligned.action.ne("NO_ADVICE")
        expected, tail, utility = (aligned[n].map(_number).to_numpy(dtype=float)
            for n in ("expected_net_bps", "expected_terminal_excess_loss_bps", "expected_utility_bps"))
        if (not np.isfinite(np.column_stack((expected, tail, utility))[known]).all()
                or not np.isnan(np.column_stack((expected, tail, utility))[~known]).all()
                or (tail[known] < 0).any() or not np.allclose(utility[known], expected[known]-tail[known], atol=1e-9, rtol=0)
                or not np.array_equal(aligned.action.loc[known].eq("TAKE"), (expected[known] > 0) & (utility[known] > 0))):
            raise ValueError("prediction financial values contradict their known action")
        for field in ("observed_gap_bps", *CONTEXT_COLUMNS):
            a, b = (v.map(_number).to_numpy(dtype=float) for v in (aligned[field], rows[field]))
            # g is algebraically recovered from (price/reference-1) by the
            # public query; accept only sub-nanobasis-point float roundoff.
            same = np.allclose(a, b, atol=1e-8, rtol=0, equal_nan=True) if field == "observed_gap_bps" else np.array_equal(a, b, equal_nan=True)
            if not same:
                raise ValueError("frozen prediction context differs from evaluation")
        maps[arm] = indexed.to_dict("index")
    if len({predictions[a].bundle_sha256.iloc[0] for a in ARMS if len(predictions[a])}) > 1:
        raise ValueError("comparison arms use different common bundles")
    for key in maps[ARMS[0]]:
        if (maps[ARMS[0]][key]["action"] == "NO_ADVICE") != (maps[ARMS[1]][key]["action"] == "NO_ADVICE"):
            raise ValueError("matched and candidate do not share their common input/support")
    return maps


def _episode(row, maps, mature):
    key = tuple(row[n] for n in ROSTER_KEY)
    value = _number(row["hypothetical_liquidation_net_bps"])
    gross = _number(row["valuation_gross_terminal_ratio"])
    mark = _number(row["mark_to_market_net_bps"])
    financial_known = row["valuation_status"] in ("AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE") and value is not None
    if mature and row["valuation_status"] == "AVAILABLE" and (value is None or gross is None):
        raise ValueError("AVAILABLE valuation lacks its financial identity")
    unbuyable = row["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE"
    if unbuyable and value != 0:
        raise ValueError("known unbuyable entry must retain the native cash outcome")
    gap = _number(row["observed_gap_bps"])
    actions, composition, contribution = {}, {}, {}
    for arm in COHORT_ARMS:
        if arm == "baseline":
            action = "TAKE"
        elif arm == "rule_300bps":
            action = "NO_ADVICE" if gap is None else "TAKE" if -300 <= gap <= 300 else "SKIP"
        else:
            action = maps[arm][key]["action"]
        actions[arm] = action
        effective = "NOT_EXECUTABLE" if unbuyable else "BASELINE_UNCHANGED_NO_ADVICE" if action == "NO_ADVICE" else action
        composition[arm] = effective
        contribution[arm] = None if not mature or not financial_known else 0. if effective in ("SKIP", "NOT_EXECUTABLE") else value
    return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(),
        target_date=row[KEY[1]].date().isoformat(), instrument=row["instrument"], rank=int(row["selection_effective_rank"]),
        label_cluster=row["label_cluster"], mature=bool(mature), financial_known=bool(financial_known),
        valuation_status=row["valuation_status"], valuation_reason=row["valuation_reason"],
        exit_execution_status=row["exit_execution_status"], baseline_net_bps=value, mark_only_net_bps=mark,
        nominal_cost_bps=None if gross is None or value is None or gap is None else 10000*(gross/(1+gap/10000)-1)-value,
        d_anchor_path_min_ratio=_number(row["valuation_path_min_ratio"]),
        actions=actions, composition=composition, contributions_bps=contribution,
        model_statuses={a: maps[a][key]["status"] for a in ARMS},
        market_regime="UNKNOWN" if _number(row[CONTEXT_COLUMNS[0]]) is None else "UP" if row[CONTEXT_COLUMNS[0]] > 0 else "DOWN_OR_FLAT",
        actual_fill_proven=False, realized_return_bps=None)


def _panels(episodes, days, packages):
    result = {a: [] for a in COHORT_ARMS}
    for d in days:
        items = [e for e in episodes if e["decision_date"] == d and e["package_id"] in packages]
        for arm in COHORT_ARMS:
            values = [e["contributions_bps"][arm] for e in items]
            result[arm].append(None if any(v is None for v in values) else float(sum(values)/(5*len(packages))))
    return result


def _summary(*, episodes, days, packages):
    selected = [e for e in episodes if e["package_id"] in packages and e["decision_date"] in days]
    panels = _panels(selected, days, packages)
    deltas = {op: [None if a is None or b is None else a-b for a, b in zip(panels[ARMS[1]], panels[op], strict=True)]
        for op in ("baseline", ARMS[0])}
    statistics = paired_statistics({"baseline": deltas["baseline"], "matched_core": deltas[ARMS[0]]})
    statistics[ARMS[0]] = statistics.pop("matched_core")
    attribution = {}
    for opponent in ("baseline", ARMS[0]):
        paired = {d for d, v in zip(days, deltas[opponent], strict=True) if v is not None}
        scale = 1/(5*len(packages)*len(paired)) if paired else 0.
        parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
            no_advice_identity_contribution_bps=0., net_increment_bps=0.)
        clusters, dates, regimes = set(), set(), Counter()
        for e in selected:
            if e["decision_date"] not in paired:
                continue
            a, b = e["contributions_bps"][ARMS[1]], e["contributions_bps"][opponent]
            delta = a-b
            parts["net_increment_bps"] += delta*scale
            if e["composition"][ARMS[1]] == "BASELINE_UNCHANGED_NO_ADVICE" and opponent == "baseline" and delta != 0:
                raise ValueError("NO_ADVICE manufactured a baseline increment")
            a_action, b_action = ("TAKE" if e["composition"][arm] == "BASELINE_UNCHANGED_NO_ADVICE"
                else e["composition"][arm] for arm in (ARMS[1], opponent))
            if a_action != b_action and "NOT_EXECUTABLE" not in (a_action, b_action):
                clusters.add(e["label_cluster"])
                dates.add(e["decision_date"])
                regimes[e["market_regime"]] += 1
            if delta != 0:
                if a == 0 and b != 0:
                    parts["avoided_loss_bps" if b < 0 else "missed_profit_bps"] += abs(b)*scale
                elif b == 0 and a != 0:
                    parts["new_take_profit_bps" if a > 0 else "new_take_loss_bps"] += abs(a)*scale
                else:
                    raise ValueError("fixed-slot interventions must be native take versus skip")
        reconciled = parts["avoided_loss_bps"]-parts["missed_profit_bps"]+parts["new_take_profit_bps"]-parts["new_take_loss_bps"]
        if not np.isclose(parts["net_increment_bps"], reconciled, atol=1e-9):
            raise ValueError("economic attribution does not reconcile")
        attribution[opponent] = dict(parts, intervened_unique_stock_days=len(clusters), intervention_days=len(dates),
            intervention_day_fraction=len(dates)/len(days) if days else 0., regime_distribution=dict(regimes))
    cohorts = {}
    for arm in COHORT_ARMS:
        vals = panels[arm]
        complete = [v for v in vals if v is not None]
        cohorts[arm] = dict(original_mature_days=len(days), known_group_days=len(complete), unknown_group_days=len(vals)-len(complete),
            complete_fixed_slot_mean_bps=float(np.mean(complete)) if len(complete) == len(vals) and complete else None,
            known_group_diagnostic_mean_bps=float(np.mean(complete)) if complete else None,
            mean_utility_bps=float(np.mean([v-max(-v-800, 0) for v in complete])) if len(complete) == len(vals) and complete else None,
            mean_terminal_excess_loss_bps=float(np.mean([max(-v-800, 0) for v in complete])) if len(complete) == len(vals) and complete else None,
            risk_unit="FIXED_FIVE_SLOT_GROUP_TERMINAL_NOT_PATH_NOT_NAV",
            actions=dict(Counter(e["actions"][arm] for e in selected)), composition=dict(Counter(e["composition"][arm] for e in selected)),
            nominal_fee_bps_on_known_slots=float(sum((e["nominal_cost_bps"] or 0.) for e in selected
                if e["financial_known"] and e["composition"][arm] in ("TAKE", "BASELINE_UNCHANGED_NO_ADVICE"))/(5*len(packages)*len(days))) if days else None)
        budget = 5*len(packages)*len(days)
        cohorts[arm].update(fixed_slot_budget=budget, original_observed_slots=len(selected),
            model_advice_fraction=sum(e["actions"][arm] != "NO_ADVICE" for e in selected)/budget if budget else None,
            no_advice_fraction=sum(e["actions"][arm] == "NO_ADVICE" for e in selected)/budget if budget else None,
            take_fraction=sum(e["composition"][arm] == "TAKE" for e in selected)/budget if budget else None,
            known_cash_fraction=(budget-len(selected)+sum(e["composition"][arm] in ("SKIP", "NOT_EXECUTABLE") and e["financial_known"] for e in selected))/budget if budget else None,
            unknown_financial_fraction=sum(not e["financial_known"] for e in selected)/budget if budget else None,
            no_advice_reasons=dict(Counter(e["model_statuses"][arm] for e in selected if e["actions"][arm] == "NO_ADVICE")) if arm in ARMS else {})
    return dict(packages=packages, original_mature_days=days, cohorts=cohorts, paired_statistics=statistics,
        attribution=attribution, fixed_slot_panels_bps=panels)


def evaluate_sector_relative_open_v1(*, rows, days, predictions, plan, calendar):
    maps = _validate(rows, days, predictions, plan, calendar)
    mature_dates = sorted(days.loc[days.horizon_end.le(pd.Timestamp(plan.evaluation_end)), "decision_date"].unique())
    mature = {pd.Timestamp(d).date().isoformat() for d in mature_dates}
    episodes = [_episode(r, maps, r[KEY[0]].date().isoformat() in mature) for r in rows.to_dict("records")]
    packages = [s.package_id for s in plan.sources]
    dates = sorted(mature)
    aggregate = _summary(episodes=episodes, days=dates, packages=packages)
    by_package = {p: _summary(episodes=episodes, days=dates, packages=[p]) for p in packages}
    by_week = {week: _summary(episodes=episodes, days=[d for d in dates if str(pd.Timestamp(d).to_period("W")) == week], packages=packages)
        for week in sorted({str(pd.Timestamp(d).to_period("W")) for d in dates})}
    stats, attr, cohorts = (aggregate[k] for k in ("paired_statistics", "attribution", "cohorts"))
    base, matched, candidate = (cohorts[k] for k in ("baseline", ARMS[0], ARMS[1]))
    conditions = dict(
        base_increment=stats["baseline"]["mean_increment_bps"] is not None and stats["baseline"]["mean_increment_bps"] >= 5.,
        matched_increment=stats[ARMS[0]]["mean_increment_bps"] is not None and stats[ARMS[0]]["mean_increment_bps"] > 0,
        complete_finance=all(c["unknown_group_days"] == 0 for c in cohorts.values()),
        utility_not_worse=all(candidate["mean_utility_bps"] is not None and c["mean_utility_bps"] is not None and candidate["mean_utility_bps"] >= c["mean_utility_bps"] for c in (base, matched)),
        terminal_tail_not_worse=all(candidate["mean_terminal_excess_loss_bps"] is not None and c["mean_terminal_excess_loss_bps"] is not None and candidate["mean_terminal_excess_loss_bps"] <= c["mean_terminal_excess_loss_bps"] for c in (base, matched)),
        supported_interventions=all(a["intervened_unique_stock_days"] >= INTERVENTION["minimum_stock_days"]
            and a["intervention_days"] >= INTERVENTION["minimum_days"] and a["intervention_day_fraction"] >= INTERVENTION["minimum_day_fraction"] for a in attr.values()))
    return dict(status="EXPLORATORY_NAVIGATION_ONLY" if all(conditions.values()) else "EXACT_CANDIDATE_NOT_SUPPORTED",
        hypothesis="GP5-SECTOR-RELATIVE-OPEN-1", policy_sha256=POLICY_SHA256, schema_sha256=SCHEMA_SHA256,
        evaluation_original_rows=len(rows), original_package_days=len(days), original_decision_days=len(days.decision_date.unique()),
        mature_decision_days=len(dates), immature_decision_days=len(days.decision_date.unique())-len(dates),
        aggregate=aggregate, by_package=by_package, by_week=by_week, episodes=episodes, navigation_conditions=conditions,
        test_or_sealed_finance_consumed=False, independent_oos_evidence=False, confirmation=False, deployable=False,
        objective_contract="RISK_MANAGED_ADVISORY", study_type="EXPLORATORY_SCREEN", decision_use="NAVIGATION_ONLY",
        measurement="FIXED_FIVE_ORIGINAL_SLOT_FIVE_SESSION_NOMINAL_NOT_NAV_NOT_FILL_NOT_EXCESS")
