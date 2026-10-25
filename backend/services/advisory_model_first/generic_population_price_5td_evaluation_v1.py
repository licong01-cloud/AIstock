"""Fixed original five-slot cohort accounting, not NAV or an activation decision."""
import math

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import STATISTICS, STUDY_ARMS
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY, POLICY_SHA256

ARMS = ("baseline", "rule_300bps", *STUDY_ARMS)
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_PRICE_SCENARIO", "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION"}
NUMERIC_PREDICTIONS = ("expected_net_bps", "downside_q90_bps", "profit_probability", "path_min_ratio_q10")


def _finite(value):
    return None if pd.isna(value) else _number(value)


def _net(gap, terminal):
    return 10000*(terminal*(1-POLICY["sell_bps"]/10000)/((1+gap/10000)*(1+POLICY["buy_bps"]/10000))-1)


def _statistics(panel):
    known = [v for v in panel if v is not None]
    result = dict(paired_days=len(known), original_mature_days=len(panel),
                  mean_increment_bps=float(np.mean(known)) if known else None,
                  confidence_interval_95_bps=None, mde_80_bps=None)
    block = STATISTICS["block_sessions"]
    if len(known) != len(panel) or len(known) < 2*block:
        result["inference_status"] = "UNAVAILABLE_ORIGINAL_PANEL_HOLE_OR_TOO_SHORT"
        return result
    values = np.array(known, dtype=float)
    rng = np.random.default_rng(STATISTICS["bootstrap_seed"])
    starts = rng.integers(0, len(values)-block+1, size=(STATISTICS["bootstrap_count"], math.ceil(len(values)/block)))
    positions = (starts[:, :, None]+np.arange(block)).reshape(len(starts), -1)[:, :len(values)]
    means = values[positions].mean(axis=1)
    result.update(confidence_interval_95_bps=np.quantile(means, [.025, .975]).tolist(),
                  mde_80_bps=float((1.959963984540054+.8416212335729143)*means.std(ddof=1)),
                  inference_status="MOVING_BLOCK_ORIGINAL_DAY_CLUSTER_EXPLORATORY")
    return result


def _validated(rows, clusters, days, predictions, calendar, start, end):
    required = {*KEY, "source_id", "package_id", "manifest_sha256", "selection_effective_rank",
                "candidate_group_size", "label_information_end", "label_status", "observed_gap_bps",
                "gross_terminal_ratio", "path_min_ratio", "policy_sha256", "label_contract", "csi300_ret_5"}
    if (not required.issubset(rows) or not set(KEY).issubset(clusters)
            or not {"source_id", KEY[0], "candidate_count", "roster_status"}.issubset(days)
            or set(predictions) != set(STUDY_ARMS)):
        raise ValueError("population evaluation schema/arms differ")
    rows, clusters, days = rows.copy(), clusters.copy(), days.copy()
    for frame in (rows, clusters, days):
        frame[KEY[0]] = frame[KEY[0]].map(_day)
        if not frame[KEY[0]].between(start, end).all():
            raise ValueError("population evaluation would consume another window")
    for frame in (rows, clusters):
        frame[KEY[1]] = frame[KEY[1]].map(_day)
    rows.label_information_end = rows.label_information_end.map(_day)
    if (rows.duplicated(["source_id", *KEY]).any() or clusters.duplicated(list(KEY)).any()
            or days.duplicated(["source_id", KEY[0]]).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()
            or not rows.label_status.isin(["AVAILABLE", "UNKNOWN", "IMMATURE", "ENTRY_NOT_EXECUTABLE"]).all()):
        raise ValueError("population evaluation keys/policy/labels contradict")
    positions = {d: i for i, d in enumerate(calendar)}
    for d, t, h in rows[[KEY[0], KEY[1], "label_information_end"]].drop_duplicates().itertuples(index=False, name=None):
        i = positions.get(d)
        if i is None or i+5 >= len(calendar) or (t, h) != (calendar[i+1], calendar[i+5]):
            raise ValueError("population evaluation original D/T/H clock differs")
    if not rows.label_status.eq("IMMATURE").eq(rows.label_information_end.gt(end)).all():
        raise ValueError("population evaluation maturity contradicts its original cutoff")
    for g, y, low in rows.loc[rows.label_status.eq("AVAILABLE"), ["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"]].itertuples(index=False, name=None):
        g, y, low = _finite(g), _finite(y), _finite(low)
        if g is None or g <= -10000 or y is None or low is None or not 0 < low <= y:
            raise ValueError("population AVAILABLE original outcome contradicts")
    keys = set(clusters[list(KEY)].itertuples(index=False, name=None))
    if keys != set(rows[list(KEY)].itertuples(index=False, name=None)):
        raise ValueError("population evaluation original cluster coverage differs")
    shared = rows.groupby(list(KEY))[["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio", "label_status", "label_information_end"]]
    if shared.nunique(dropna=False).gt(1).any().any():
        raise ValueError("population same stock/date outcome contradicts across original packages")
    maps = {}
    for arm, prediction in predictions.items():
        prediction = prediction.copy()
        if not {*KEY, "status", *NUMERIC_PREDICTIONS, "policy_sha256"}.issubset(prediction):
            raise ValueError("population prediction schema differs")
        prediction[KEY[0]] = prediction[KEY[0]].map(_day)
        prediction[KEY[1]] = prediction[KEY[1]].map(_day)
        if (prediction.duplicated(list(KEY)).any() or not prediction.policy_sha256.eq(POLICY_SHA256).all()
                or set(prediction[list(KEY)].itertuples(index=False, name=None)) != keys
                or not prediction.status.isin({"ACCEPTABLE", "AVOID", *UNKNOWN}).all()):
            raise ValueError("population predictions lost original keys/policy/status")
        maps[arm] = {}
        for item in prediction.to_dict("records"):
            if item["status"] not in UNKNOWN:
                values = {n: _finite(item[n]) for n in NUMERIC_PREDICTIONS}
                if (any(v is None for v in values.values()) or not 0 <= values["profit_probability"] <= 1
                        or values["downside_q90_bps"] < 0 or values["path_min_ratio_q10"] <= 0
                        or (item["status"] == "ACCEPTABLE") != (
                            values["expected_net_bps"] > 0 and values["downside_q90_bps"] <= POLICY["risk_bps"])):
                    raise ValueError("population known prediction numeric/action contract differs")
            maps[arm][tuple(item[n] for n in KEY)] = item
    if not set(rows.source_id).issubset(set(days.source_id)):
        raise ValueError("population evaluation rows have no original day provenance")
    for _, group in rows.groupby("source_id"):
        if len(group[["package_id", "manifest_sha256"]].drop_duplicates()) != 1:
            raise ValueError("population source package/manifest identity changed")
    return rows, clusters, days, maps


def _episodes(group, maps, mature):
    result = []
    for row in group.loc[group.selection_effective_rank.le(5)].to_dict("records"):
        key = tuple(row[n] for n in KEY)
        gap, terminal, path = (_finite(row[n]) for n in ("observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"))
        status = row["label_status"]
        if status == "AVAILABLE" and (not mature or gap is None or gap <= -10000 or terminal is None
                                      or path is None or not 0 < path <= terminal):
            raise ValueError("population AVAILABLE outcome/clock contradicts")
        if mature and status == "IMMATURE":
            raise ValueError("population immature label contradicts its original clock")
        actual = _net(gap, terminal) if status == "AVAILABLE" else None
        actions = {"baseline": "TAKE", "rule_300bps": "UNKNOWN" if gap is None else (
            "TAKE" if -300 <= gap <= 300 else "AVOID")}
        predicted = {arm: maps[arm][key] for arm in STUDY_ARMS}
        actions.update({arm: "TAKE" if p["status"] == "ACCEPTABLE" else "AVOID" if p["status"] == "AVOID" else "UNKNOWN"
                        for arm, p in predicted.items()})
        contributions = {arm: None if not mature else 0. if status == "ENTRY_NOT_EXECUTABLE" or action != "TAKE"
                         else actual for arm, action in actions.items()}
        result.append(dict(decision_date=key[0].isoformat(), instrument=key[2], source_id=row["source_id"],
            rank=int(row["selection_effective_rank"]), label_status=status, actual_net_bps=actual,
            gross_terminal_ratio=terminal, path_min_ratio=path, observed_gap_bps=gap,
            actions=actions, contributions_bps=contributions,
            predictions={arm: {n: _finite(p[n]) if n in NUMERIC_PREDICTIONS else p[n]
                               for n in ("status", *NUMERIC_PREDICTIONS)} for arm, p in predicted.items()}))
    return result


def _groups(rows, days, maps, calendar, start, end):
    sources = sorted(set(days.source_id))
    decision_dates = [d for d in calendar if start <= d <= end]
    positions = {d: i for i, d in enumerate(calendar)}
    rosters = {(s, d): g for (s, d), g in rows.groupby(["source_id", KEY[0]], sort=False)}
    declared = {(r["source_id"], r[KEY[0]]): r for r in days.to_dict("records")}
    groups, episodes = [], []
    for source in sources:
        for d in decision_dates:
            original = declared.get((source, d))
            group = rosters.get((source, d), rows.iloc[:0])
            state = "UNKNOWN_UNDECLARED_DAY" if original is None else original["roster_status"]
            count = None if original is None else _finite(original["candidate_count"])
            if (state == "PRESENT" and (count != len(group) or count is None or count <= 0)
                    or state == "EXPLICIT_EMPTY" and (count != 0 or len(group))
                    or state == "UNKNOWN_ABSENT_FROZEN_DAY" and (count is not None or len(group))
                    or state not in {"PRESENT", "EXPLICIT_EMPTY", "UNKNOWN_ABSENT_FROZEN_DAY", "UNKNOWN_UNDECLARED_DAY"}):
                raise ValueError("population original roster count/state differs")
            if len(group):
                ranks = sorted(group.selection_effective_rank)
                if (ranks != list(range(1, len(group)+1)) or not group.candidate_group_size.eq(len(group)).all()):
                    raise ValueError("population original unique rank/group size differs")
            h = calendar[positions[d]+5] if positions[d]+5 < len(calendar) else None
            mature = h is not None and h <= end
            items = _episodes(group, maps, mature)
            episodes.extend(items)
            known_roster = state in {"PRESENT", "EXPLICIT_EMPTY"}
            returns = {}
            for arm in ARMS:
                values = [item["contributions_bps"][arm] for item in items]
                returns[arm] = None if not mature or not known_roster or any(v is None for v in values) else sum(values)/5
            regime_values = group.csi300_ret_5.dropna().map(_number).unique()
            if len(regime_values) > 1:
                raise ValueError("population same-D index observation contradicts")
            regime = "UNKNOWN" if not len(regime_values) else "POSITIVE" if regime_values[0] > 0 else (
                "NEGATIVE" if regime_values[0] < 0 else "ZERO")
            groups.append(dict(source_id=source, decision_date=d.isoformat(), roster_status=state,
                original_candidate_count=None if count is None else int(count), top5_slots=len(items),
                empty_slots=5-len(items) if known_roster else None, horizon_mature=mature,
                lagged_csi300_regime=regime, cohort_net_bps=returns))
    return groups, episodes


def _pairs(groups, episodes, opponent, source=None, *, inference=True):
    selected = [g for g in groups if source is None or g["source_id"] == source]
    dates = sorted({g["decision_date"] for g in selected if g["horizon_mature"]})
    by_day = {d: [g for g in selected if g["decision_date"] == d] for d in dates}
    panel, paired = [], {}
    for d, cells in by_day.items():
        values = [None if any(g["cohort_net_bps"][a] is None for a in ("candidate_transfer", opponent))
                  else g["cohort_net_bps"]["candidate_transfer"]-g["cohort_net_bps"][opponent] for g in cells]
        value = None if any(v is None for v in values) else float(np.mean(values))
        panel.append(value)
        if value is not None:
            paired[d] = len(cells)
    parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
                 unknown_cash_contribution_bps=0., known_action_contribution_bps=0.)
    all_changes = [r for r in episodes if (source is None or r["source_id"] == source)
                   and r["label_status"] != "ENTRY_NOT_EXECUTABLE"
                   and "UNKNOWN" not in (r["actions"]["candidate_transfer"], r["actions"][opponent])
                   and r["actions"]["candidate_transfer"] != r["actions"][opponent]]
    all_days = {r["decision_date"] for r in all_changes}
    known_count, intervention_days = 0, set()
    for item in episodes:
        d = item["decision_date"]
        if d not in paired or (source is not None and item["source_id"] != source):
            continue
        a, b = item["actions"]["candidate_transfer"], item["actions"][opponent]
        delta = item["contributions_bps"]["candidate_transfer"]-item["contributions_bps"][opponent]
        scale = 1/(5*paired[d]*len(paired))
        if "UNKNOWN" in (a, b):
            parts["unknown_cash_contribution_bps"] += delta*scale
        else:
            parts["known_action_contribution_bps"] += delta*scale
            actual = item["actual_net_bps"]
            if a != b and actual is not None:
                known_count += 1
                intervention_days.add(d)
                if a == "AVOID":
                    name = "avoided_loss_bps" if actual < 0 else "missed_profit_bps"
                    parts[name] += abs(actual)*scale
                else:
                    name = "new_take_profit_bps" if actual >= 0 else "new_take_loss_bps"
                    parts[name] += abs(actual)*scale
    stats = _statistics(panel) if inference else dict(paired_days=len(paired), original_mature_days=len(panel),
        mean_increment_bps=float(np.mean([v for v in panel if v is not None])) if paired else None,
        confidence_interval_95_bps=None, mde_80_bps=None, inference_status="DESCRIPTIVE_STRATUM_NOT_CONTIGUOUS_SESSIONS")
    increment = stats["mean_increment_bps"]
    if increment is not None and not math.isclose(increment, parts["known_action_contribution_bps"]+
                                                parts["unknown_cash_contribution_bps"], rel_tol=1e-9, abs_tol=1e-8):
        raise ValueError("population paired attribution does not reconcile")
    return dict(opponent=opponent, **stats, attribution=parts,
        all_known_action_changes=len(all_changes), all_action_change_days=len(all_days),
        settled_known_interventions=known_count, settled_intervention_days=len(intervention_days),
        settled_intervention_day_fraction=len(intervention_days)/len(dates) if dates else None,
        exploratory_support_hint=known_count >= 20 and len(intervention_days) >= 12
            and bool(dates) and len(intervention_days)/len(dates) >= .1,
        support_hint_is_confirmation_gate=False)


def _calibration(observations, arm):
    n, bins = len(observations), []
    for i in range(5):
        selected = [r for r in observations if i/5 <= r["predictions"][arm]["profit_probability"] <= 1
                    and (r["predictions"][arm]["profit_probability"] < (i+1)/5 or i == 4)]
        bins.append(dict(low=i/5, high=(i+1)/5, n=len(selected),
            realized_profit_rate=sum(r["actual_net_bps"] > 0 for r in selected)/len(selected) if selected else None))
    return dict(n=n, fitting_performed=False, fixed_probability_bins=bins,
        mean_net_error_bps=float(np.mean([r["predictions"][arm]["expected_net_bps"]-r["actual_net_bps"] for r in observations])) if n else None,
        brier=float(np.mean([(r["predictions"][arm]["profit_probability"]-int(r["actual_net_bps"] > 0))**2 for r in observations])) if n else None,
        path_below_q10_fraction=sum(r["path_min_ratio"] <= r["predictions"][arm]["path_min_ratio_q10"] for r in observations)/n if n else None)


def _summary(groups, episodes, source, *, inference=True):
    selected = [g for g in groups if source is None or g["source_id"] == source]
    items = [r for r in episodes if source is None or r["source_id"] == source]
    by_day = {}
    for group in selected:
        by_day.setdefault(group["decision_date"], []).append(group)
    day_returns = {d: {a: None if any(g["cohort_net_bps"][a] is None for g in cells) else
                       float(np.mean([g["cohort_net_bps"][a] for g in cells])) for a in ARMS} for d, cells in by_day.items()}
    arms = {}
    for arm in ARMS:
        takes = [r["actual_net_bps"] for r in items if r["actions"][arm] == "TAKE" and r["actual_net_bps"] is not None]
        positive, negative = [v for v in takes if v > 0], [v for v in takes if v < 0]
        complete = [v[arm] for v in day_returns.values() if v[arm] is not None]
        arms[arm] = dict(complete_day_clusters=len(complete), cohort_mean_bps=float(np.mean(complete)) if complete else None,
            settled_take_count=len(takes), win_rate=len(positive)/len(takes) if takes else None,
            mean_win_bps=float(np.mean(positive)) if positive else None,
            mean_loss_bps=float(np.mean(negative)) if negative else None,
            settled_take_tail_q10_bps=float(np.quantile(takes, .1)) if takes else None,
            unknown_action_slots=sum(r["actions"][arm] == "UNKNOWN" for r in items),
            execution_unavailable_slots=sum(r["label_status"] == "ENTRY_NOT_EXECUTABLE" for r in items),
            unresolved_take_slots=sum(r["actions"][arm] == "TAKE" and r["actual_net_bps"] is None
                                      and r["label_status"] != "ENTRY_NOT_EXECUTABLE" for r in items))
    common = [v for v in day_returns.values() if all(v[a] is not None for a in ARMS)]
    calibration = {}
    for arm in STUDY_ARMS:
        observations = [r for r in items if r["actual_net_bps"] is not None and r["predictions"][arm]["status"] not in UNKNOWN]
        calibration[arm] = _calibration(observations, arm)
    return dict(original_days=len({g["decision_date"] for g in selected}), original_cohort_cells=len(selected), arms=arms,
        common_four_arm_day_clusters=len(common),
        common_four_arm_means_bps={a: float(np.mean([v[a] for v in common])) if common else None for a in ARMS},
        calibration_diagnostic_only=calibration,
        paired={op: _pairs(groups, episodes, op, source, inference=inference) for op in ("matched_anchor", "baseline", "rule_300bps")})


def evaluate_population_cohorts_v1(*, rows, clusters, days, predictions, calendar, evaluation_start, evaluation_end, anchor_source_id):
    calendar, start, end = [_day(d) for d in calendar], _day(evaluation_start), _day(evaluation_end)
    if not calendar or calendar != sorted(set(calendar)) or start > end or start not in calendar or end not in calendar:
        raise ValueError("population original evaluation calendar differs")
    rows, clusters, days, maps = _validated(rows, clusters, days, predictions, calendar, start, end)
    if anchor_source_id not in set(days.source_id):
        raise ValueError("population preregistered anchor is absent")
    groups, episodes = _groups(rows, days, maps, calendar, start, end)
    by_source = {s: _summary(groups, episodes, s) for s in sorted(set(days.source_id))}
    regime_summaries = {}
    for regime in ("POSITIVE", "NEGATIVE", "ZERO", "UNKNOWN"):
        stratum = [g for g in groups if g["lagged_csi300_regime"] == regime]
        membership = {(g["source_id"], g["decision_date"]) for g in stratum}
        items = [r for r in episodes if (r["source_id"], r["decision_date"]) in membership]
        regime_summaries[regime] = {s: _summary(stratum, items, s, inference=False) for s in sorted(set(days.source_id))}
    primary = by_source[anchor_source_id]["paired"]
    positive = all(p["mean_increment_bps"] is not None and p["mean_increment_bps"] > 0
                   and p["attribution"]["known_action_contribution_bps"] > 0 for p in (primary["matched_anchor"], primary["baseline"]))
    unique_nodes = []
    for r in rows.drop_duplicates(list(KEY)).to_dict("records"):
        if r["label_status"] == "AVAILABLE":
            key = tuple(r[n] for n in KEY)
            unique_nodes.append(dict(actual_net_bps=_net(_number(r["observed_gap_bps"]), _number(r["gross_terminal_ratio"])),
                                     path_min_ratio=_number(r["path_min_ratio"]), predictions={a: maps[a][key] for a in STUDY_ARMS}))
    quality = {a: _calibration([r for r in unique_nodes if r["predictions"][a]["status"] not in UNKNOWN], a) for a in STUDY_ARMS}
    return dict(status="EXPLORATORY_POSITIVE_NOT_CONFIRMED" if positive else "NEGATIVE_OR_INSUFFICIENT_STOP_THIS_CANDIDATE",
        anchor_source_id=anchor_source_id, original_rows=len(rows), unique_prediction_clusters=len(clusters),
        source_summaries=by_source, global_original_day_cluster=_summary(groups, episodes, None),
        unique_node_calibration_diagnostic_only=quality,
        lagged_regime_source_summaries=regime_summaries,
        prediction_status_counts={a: {s: sum(p["status"] == s for p in maps[a].values())
                                     for s in ("ACCEPTABLE", "AVOID", *sorted(UNKNOWN))} for a in STUDY_ARMS},
        cohorts=groups, original_top5_episodes=episodes, statistics=dict(STATISTICS),
        regime_label="D_VISIBLE_LAGGED_CSI300_5D_SIGN_NOT_HMM_OR_FULL_REGIME_COVERAGE",
        regime_counts={s: sum(g["lagged_csi300_regime"] == s for g in groups) for s in ("POSITIVE", "NEGATIVE", "ZERO", "UNKNOWN")},
        evidence_level="CONSUMED_DEVELOPMENT_EXPLORATORY_NON_VINTAGE_LEGACY", independent_oos_evidence=False,
        deployable=False, activation_evidence=False, represents_nav=False, annualized_return=None, max_drawdown=None)
