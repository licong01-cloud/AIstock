"""Original five-slot economic ledger with two matched endpoints; never NAV."""
import math

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import (
    ARMS, COHORT_ARMS, ROSTER_KEY, SCHEMA_SHA256, UNKNOWN,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import (
    EVAL_FINANCE, META, validate_finance_v1, validate_roster_v1,
)


def _maps(rows, predictions):
    if set(predictions) != set(ARMS):
        raise ValueError("risk evaluation requires exactly its two model arms")
    expected = rows.set_index(list(ROSTER_KEY))
    maps = {}
    for arm, frame in predictions.items():
        if (frame.duplicated(list(ROSTER_KEY)).any()
                or set(frame[list(ROSTER_KEY)].itertuples(index=False, name=None)) != set(expected.index)
                or not frame.schema_sha256.eq(SCHEMA_SHA256).all() or not frame.policy_sha256.eq(POLICY_SHA256).all()
                or not frame.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
                or not frame.arm.eq(arm).all() or not frame.status.isin(["ACCEPTABLE", "AVOID", *UNKNOWN]).all()):
            raise ValueError("risk prediction original identity/policy/status differs")
        known = frame.status.isin(["ACCEPTABLE", "AVOID"])
        for name in ("model_sha256", "base_model_sha256", "calibration_sha256", "bundle_sha256"):
            if frame[name].nunique() != 1 or not frame[name].str.fullmatch(r"[a-f0-9]{64}").all():
                raise ValueError("risk prediction mixes model identities")
        unknown = ~known & ~frame.status.eq("UNKNOWN_RISK_CALIBRATION")
        if (frame.loc[unknown, ["expected_net_bps", "profit_probability", "downside_q90_bps"]].notna().any().any()
                or frame.loc[frame.status.eq("UNKNOWN_RISK_CALIBRATION"), "downside_q90_bps"].notna().any()):
            raise ValueError("unknown risk predictions cannot invent values")
        if arm == ARMS[0]:
            if frame.status.eq("UNKNOWN_RISK_CALIBRATION").any() or not frame.loc[known, "downside_q90_bps"].eq(
                    frame.loc[known, "uncalibrated_downside_q90_bps"]).all():
                raise ValueError("raw control cannot consume calibrated risk")
        elif known.any():
            if (not frame.loc[known, "risk_calibration_status"].eq("AVAILABLE").all()
                    or frame.risk_calibration_delta.nunique(dropna=False) != 1
                    or not np.allclose(frame.loc[known, "downside_q90_bps"], np.clip(
                        frame.loc[known, "uncalibrated_downside_q90_bps"]+frame.loc[known, "risk_calibration_delta"], 0, 10000))):
                raise ValueError("candidate changed its fixed residual calibration equation")
        values = frame.loc[known, ["expected_net_bps", "profit_probability", "downside_q90_bps"]].map(_number)
        if (values.isna().any().any() or not values.profit_probability.between(0, 1).all()
                or not values.downside_q90_bps.between(0, 10000).all()
                or not frame.loc[known, "status"].eq("ACCEPTABLE").eq(
                    values.expected_net_bps.gt(0) & values.downside_q90_bps.le(POLICY["risk_bps"])).all()):
            raise ValueError("risk prediction action/value coordinates differ")
        maps[arm] = frame.set_index(list(ROSTER_KEY)).to_dict("index")
    for key in expected.index:
        a, b = (maps[arm][key] for arm in ARMS)
        for name in ("expected_net_bps", "profit_probability", "uncalibrated_downside_q90_bps", "base_model_sha256", "bundle_sha256"):
            x, y = a[name], b[name]
            if not (x == y or pd.isna(x) and pd.isna(y)):
                raise ValueError("risk model arms changed their shared core/value head")
    return maps


def _episode(row, maps, mature):
    key = tuple(row[n] for n in ROSTER_KEY)
    actual, mark, gap = (_number(row[n]) for n in ("hypothetical_liquidation_net_bps", "mark_to_market_net_bps", "observed_gap_bps"))
    actions, contributions, marks = {}, {}, {}
    not_executable = row["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE"
    for arm in COHORT_ARMS:
        if not_executable:
            action = "NOT_EXECUTABLE"
        elif arm == "baseline":
            action = "TAKE"
        elif arm == "rule_300bps":
            action = "UNKNOWN" if gap is None else "TAKE" if abs(gap) <= 300 else "SKIP"
        else:
            state = maps[arm][key]["status"]
            action = "TAKE" if state == "ACCEPTABLE" else "SKIP" if state == "AVOID" else "UNKNOWN"
        actions[arm] = action
        # Unknown outcomes do not become a known zero merely because a model abstains.
        contributions[arm] = None if not mature or actual is None else actual if action == "TAKE" else 0.
        marks[arm] = None if not mature or mark is None else mark if action == "TAKE" else 0.
        if mature and not_executable:
            contributions[arm] = marks[arm] = 0.
    return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(), instrument=row["instrument"],
        rank=int(row["selection_effective_rank"]), actual_net_bps=actual, actions=actions, contributions_bps=contributions,
        mark_contributions_bps=marks, valuation_status=row["valuation_status"], exit_execution_status=row["exit_execution_status"],
        actual_fill_proven=False, realized_return_bps=None)


def _summary(groups, episodes, package_count, inference):
    dates = sorted({g["decision_date"] for g in groups if g["horizon_mature"]})
    aggregates = {}
    for d in dates:
        cells = [g for g in groups if g["decision_date"] == d]
        aggregates[d] = {a: None if len(cells) != package_count or any(g["cohort_net_bps"][a] is None for g in cells)
            else float(np.mean([g["cohort_net_bps"][a] for g in cells])) for a in COHORT_ARMS}
    panels = {opponent: [None if any(aggregates[d][a] is None for a in (ARMS[1], opponent))
        else aggregates[d][ARMS[1]]-aggregates[d][opponent] for d in dates] for opponent in ("baseline", ARMS[0])}
    statistics = paired_statistics(panels)
    pairs = {}
    for opponent, panel in panels.items():
        paired = {d for d, v in zip(dates, panel, strict=True) if v is not None}
        parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
            known_action_contribution_bps=0., unknown_cash_contribution_bps=0.)
        changed, changed_days, clusters = 0, set(), set()
        for item in episodes:
            if item["decision_date"] not in paired:
                continue
            a, b = item["actions"][ARMS[1]], item["actions"][opponent]
            scale = 1/(5*package_count*len(paired))
            delta = (item["contributions_bps"][ARMS[1]]-item["contributions_bps"][opponent])*scale
            name = "unknown_cash_contribution_bps" if "UNKNOWN" in (a, b) else "known_action_contribution_bps"
            parts[name] += delta
            if name.startswith("known") and a != b and item["actual_net_bps"] is not None and "NOT_EXECUTABLE" not in (a, b):
                changed += 1
                changed_days.add(item["decision_date"])
                clusters.add((item["decision_date"], item["instrument"]))
                actual = item["actual_net_bps"]
                cell = ("avoided_loss_bps" if actual < 0 else "missed_profit_bps") if a == "SKIP" else (
                    "new_take_profit_bps" if actual >= 0 else "new_take_loss_bps")
                parts[cell] += abs(actual)*scale
        stats = statistics[opponent]
        if not inference:
            stats.update(confidence_interval_95_diagnostic_bps=None, simultaneous_95_bonferroni_bps=None,
                mde_80_bps=None, inference_status="DESCRIPTIVE_PACKAGE_STRATUM_ONLY")
        mean = stats["mean_increment_bps"]
        if mean is not None and not math.isclose(mean, parts["known_action_contribution_bps"]+parts["unknown_cash_contribution_bps"], abs_tol=1e-8):
            raise ValueError("risk economic attribution does not reconcile")
        pairs[opponent] = dict(**stats, attribution=parts, settled_known_interventions=changed,
            settled_intervention_days=len(changed_days), settled_intervention_unique_stock_days=len(clusters),
            intervention_day_fraction=len(changed_days)/len(dates) if dates else None)
    common = [v for v in aggregates.values() if all(x is not None for x in v.values())]
    means, takes = {}, {}
    for arm in COHORT_ARMS:
        known = [v[arm] for v in aggregates.values() if v[arm] is not None]
        means[arm] = dict(known_days=len(known), mean_bps=float(np.mean(known)) if known else None,
            common_four_arm_days=len(common), common_four_arm_mean_bps=float(np.mean([v[arm] for v in common])) if common else None)
        profits = [e["actual_net_bps"] for e in episodes if e["decision_date"] in dates
            and e["actions"][arm] == "TAKE" and e["actual_net_bps"] is not None]
        wins, losses = [v for v in profits if v > 0], [v for v in profits if v < 0]
        takes[arm] = dict(valued_take_episodes=len(profits), hit_rate=sum(v > 0 for v in profits)/len(profits) if profits else None,
            mean_winner_bps=float(np.mean(wins)) if wins else None, mean_loser_bps=float(np.mean(losses)) if losses else None,
            q10_net_bps=float(np.quantile(profits, .1)) if profits else None,
            known_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] in ("SKIP", "NOT_EXECUTABLE") for e in episodes),
            unknown_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] == "UNKNOWN" for e in episodes))
    mark_means = {}
    for arm in COHORT_ARMS:
        values = [sum(e["mark_contributions_bps"][arm] for e in episodes
            if e["decision_date"] == d)/(5*package_count) for d in dates
            if all(e["mark_contributions_bps"][arm] is not None for e in episodes if e["decision_date"] == d)
            and all(g["source_status"] == "KNOWN_ROSTER" for g in groups if g["decision_date"] == d)]
        mark_means[arm] = dict(known_days=len(values), mean_bps=float(np.mean(values)) if values else None,
            actual_fill_proven=False)
    return dict(original_mature_days=len(dates), pairs=pairs, cohort_means=means, take_statistics=takes,
        mark_to_market_diagnostic=mark_means,
        equal_package_weight=1/package_count, overlapping_five_session_cohort_not_nav=True)


def evaluate_risk_tail_cohorts_v1(*, rows, days, predictions, original_roster, plan, calendar):
    validate_roster_v1(original_roster, days, plan, calendar)
    mask = rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
    rows = rows.loc[mask].copy()
    days = days.loc[days.decision_date.between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    original = original_roster.loc[original_roster[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))]
    if (rows.duplicated(list(ROSTER_KEY)).any() or set(rows[list(ROSTER_KEY)].itertuples(index=False, name=None))
            != set(original[list(ROSTER_KEY)].itertuples(index=False, name=None))):
        raise ValueError("risk evaluation removed/changed an original row")
    if not rows[list(META)].set_index(list(ROSTER_KEY)).sort_index().equals(
            original[list(META)].set_index(list(ROSTER_KEY)).sort_index()):
        raise ValueError("risk evaluation changed original ranks/slot population")
    validate_finance_v1(rows, calendar, EVAL_FINANCE)
    maps = _maps(rows, predictions)
    for row in rows.loc[rows.valuation_status.eq("AVAILABLE")].to_dict("records"):
        g, y, actual, mark = (_number(row[n]) for n in ("observed_gap_bps", "valuation_gross_terminal_ratio",
            "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"))
        raw = y/((1+g/10000)*(1+POLICY["buy_bps"]/10000))
        if actual is None or mark is None or not math.isclose(actual, 10000*(raw*(1-POLICY["sell_bps"]/10000)-1), abs_tol=1e-6) or not math.isclose(mark, 10000*(raw-1), abs_tol=1e-6):
            raise ValueError("risk valuation costs differ from the original policy")
    groups, episodes = [], []
    for day in days.to_dict("records"):
        selected = rows.loc[rows.package_id.eq(day["package_id"]) & rows[KEY[0]].eq(day["decision_date"])
            & rows.selection_effective_rank.le(5)]
        mature = pd.notna(day["horizon_end"]) and day["horizon_end"] <= pd.Timestamp(plan.evaluation_end)
        records = [_episode(row, maps, mature) for row in selected.to_dict("records")]
        episodes.extend(records)
        known_roster = day["source_status"] == "KNOWN_ROSTER"
        values = {a: None if not mature or not known_roster or any(e["contributions_bps"][a] is None for e in records)
            else sum(e["contributions_bps"][a] for e in records)/5 for a in COHORT_ARMS}
        groups.append(dict(package_id=day["package_id"], decision_date=day["decision_date"].date().isoformat(),
            horizon_mature=bool(mature), source_status=day["source_status"], original_candidates=day["original_candidates"],
            original_slots=5, structural_empty_slots=5-len(records) if known_roster else None, cohort_net_bps=values))
    primary = _summary(groups, episodes, 2, True)
    package_results = {s.package_id: _summary([g for g in groups if g["package_id"] == s.package_id],
        [e for e in episodes if e["package_id"] == s.package_id], 1, False) for s in plan.sources}
    risk_diagnostics = {}
    for arm in ARMS:
        observations, probabilities = [], []
        for row in rows.loc[rows.selection_effective_rank.le(5) & rows.valuation_status.eq("AVAILABLE")
                & rows.label_information_end.le(pd.Timestamp(plan.evaluation_end))].to_dict("records"):
            forecast = maps[arm][tuple(row[n] for n in ROSTER_KEY)]
            if forecast["status"] not in ("ACCEPTABLE", "AVOID"):
                continue
            actual_b = 10000*max(0., 1-row["valuation_path_min_ratio"]/(1+row["observed_gap_bps"]/10000))
            observations.append(actual_b > forecast["downside_q90_bps"])
            probabilities.append((forecast["profit_probability"], row["hypothetical_liquidation_net_bps"] > 0))
        risk_diagnostics[arm] = dict(known_original_top5=len(observations),
            actual_exceeds_predicted_q90_fraction=float(np.mean(observations)) if observations else None,
            brier=float(np.mean([(p-float(y))**2 for p, y in probabilities])) if probabilities else None,
            conditional_coverage_guaranteed=False, economic_slot_weights_not_calibration_cluster_weights=True)
    continuing = all(p["mean_increment_bps"] is not None and p["mean_increment_bps"] > 0
        and p["attribution"]["known_action_contribution_bps"] > 0 and p["settled_known_interventions"] > 0
        for p in primary["pairs"].values())
    return dict(status="PROMISING_NAVIGATION_NOT_CONFIRMED" if continuing else "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE",
        hypothesis="GP5-RISK-TAIL-CALIBRATION-1", primary=primary, packages=package_results,
        risk_diagnostics=risk_diagnostics, original_rows=len(rows), original_package_days=len(days),
        original_groups=groups, original_episodes=episodes, original_top5_slots=sum(min(5, int(v)) for v in days.original_candidates),
        fixed_slot_budget=5*len(days),
        prediction_status_counts={a: {str(k): int(v) for k, v in f.status.value_counts().items()} for a, f in predictions.items()},
        exit_execution_counts={str(k): int(v) for k, v in rows.exit_execution_status.value_counts().items()},
        evidence_use=plan.decision_use, objective_contract=plan.objective_contract, study_type=plan.study_type,
        regime_support="UNKNOWN", independent_oos_evidence=False, activation_evidence=False,
        actual_fill_proven=False, realized_return_bps=None, dataset_vintage="CURRENT_DATABASE_NON_VINTAGE",
        nav_or_annualized_return_claimed=False, automatic_followup_trial=False, deployable=False)
