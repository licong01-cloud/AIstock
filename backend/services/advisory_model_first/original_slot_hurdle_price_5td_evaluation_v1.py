"""Four original-slot ledgers; net value, utility, unknowns and support separate."""
import math

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import (
    ARMS, COHORT_ARMS, COMPARISON_MAPPING_SHA256, HYPOTHESIS, INTERVENTION, KEY, POLICY_SHA256,
    ROSTER_KEY, SCHEMA_SHA256, UNKNOWN, VALUATION_POLICY_SHA256, VALUE_FIELDS,
)
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_inputs_v1 import (
    META, validate_finance_v1, validate_roster_v1,
)
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics


def utility_v1(r):
    return r-max(-r-800., 0.)


def _maps(rows, predictions):
    expected = set(rows[list(ROSTER_KEY)].itertuples(index=False, name=None))
    if set(predictions) != set(ARMS):
        raise ValueError("hurdle evaluation needs both preregistered model arms")
    maps, bundle_ids = {}, set()
    for arm, frame in predictions.items():
        if (frame.duplicated(list(ROSTER_KEY)).any()
                or set(frame[list(ROSTER_KEY)].itertuples(index=False, name=None)) != expected
                or not frame.arm.eq(arm).all() or not frame.schema_sha256.eq(SCHEMA_SHA256).all()
                or not frame.policy_sha256.eq(POLICY_SHA256).all()
                or not frame.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
                or not frame.status.isin(["ACCEPTABLE_VALUE_PREDICTED", "AVOID", *UNKNOWN]).all()
                or bool(expected) and frame.bundle_sha256.nunique() != 1
                or not frame.bundle_sha256.str.fullmatch(r"[a-f0-9]{64}").all()
                or not frame.probabilities_calibrated.eq(False).all() or not frame.actual_fill_proven.eq(False).all()):
            raise ValueError("hurdle prediction original identity/policy/status differs")
        bundle_ids.update(frame.bundle_sha256)
        known = frame.status.isin(["ACCEPTABLE_VALUE_PREDICTED", "AVOID"])
        if frame.loc[~known, list(VALUE_FIELDS)].notna().any().any():
            raise ValueError("UNKNOWN forecasts cannot invent probabilities or values")
        v = frame.loc[known, list(VALUE_FIELDS)].map(_number)
        if (v.isna().any().any() or not v[["p_positive", "p_negative", "p_zero"]].ge(0).all().all()
                or not v[["p_positive", "p_negative", "p_zero"]].le(1).all().all()
                or not np.allclose(v.p_positive+v.p_negative+v.p_zero, 1, atol=1e-12, rtol=0)
                or not v.positive_mean_bps.gt(0).all() or not v.negative_mean_bps.between(0, 10000, inclusive="neither").all()
                or not v.probability_terminal_loss_gt_800.ge(0).all()
                or not v.probability_terminal_loss_gt_800.le(v.p_negative+1e-12).all()
                or not v.expected_terminal_excess_loss_bps.ge(0).all()
                or not v.expected_terminal_excess_loss_bps.le(v.p_negative*v.negative_mean_bps+1e-9).all()
                or not np.allclose(v.expected_net_bps, v.p_positive*v.positive_mean_bps-v.p_negative*v.negative_mean_bps, atol=1e-8, rtol=0)
                or not np.allclose(v.expected_utility_bps, v.expected_net_bps-v.expected_terminal_excess_loss_bps, atol=1e-8, rtol=0)
                or not frame.loc[known, "status"].eq("ACCEPTABLE_VALUE_PREDICTED").eq(v.expected_utility_bps.gt(0)).all()):
            raise ValueError("hurdle forecast probability/value/action coordinates differ")
        maps[arm] = frame.set_index(list(ROSTER_KEY)).to_dict("index")
    if len(bundle_ids) != (1 if expected else 0):
        raise ValueError("hurdle arms belong to different bundles")
    return maps


def _episode(row, maps, mature):
    actual, mark, gap = (_number(row[n]) for n in ("hypothetical_liquidation_net_bps", "mark_to_market_net_bps", "observed_gap_bps"))
    key = tuple(row[n] for n in ROSTER_KEY)
    known_valuation = row["valuation_status"] == "AVAILABLE"
    not_executable = row["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE"
    actions, net, utilities, marks, tails = {}, {}, {}, {}, {}
    for arm in COHORT_ARMS:
        if not_executable:
            action = "NOT_EXECUTABLE"
        elif arm == "baseline":
            action = "TAKE"
        elif arm == "rule_300bps":
            action = "UNKNOWN" if gap is None else "TAKE" if abs(gap) <= 300 else "SKIP"
        else:
            status = maps[arm][key]["status"]
            action = "TAKE" if status == "ACCEPTABLE_VALUE_PREDICTED" else "SKIP" if status == "AVOID" else "UNKNOWN"
        actions[arm] = action
        if not mature or not known_valuation or actual is None or mark is None:
            net[arm] = utilities[arm] = marks[arm] = tails[arm] = None
        else:
            net[arm] = actual if action == "TAKE" else 0.
            utilities[arm] = utility_v1(actual) if action == "TAKE" else 0.
            tails[arm] = max(-actual-800, 0.) if action == "TAKE" else 0.
            marks[arm] = mark if action == "TAKE" else 0.
        if mature and not_executable:
            net[arm] = utilities[arm] = marks[arm] = tails[arm] = 0.
    regime = _number(row["csi300_ret_5"])
    path = _number(row["valuation_path_min_ratio"])
    adverse = 10000*max(0., 1-path/(1+gap/10000)) if known_valuation and path is not None and gap is not None else None
    return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(), instrument=row["instrument"],
        rank=int(row["selection_effective_rank"]), actual_net_bps=actual if known_valuation else None,
        actions=actions, contributions_bps=net, utility_contributions_bps=utilities, tail_contributions_bps=tails,
        mark_contributions_bps=marks, path_adverse_bps=adverse,
        valuation_status=row["valuation_status"], exit_execution_status=row["exit_execution_status"],
        regime="UNKNOWN" if regime is None else "POSITIVE" if regime > 0 else "NONPOSITIVE",
        actual_fill_proven=False, realized_return_bps=None)


def _aggregate(groups, package_count, dates, field):
    out = {}
    for d in dates:
        cells = [g for g in groups if g["decision_date"] == d]
        out[d] = {a: None if len(cells) != package_count or any(g[field][a] is None for g in cells)
            else float(np.mean([g[field][a] for g in cells])) for a in COHORT_ARMS}
    return out


def _summary(groups, episodes, package_count, *, inference):
    dates = sorted({g["decision_date"] for g in groups if g["horizon_mature"]})
    aggregate = _aggregate(groups, package_count, dates, "cohort_net_bps")
    utilities = _aggregate(groups, package_count, dates, "cohort_utility_bps")
    tails = _aggregate(groups, package_count, dates, "cohort_tail_bps")
    panels = {opponent: [None if any(aggregate[d][a] is None for a in (ARMS[1], opponent))
        else aggregate[d][ARMS[1]]-aggregate[d][opponent] for d in dates] for opponent in ("baseline", ARMS[0])}
    # Deliberate internal alias only: old helper is unchanged and not the evaluator.
    stats = paired_statistics({"baseline": panels["baseline"], "matched_core": panels[ARMS[0]]})
    stats[ARMS[0]] = stats.pop("matched_core")
    pairs = {}
    for opponent, panel in panels.items():
        paired = {d for d, value in zip(dates, panel, strict=True) if value is not None}
        parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
            known_action_contribution_bps=0., unknown_cash_contribution_bps=0.)
        changed_days, clusters = set(), set()
        regimes = {n: set() for n in ("POSITIVE", "NONPOSITIVE", "UNKNOWN")}
        for item in episodes:
            if item["decision_date"] not in paired:
                continue
            a, b = item["actions"][ARMS[1]], item["actions"][opponent]
            scale = 1/(5*package_count*len(paired))
            delta = (item["contributions_bps"][ARMS[1]]-item["contributions_bps"][opponent])*scale
            category = "unknown_cash_contribution_bps" if "UNKNOWN" in (a, b) else "known_action_contribution_bps"
            parts[category] += delta
            if category.startswith("known") and a != b and item["actual_net_bps"] is not None and "NOT_EXECUTABLE" not in (a, b):
                changed_days.add(item["decision_date"])
                cluster = (item["decision_date"], item["instrument"])
                clusters.add(cluster)
                regimes[item["regime"]].add(cluster)
                r = item["actual_net_bps"]
                name = ("avoided_loss_bps" if r < 0 else "missed_profit_bps") if a == "SKIP" else (
                    "new_take_profit_bps" if r >= 0 else "new_take_loss_bps")
                parts[name] += abs(r)*scale
        s = stats[opponent]
        if not inference:
            s.update(confidence_interval_95_diagnostic_bps=None, simultaneous_95_bonferroni_bps=None,
                mde_80_bps=None, inference_status="DESCRIPTIVE_PACKAGE_STRATUM_ONLY")
        if s["mean_increment_bps"] is not None and not math.isclose(s["mean_increment_bps"],
                parts["known_action_contribution_bps"]+parts["unknown_cash_contribution_bps"], abs_tol=1e-8):
            raise ValueError("hurdle net attribution does not reconcile")
        decomposition = parts["avoided_loss_bps"]-parts["missed_profit_bps"]+parts["new_take_profit_bps"]-parts["new_take_loss_bps"]
        if not math.isclose(decomposition, parts["known_action_contribution_bps"], abs_tol=1e-8):
            raise ValueError("hurdle known intervention ledger does not reconcile")
        u = [utilities[d][ARMS[1]]-utilities[d][opponent] for d in paired]
        fraction = len(changed_days)/len(dates) if dates else 0.
        support = (len(clusters) >= INTERVENTION["minimum_stock_days"] and len(changed_days) >= INTERVENTION["minimum_days"]
            and fraction >= INTERVENTION["minimum_day_fraction"])
        pairs[opponent] = dict(**s, utility_increment_bps=float(np.mean(u)) if u else None, attribution=parts,
            settled_intervention_unique_stock_days=len(clusters), settled_intervention_days=len(changed_days),
            intervention_day_fraction=fraction, intervention_support_met=support,
            regime_intervention_unique_stock_days={n: len(v) for n, v in regimes.items()})
    common = [v for v in aggregate.values() if all(x is not None for x in v.values())]
    means, take_stats, risk, marks = {}, {}, {}, {}
    for arm in COHORT_ARMS:
        valued = [v[arm] for v in aggregate.values() if v[arm] is not None]
        profits = [e["actual_net_bps"] for e in episodes if e["decision_date"] in dates and e["actions"][arm] == "TAKE"
            and e["actual_net_bps"] is not None]
        gains, losses = [v for v in profits if v > 0], [v for v in profits if v < 0]
        means[arm] = dict(known_days=len(valued), mean_bps=float(np.mean(valued)) if valued else None,
            common_four_arm_days=len(common), common_four_arm_mean_bps=float(np.mean([v[arm] for v in common])) if common else None)
        take_stats[arm] = dict(valued_take_episodes=len(profits), hit_rate=float(np.mean(np.asarray(profits) > 0)) if profits else None,
            mean_winner_bps=float(np.mean(gains)) if gains else None, mean_loser_bps=float(np.mean(losses)) if losses else None,
            take_slot_coverage=len(profits)/(5*package_count*len(dates)) if dates else None,
            unknown_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] == "UNKNOWN" for e in episodes),
            known_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] in ("SKIP", "NOT_EXECUTABLE") for e in episodes))
        risk[arm] = {}
        for name, data in (("utility_mean_bps", utilities), ("terminal_excess_loss_mean_bps", tails)):
            values = [v[arm] for v in data.values() if v[arm] is not None]
            risk[arm][name] = float(np.mean(values)) if values else None
        paths = [e["path_adverse_bps"] for e in episodes if e["decision_date"] in dates
            and e["actions"][arm] == "TAKE" and e["path_adverse_bps"] is not None]
        risk[arm].update(known_take_path_count=len(paths), take_path_adverse_mean_bps=float(np.mean(paths)) if paths else None,
            take_path_adverse_q90_bps=float(np.quantile(paths, .9)) if paths else None, path_protection_claimed=False)
        mark_values = []
        for d in dates:
            cells = [g for g in groups if g["decision_date"] == d]
            items = [e for e in episodes if e["decision_date"] == d]
            if len(cells) == package_count and all(g["source_status"] == "KNOWN_ROSTER" for g in cells) and all(
                    e["mark_contributions_bps"][arm] is not None for e in items):
                mark_values.append(sum(e["mark_contributions_bps"][arm] for e in items)/(5*package_count))
        marks[arm] = dict(known_days=len(mark_values), mean_bps=float(np.mean(mark_values)) if mark_values else None,
            exit_execution_proven=False, actual_fill_proven=False)
    return dict(original_mature_days=len(dates), pairs=pairs, cohort_means=means, take_statistics=take_stats,
        utility_and_terminal_risk=risk, mark_to_market_diagnostic=marks, equal_package_weight=1/package_count,
        comparison_mapping_sha256=COMPARISON_MAPPING_SHA256, overlapping_five_session_cohort_not_nav=True)


def _forecast_diagnostics(rows, maps):
    result = {}
    for arm in ARMS:
        records = []
        for row in rows.loc[rows.valuation_status.eq("AVAILABLE")].to_dict("records"):
            f = maps[arm][tuple(row[n] for n in ROSTER_KEY)]
            if f["status"] not in ("ACCEPTABLE_VALUE_PREDICTED", "AVOID"):
                continue
            r = row["hypothetical_liquidation_net_bps"]
            path = 10000*max(0., 1-row["valuation_path_min_ratio"]/(1+row["observed_gap_bps"]/10000))
            records.append(dict(r=r, forecast=f, path_adverse_bps=path))
        metrics = dict(known_original_top5=len(records), probabilities_calibrated=False, path_protection_claimed=False)
        terms = dict(sign_positive_brier=[(v["forecast"]["p_positive"]-float(v["r"] > 0))**2 for v in records],
            sign_multiclass_brier=[sum((v["forecast"][n]-float(np.sign(v["r"]) == c))**2 for n, c in (
                ("p_positive", 1), ("p_negative", -1), ("p_zero", 0))) for v in records],
            terminal_deep_loss_brier=[(v["forecast"]["probability_terminal_loss_gt_800"]-float(v["r"] < -800))**2 for v in records],
            positive_magnitude_mae_bps=[abs(v["forecast"]["positive_mean_bps"]-v["r"]) for v in records if v["r"] > 0],
            negative_magnitude_mae_bps=[abs(v["forecast"]["negative_mean_bps"]+v["r"]) for v in records if v["r"] < 0],
            expected_net_mae_bps=[abs(v["forecast"]["expected_net_bps"]-v["r"]) for v in records],
            terminal_excess_loss_mae_bps=[abs(v["forecast"]["expected_terminal_excess_loss_bps"]-max(-v["r"]-800, 0)) for v in records],
            path_adverse_mean_bps=[v["path_adverse_bps"] for v in records])
        metrics.update({n: float(np.mean(v)) if v else None for n, v in terms.items()})
        result[arm] = metrics
    return result


def evaluate_hurdle_cohorts_v1(*, rows, days, predictions, original_roster, plan, calendar):
    validate_roster_v1(original_roster, days, plan, calendar)
    dates = pd.to_datetime(calendar)
    dates = dates[(dates >= pd.Timestamp(plan.evaluation_start)) & (dates <= pd.Timestamp(plan.evaluation_end))]
    original = original_roster.loc[original_roster[KEY[0]].isin(dates) & original_roster.selection_effective_rank.le(5)]
    if (len(rows) > 3050 or rows.duplicated(list(ROSTER_KEY)).any()
            or not rows[list(META)].set_index(list(ROSTER_KEY)).sort_index().equals(
                original[list(META)].set_index(list(ROSTER_KEY)).sort_index())):
        raise ValueError("hurdle evaluation changed the original Top5 slots/ranks/keys")
    validate_finance_v1(rows, calendar, evaluation=True)
    # IMMATURE is retained but cannot contain a decoded future outcome.
    immature = rows.label_information_end.gt(pd.Timestamp(plan.evaluation_end))
    if (not rows.loc[immature, "valuation_status"].eq("IMMATURE").all()
            or rows.loc[immature, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio",
                "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]].notna().any().any()):
        raise ValueError("IMMATURE original slots cannot consume future outcomes")
    maps = _maps(rows, predictions)
    groups, episodes = [], []
    for day in days.loc[days.decision_date.isin(dates)].to_dict("records"):
        selected = rows.loc[rows.package_id.eq(day["package_id"]) & rows[KEY[0]].eq(day["decision_date"])]
        mature = pd.notna(day["horizon_end"]) and day["horizon_end"] <= pd.Timestamp(plan.evaluation_end)
        records = [_episode(r, maps, mature) for r in selected.to_dict("records")]
        episodes.extend(records)
        values = {}
        for output, field in (("cohort_net_bps", "contributions_bps"), ("cohort_utility_bps", "utility_contributions_bps"),
                ("cohort_tail_bps", "tail_contributions_bps")):
            values[output] = {a: None if not mature or day["source_status"] != "KNOWN_ROSTER"
                or any(e[field][a] is None for e in records) else sum(e[field][a] for e in records)/5 for a in COHORT_ARMS}
        groups.append(dict(package_id=day["package_id"], decision_date=day["decision_date"].date().isoformat(),
            horizon_mature=bool(mature), source_status=day["source_status"], original_candidates=day["original_candidates"],
            original_slots=5, structural_empty_slots=5-len(records) if day["source_status"] == "KNOWN_ROSTER" else None, **values))
    primary = _summary(groups, episodes, 2, inference=True)
    packages = {s.package_id: _summary([g for g in groups if g["package_id"] == s.package_id],
        [e for e in episodes if e["package_id"] == s.package_id], 1, inference=False) for s in plan.sources}
    pairs = list(primary["pairs"].values())
    positive = all(p["mean_increment_bps"] is not None and p["mean_increment_bps"] > 0
        and p["utility_increment_bps"] is not None and p["utility_increment_bps"] >= 0 for p in pairs)
    supported = all(p["intervention_support_met"] and p["paired_days"] == p["original_mature_days"] for p in pairs)
    status = ("EXPLORATORY_WORTH_CONFIRMING" if positive and supported else "EXPLORATORY_INSUFFICIENT_SUPPORT" if positive
        else "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE")
    return dict(status=status, hypothesis=HYPOTHESIS, primary=primary, packages=packages,
        forecast_diagnostics=_forecast_diagnostics(rows.loc[~immature], maps), original_groups=groups, original_episodes=episodes,
        original_top50_rows=len(original_roster.loc[original_roster[KEY[0]].isin(dates)]), original_top5_rows=len(rows),
        original_package_days=len(groups), fixed_slot_budget=5*len(groups),
        prediction_status_counts={a: {str(k): int(v) for k, v in f.status.value_counts().items()} for a, f in predictions.items()},
        exit_execution_counts={str(k): int(v) for k, v in rows.exit_execution_status.value_counts().items()},
        policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        evidence_use=plan.decision_use, study_type=plan.study_type, objective_contract=plan.objective_contract,
        dataset_vintage="CURRENT_DATABASE_NON_VINTAGE", historical_universe_completeness="UNKNOWN",
        outputs_seen_before_design=True, independent_oos_evidence=False, activation_evidence=False,
        actual_fill_proven=False, realized_return_bps=None, nav_or_annualized_return_claimed=False,
        automatic_followup_trial=False, deployable=False)
