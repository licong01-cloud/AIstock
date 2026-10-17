"""Four original-five-slot cohorts; equal-package time evidence, never NAV or fills."""
import math

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ARMS, SCHEMA_SHA256, STATISTICS

COHORT_ARMS = (*ARMS, "baseline", "rule_300bps")
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_PACKAGE_ADAPTER", "UNKNOWN_PARENT_SCORE",
           "UNKNOWN_PRICE_SCENARIO", "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION"}


def _finite(value):
    return _number(value)


def paired_statistics(panels):
    """Same original D block draws for both endpoints; holes are never compressed."""
    if set(panels) != {"baseline", "matched_core"} or len({len(v) for v in panels.values()}) != 1:
        raise ValueError("the two preregistered original-D endpoints differ")
    result = {}
    for name, panel in panels.items():
        known = [v for v in panel if v is not None]
        result[name] = dict(original_mature_days=len(panel), paired_days=len(known),
            mean_increment_bps=float(np.mean(known)) if known else None,
            confidence_interval_95_diagnostic_bps=None, simultaneous_95_bonferroni_bps=None,
            mde_80_bps=None, endpoint_alpha=.025, family_alpha=.05,
            inference_status="UNAVAILABLE_ORIGINAL_PANEL_HOLE_OR_TOO_SHORT")
    if any(any(v is None for v in p) or len(p) < 10 for p in panels.values()):
        return result
    n, block = len(next(iter(panels.values()))), STATISTICS["block_sessions"]
    rng = np.random.default_rng(STATISTICS["bootstrap_seed"])
    starts = rng.integers(0, n-block+1, size=(STATISTICS["bootstrap_count"], math.ceil(n/block)))
    positions = (starts[:, :, None]+np.arange(block)).reshape(len(starts), -1)[:, :n]
    for name, panel in panels.items():
        means = np.asarray(panel, dtype=float)[positions].mean(axis=1)
        result[name].update(confidence_interval_95_diagnostic_bps=np.quantile(means, [.025, .975]).tolist(),
            simultaneous_95_bonferroni_bps=np.quantile(means, [.0125, .9875]).tolist(),
            mde_80_bps=float((2.241402727604947+.8416212335729143)*means.std(ddof=1)),
            inference_status="SYNC_MOVING_BLOCK_TWO_ENDPOINT_EXPLORATORY_NOT_CONFIRMATION")
    return result


def _validate(*, rows, days, predictions, plan, calendar):
    if (rows.duplicated(["package_id", *KEY]).any() or days.duplicated(["package_id", "decision_date"]).any()
            or len(rows) > 200000 or set(predictions) != set(ARMS)):
        raise ValueError("four-arm original keys/row budget differ")
    identities = {s.package_id: s for s in plan.sources}
    if set(days.package_id) != set(identities) or not set(rows.package_id).issubset(identities):
        raise ValueError("evaluation original package identity differs")
    sessions = pd.to_datetime(calendar)
    if list(sessions) != sorted(set(sessions)):
        raise ValueError("evaluation requires original sorted unique sessions")
    expected_days = list(sessions[(sessions >= pd.Timestamp(plan.evaluation_start)) & (sessions <= pd.Timestamp(plan.evaluation_end))])
    for package in identities:
        if days.loc[days.package_id.eq(package), "decision_date"].tolist() != expected_days:
            raise ValueError("evaluation removed an original package/date")
    for package, group in rows.groupby("package_id", sort=False):
        source = identities[package]
        if not group.manifest_sha256.eq(source.manifest_sha256).all() or not group.run_id.eq(source.run_id).all():
            raise ValueError("evaluation changed original package/run")
        for _, roster in group.groupby(KEY[0], sort=False):
            if (roster.selection_effective_rank.tolist() != list(range(1, len(roster)+1))
                    or not roster.candidate_group_size.eq(len(roster)).all() or len(roster) > 50
                    or roster[KEY[1]].nunique() != 1):
                raise ValueError("evaluation changed original ranks or T")
    fields = [*FEATURES, "valuation_gross_terminal_ratio", "valuation_path_min_ratio", "valuation_status",
              "hypothetical_liquidation_net_bps", "observed_gap_bps", "label_information_end"]
    if rows.groupby(list(KEY))[fields].nunique(dropna=True).gt(1).any().any():
        raise ValueError("evaluation duplicate-label clusters have contradictory finance")
    if not rows.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all():
        raise ValueError("evaluation valuation policy differs")
    available = rows.loc[rows.valuation_status.eq("AVAILABLE")]
    for row in available.to_dict("records"):
        gap, terminal, lower, net, mark = (_finite(row[n]) for n in ("observed_gap_bps",
            "valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"))
        if any(v is None for v in (gap, terminal, lower, net, mark)) or gap <= -10000 or not 0 < lower <= terminal:
            raise ValueError("known valuation has incomplete or contradictory paired coordinates")
        raw = terminal/((1+gap/10000)*(1+POLICY["buy_bps"]/10000))
        if not math.isclose(net, 10000*(raw*(1-POLICY["sell_bps"]/10000)-1), abs_tol=1e-6) or not math.isclose(mark, 10000*(raw-1), abs_tol=1e-6):
            raise ValueError("known valuation costs are not applied exactly once")
    expected = set(rows.loc[:, ["package_id", *KEY]].itertuples(index=False, name=None))
    maps = {}
    for arm, frame in predictions.items():
        if (frame.duplicated(["package_id", *KEY]).any()
                or set(frame.loc[:, ["package_id", *KEY]].itertuples(index=False, name=None)) != expected
                or not frame.schema_sha256.eq(SCHEMA_SHA256).all() or not frame.policy_sha256.eq(POLICY_SHA256).all()
                or not frame.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
                or not frame.status.isin(["ACCEPTABLE", "AVOID", *UNKNOWN]).all()):
            raise ValueError("prediction keys/schema/policy differ from the original roster")
        known = frame.status.isin(["ACCEPTABLE", "AVOID"])
        values = frame.loc[known, ["expected_net_bps", "downside_q90_bps", "profit_probability"]].map(_number)
        acceptable = values.expected_net_bps.gt(0) & values.downside_q90_bps.le(POLICY["risk_bps"])
        if (values.isna().any().any() or not values.profit_probability.between(0, 1).all()
                or not values.downside_q90_bps.ge(0).all()
                or not frame.loc[known, "status"].eq("ACCEPTABLE").eq(acceptable).all()):
            raise ValueError("known predictions have invalid economic coordinates")
        if frame.loc[~known, ["expected_net_bps", "downside_q90_bps", "profit_probability"]].notna().any().any():
            raise ValueError("UNKNOWN predictions cannot claim known value/probability")
        expected_identity = rows.set_index(["package_id", *KEY]).loc[:, ["manifest_sha256", "run_id"]]
        actual_identity = frame.set_index(["package_id", *KEY]).loc[expected_identity.index, ["manifest_sha256", "run_id"]]
        if not actual_identity.equals(expected_identity):
            raise ValueError("prediction changed the original manifest/run")
        maps[arm] = frame.set_index(["package_id", *KEY]).to_dict("index")
    for item in days.itertuples(index=False):
        position = list(sessions).index(item.decision_date)
        expected_t = sessions[position+1] if position+1 < len(sessions) else pd.NaT
        expected_h = sessions[position+5] if position+5 < len(sessions) else pd.NaT
        if item.target_date != expected_t or not (item.horizon_end == expected_h or pd.isna(item.horizon_end) and pd.isna(expected_h)):
            raise ValueError("D/T/H are not the original five calendar sessions")
        group = rows.loc[rows.package_id.eq(item.package_id) & rows[KEY[0]].eq(item.decision_date)]
        if len(group) != item.original_candidates or (len(group) and not group[KEY[1]].eq(item.target_date).all()):
            raise ValueError("day schedule contradicts its frozen roster")
        if len(group) and not group.label_information_end.eq(item.horizon_end).all():
            raise ValueError("five-session label H differs from the original schedule")
    return maps


def _episode(row, maps, mature):
    key = tuple(row[n] for n in ("package_id", *KEY))
    actual, mark = _finite(row["hypothetical_liquidation_net_bps"]), _finite(row["mark_to_market_net_bps"])
    gap = _finite(row["observed_gap_bps"])
    unavailable_entry = row["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE"
    actions, contributions = {}, {}
    for arm in COHORT_ARMS:
        if unavailable_entry:
            action = "NOT_EXECUTABLE"
        elif arm == "baseline":
            action = "TAKE"
        elif arm == "rule_300bps":
            action = "UNKNOWN" if gap is None else "TAKE" if -300 <= gap <= 300 else "SKIP"
        else:
            state = maps[arm][key]["status"]
            action = "TAKE" if state == "ACCEPTABLE" else "SKIP" if state == "AVOID" else "UNKNOWN"
        actions[arm] = action
        contributions[arm] = actual if mature and action == "TAKE" else None if not mature else 0.
    mark_contributions = {arm: mark if mature and actions[arm] == "TAKE" else None if not mature else 0. for arm in COHORT_ARMS}
    return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(),
        instrument=row["instrument"], rank=int(row["selection_effective_rank"]), actual_net_bps=actual,
        mark_only_net_bps=mark, label_status=row["label_status"], valuation_status=row["valuation_status"],
        exit_execution_status=row["exit_execution_status"], actions=actions, contributions_bps=contributions,
        mark_contributions_bps=mark_contributions,
        actual_fill_proven=False, realized_return_bps=None)


def _attribution(*, episodes, paired_dates, opponent, package_count):
    parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
        known_action_contribution_bps=0., unknown_cash_contribution_bps=0.)
    settled, changed_days, clusters = 0, set(), set()
    if paired_dates:
        scale = 1/(5*package_count*len(paired_dates))
        for item in episodes:
            if item["decision_date"] not in paired_dates:
                continue
            a, b = item["actions"][ARMS[1]], item["actions"][opponent]
            delta = item["contributions_bps"][ARMS[1]]-item["contributions_bps"][opponent]
            name = "unknown_cash_contribution_bps" if "UNKNOWN" in (a, b) else "known_action_contribution_bps"
            parts[name] += delta*scale
            if name.startswith("known") and a != b and item["actual_net_bps"] is not None and "NOT_EXECUTABLE" not in (a, b):
                settled += 1
                changed_days.add(item["decision_date"])
                clusters.add((item["decision_date"], item["instrument"]))
                actual = item["actual_net_bps"]
                if a == "SKIP":
                    cell = "avoided_loss_bps" if actual < 0 else "missed_profit_bps"
                else:
                    cell = "new_take_profit_bps" if actual >= 0 else "new_take_loss_bps"
                parts[cell] += abs(actual)*scale
    return parts, settled, changed_days, clusters


def _summary(groups, episodes, *, package_count, inference):
    dates = sorted({g["decision_date"] for g in groups if g["horizon_mature"]})
    aggregate, mark_aggregate = {}, {}
    for d in dates:
        cells = [g for g in groups if g["decision_date"] == d]
        aggregate[d] = {arm: None if len(cells) != package_count or any(g["cohort_net_bps"][arm] is None for g in cells)
            else float(np.mean([g["cohort_net_bps"][arm] for g in cells])) for arm in COHORT_ARMS}
        mark_aggregate[d] = {arm: None if len(cells) != package_count or any(g["cohort_mark_bps"][arm] is None for g in cells)
            else float(np.mean([g["cohort_mark_bps"][arm] for g in cells])) for arm in COHORT_ARMS}
    panels = {opponent: [None if any(aggregate[d][a] is None for a in (ARMS[1], opponent))
        else aggregate[d][ARMS[1]]-aggregate[d][opponent] for d in dates] for opponent in ("baseline", ARMS[0])}
    statistics = paired_statistics(panels)
    if not inference:
        for stats in statistics.values():
            stats.update(confidence_interval_95_diagnostic_bps=None, simultaneous_95_bonferroni_bps=None,
                         mde_80_bps=None, inference_status="DESCRIPTIVE_PACKAGE_STRATUM_ONLY")
    pairs = {}
    for opponent, panel in panels.items():
        paired_dates = {d for d, value in zip(dates, panel, strict=True) if value is not None}
        parts, interventions, intervention_days, intervention_clusters = _attribution(episodes=episodes, paired_dates=paired_dates,
                                                               opponent=opponent, package_count=package_count)
        mean = statistics[opponent]["mean_increment_bps"]
        if mean is not None and not math.isclose(mean, parts["known_action_contribution_bps"]+parts["unknown_cash_contribution_bps"], rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError("incremental slot attribution does not reconcile")
        pairs[opponent] = dict(**statistics[opponent], attribution=parts, settled_known_interventions=interventions,
            settled_intervention_unique_stock_days=len(intervention_clusters),
            settled_intervention_days=len(intervention_days), intervention_day_fraction=len(intervention_days)/len(dates) if dates else None,
            exploratory_support_hint=bool(dates) and interventions >= 20 and len(intervention_days) >= 12 and len(intervention_days)/len(dates) >= .1,
            support_hint_is_confirmation_gate=False)
    common = [g for g in aggregate.values() if all(v is not None for v in g.values())]
    means = {arm: dict(known_days=sum(g[arm] is not None for g in aggregate.values()),
        mean_bps=float(np.mean([g[arm] for g in aggregate.values() if g[arm] is not None])) if any(g[arm] is not None for g in aggregate.values()) else None,
        common_four_arm_days=len(common), common_four_arm_mean_bps=float(np.mean([g[arm] for g in common])) if common else None) for arm in COHORT_ARMS}
    for arm in COHORT_ARMS:
        marks = [g[arm] for g in mark_aggregate.values() if g[arm] is not None]
        means[arm].update(mark_only_known_days=len(marks), mark_only_mean_bps=float(np.mean(marks)) if marks else None)
    takes = {}
    for arm in COHORT_ARMS:
        known = [e["actual_net_bps"] for e in episodes if e["decision_date"] in dates and e["actions"][arm] == "TAKE" and e["actual_net_bps"] is not None]
        wins, losses = [v for v in known if v > 0], [v for v in known if v < 0]
        takes[arm] = dict(valued_take_episodes=len(known), hit_rate=sum(v > 0 for v in known)/len(known) if known else None,
            unique_instruments=len({e["instrument"] for e in episodes if e["decision_date"] in dates and e["actions"][arm] == "TAKE" and e["actual_net_bps"] is not None}),
            unique_valued_stock_days=len({(e["decision_date"], e["instrument"]) for e in episodes if e["decision_date"] in dates and e["actions"][arm] == "TAKE" and e["actual_net_bps"] is not None}),
            mean_winner_bps=float(np.mean(wins)) if wins else None, mean_loser_bps=float(np.mean(losses)) if losses else None,
            average_win_loss_ratio=float(np.mean(wins)/abs(np.mean(losses))) if wins and losses else None,
            q10_net_bps=float(np.quantile(known, .1)) if known else None,
            known_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] in ("SKIP", "NOT_EXECUTABLE") for e in episodes),
            unknown_cash_slots=sum(e["decision_date"] in dates and e["actions"][arm] == "UNKNOWN" for e in episodes))
    return dict(original_mature_days=len(dates), pairs=pairs, cohort_means=means, take_statistics=takes,
        equal_package_weight=1/package_count, overlapping_five_session_cohort_not_nav=True)


def evaluate_parent_score_cohorts(*, rows, days, predictions, plan, calendar):
    # Temporal keys are projected before outcomes are parsed; no test/sealed consumer.
    rows = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    days = days.loc[days.decision_date.between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    predictions = {arm: frame.loc[frame[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
                   for arm, frame in predictions.items()}
    maps = _validate(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    groups, episodes = [], []
    for day in days.to_dict("records"):
        original = rows.loc[rows.package_id.eq(day["package_id"]) & rows[KEY[0]].eq(day["decision_date"])]
        mature = pd.notna(day["horizon_end"]) and day["horizon_end"] <= pd.Timestamp(plan.evaluation_end)
        selected = original.loc[original.selection_effective_rank.le(5)]
        records = [_episode(row, maps, mature) for row in selected.to_dict("records")]
        episodes.extend(records)
        missing = day["source_status"] != "KNOWN_ROSTER"
        values = {arm: None if not mature or missing or any(e["contributions_bps"][arm] is None for e in records)
            else sum(e["contributions_bps"][arm] for e in records)/5 for arm in COHORT_ARMS}
        mark_values = {arm: None if not mature or missing or any(e["mark_contributions_bps"][arm] is None for e in records)
            else sum(e["mark_contributions_bps"][arm] for e in records)/5 for arm in COHORT_ARMS}
        trend = _finite(original.csi300_ret_5.iloc[0]) if len(original) else None
        groups.append(dict(package_id=day["package_id"], decision_date=day["decision_date"].date().isoformat(),
            horizon_mature=bool(mature), source_status=day["source_status"], original_candidates=len(original),
            original_slots=5, structural_empty_slots=5-len(records) if not missing else None,
            cohort_net_bps=values, cohort_mark_bps=mark_values,
            lagged_csi300_5d_proxy="UNKNOWN" if trend is None else "UP" if trend > 0 else "DOWN" if trend < 0 else "FLAT"))
    primary = _summary(groups, episodes, package_count=2, inference=True)
    package_results = {source.package_id: _summary([g for g in groups if g["package_id"] == source.package_id],
        [e for e in episodes if e["package_id"] == source.package_id], package_count=1, inference=False) for source in plan.sources}
    continuing = all(p["mean_increment_bps"] is not None and p["mean_increment_bps"] > 0
        and p["attribution"]["known_action_contribution_bps"] > 0 for p in primary["pairs"].values())
    calibration = {}
    for arm in ARMS:
        observations = []
        for row in rows.loc[rows.selection_effective_rank.le(5)].to_dict("records"):
            forecast = maps[arm][tuple(row[n] for n in ("package_id", *KEY))]
            actual, probability = _finite(row["hypothetical_liquidation_net_bps"]), _finite(forecast["profit_probability"])
            if (actual is not None and probability is not None and row["valuation_status"] == "AVAILABLE"
                    and forecast["status"] in ("ACCEPTABLE", "AVOID")):
                observations.append((probability, float(actual > 0)))
        calibration[arm] = dict(known_episodes=len(observations), brier=float(np.mean([(p-y)**2 for p, y in observations])) if observations else None,
            calibration_fit_count=0, independent_evidence=False)
    monthly = []
    for (package, month), group in rows.groupby(["package_id", rows[KEY[0]].dt.strftime("%Y-%m")], sort=True):
        score = group.parent_score.map(_number).dropna().to_numpy(float)
        cell = dict(package_id=package, month=month, original_rows=len(group), known_scores=len(score),
            raw_score_median=float(np.median(score)) if len(score) else None,
            raw_score_iqr=float(np.quantile(score, .75)-np.quantile(score, .25)) if len(score) else None,
            normalization_refit_count=0, diagnostic_only=True)
        for arm in ARMS:
            values = []
            for row in group.loc[group.selection_effective_rank.le(5)].to_dict("records"):
                forecast = maps[arm][tuple(row[n] for n in ("package_id", *KEY))]
                actual, probability = _finite(row["hypothetical_liquidation_net_bps"]), _finite(forecast["profit_probability"])
                if row["valuation_status"] == "AVAILABLE" and actual is not None and probability is not None:
                    values.append((probability, float(actual > 0)))
            cell[arm] = dict(known_episodes=len(values), brier=float(np.mean([(p-y)**2 for p, y in values])) if values else None,
                mean_probability=float(np.mean([p for p, _ in values])) if values else None,
                take_outcome_frequency=float(np.mean([y for _, y in values])) if values else None, calibration_fit_count=0)
        monthly.append(cell)
    return dict(status="EXPLORATORY_PROMISING_NOT_ACTIVATION" if continuing else "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE",
        hypothesis="GP5-PARENT-SCORE-CONDITION-1", primary=primary, packages=package_results, calibration=calibration,
        monthly_calibration_score_drift=monthly,
        original_groups=groups, original_episodes=episodes, original_rows=len(rows), original_package_days=len(days),
        legacy_v1_execution=dict(available_entries=int(rows.label_status.eq("AVAILABLE").sum()),
            unknown_entries=int(rows.label_status.eq("UNKNOWN").sum()), confidence_interval=None,
            semantics="ORIGINAL_NOMINAL_EXECUTION_SUBSET_NOT_ACTUAL_FILL"),
        exit_execution_counts={str(k): int(v) for k, v in rows.exit_execution_status.value_counts().items()},
        evidence_use="NAVIGATION_ONLY", objective_contract=plan.objective_contract, study_type=plan.study_type,
        actual_fill_proven=False, realized_return_bps=None, independent_oos_evidence=False, activation_evidence=False,
        dataset_vintage="CURRENT_DATABASE_NON_VINTAGE", nav_or_annualized_return_claimed=False,
        automatic_followup_trial=False, deployable=False)
