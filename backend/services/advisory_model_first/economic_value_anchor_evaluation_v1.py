"""Actual-open navigation under ONE independent scene, never activation."""
import json
from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_aligned_evaluation import (
    aligned_intervention_attribution_v3, fixed_block_interval_v3,
    shadow_endpoint_execution_audit_v3, shadow_portfolio_intervention_support_v3,
)
from backend.services.advisory_model_first.economic_entry_evaluation import matched_entry_priorities
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1, value_anchor_policy_v1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import evaluate_value_anchor_price_v1
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import _positive, value_anchor_shadow_inputs_v1
from backend.services.advisory_model_first.economic_value_anchor_pipeline_v1 import _ledger, _record, _value_prices, load_value_anchor_fit_v1, load_value_anchor_v1
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import value_anchor_predict_v1
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def value_anchor_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    roster = _frame(candidates.loc[:, KEY+["selection_effective_rank"]], KEY, set(KEY)|{"selection_effective_rank"})
    features = _frame(inputs.loc[:, KEY+list(D_FEATURES)+["feature_visible_through"]], KEY, set(KEY)|set(D_FEATURES)|{"feature_visible_through"})
    if not pd.to_datetime(features.feature_visible_through).eq(features[KEY[0]]).all():
        raise ValueError("value forecast cannot consume features after D")
    feature_map = features.set_index(KEY).to_dict("index")
    if (not (roster[KEY[0]] < roster[KEY[1]]).all()
            or not set(roster[KEY].itertuples(index=False, name=None)).issubset(feature_map)):
        raise ValueError("value observed roster contains a foreign candidate/clock")
    quote_fields = ["trade_date", "instrument", "raw_open_cny", "suspended", "tradability_unknown", "up_limit", "down_limit", "source_sha256", "price_coordinate_sha256"]
    quotes = _frame(prices.loc[:, quote_fields], quote_fields[:2], set(quote_fields))
    ref_fields = [*KEY, "target_reference_raw_cny", "reference_visible_through", "source_sha256"]
    refs = _frame(references.loc[:, ref_fields], KEY, set(ref_fields))
    visible = refs.reference_visible_through.map(_day)
    if not visible.le(refs[KEY[0]]).all():
        raise ValueError("value reference sees after D")
    if (not quotes.source_sha256.eq(identity.price_source_sha256).all() or not quotes.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()
            or not refs.source_sha256.eq(identity.reference_source_sha256).all()
            or any(not quotes[name].map(lambda value: type(value) is bool).all() for name in ("suspended", "tradability_unknown"))):
        raise ValueError("value observed source/trading identity differs")
    quote_map, ref_map = quotes.set_index(quote_fields[:2]).to_dict("index"), refs.set_index(KEY).to_dict("index")
    output, matrices, positions = [], [], []
    for candidate in roster.to_dict("records"):
        key = tuple(candidate[name] for name in KEY)
        quote, ref, feature = quote_map.get((key[1], key[2])), ref_map.get(key), feature_map.get(key)
        row = {**candidate, "model_action": "UNAVAILABLE", "market_admissible": False, "reason_code": "MARKET_UNPROVEN",
            "expected_net_return_bps": None, "downside_q90_bps": None, "actual_open_cny": None, "reference_cny": None}
        if quote is None or ref is None:
            output.append(row)
            continue
        if pd.Timestamp(ref["reference_visible_through"]) > key[0]:
            raise ValueError("value reference sees after D")
        price, anchor, upper, lower = (_positive(quote["raw_open_cny"]), _positive(ref["target_reference_raw_cny"]),
            _positive(quote["up_limit"]), _positive(quote["down_limit"]))
        if None not in (price, upper, lower) and (lower > upper or price < lower-1e-9 or price > upper+1e-9):
            raise ValueError("value observed open contradicts legal limits")
        if quote["suspended"] or quote["tradability_unknown"] or None in (price, anchor, upper, lower) or price >= upper or Decimal(str(price)) % Decimal(".01"):
            output.append(row)
            continue
        row.update(market_admissible=True, actual_open_cny=price, reference_cny=anchor, reason_code="UNKNOWN_D_OR_SUPPORT")
        values = [feature[name] for name in D_FEATURES] if feature is not None else []
        complete = len(values) == len(D_FEATURES) and all(value is not None and not isinstance(value, (bool, np.bool_)) and np.isfinite(float(value)) for value in values)
        if complete:
            positions.append(len(output))
            matrices.append(dict(zip(D_FEATURES, values, strict=True)))
        output.append(row)
    if matrices:
        estimates = value_anchor_predict_v1(fitted, pd.DataFrame(matrices, columns=D_FEATURES), arm=arm)
        for position, estimate in zip(positions, estimates, strict=True):
            row = output[position]
            point = evaluate_value_anchor_price_v1(estimate=estimate, price_cny=row["actual_open_cny"], reference_cny=row["reference_cny"], support=fitted.gap_support)
            row.update(model_action={"ACCEPTABLE": "TAKE", "AVOID": "SKIP", "UNKNOWN_OUT_OF_SUPPORT": "UNAVAILABLE"}[point.status],
                reason_code=point.status, expected_net_return_bps=point.expected_net_bps, downside_q90_bps=point.downside_q90_bps)
    return pd.DataFrame(output)


def _holding_audit(episodes, prices, targets):
    quotes = prices.set_index(["trade_date", "instrument"]).to_dict("index")
    issues = []
    for episode in episodes.to_dict("records"):
        end = episode["exit_trade_date"]
        for day in targets[(targets >= episode["entry_trade_date"]) & (targets <= end if pd.notna(end) else True)]:
            quote = quotes.get((day, episode["instrument"]))
            if quote is None or quote["tradability_unknown"] or (not quote["suspended"] and None in (_positive(quote["raw_open_cny"]), _positive(quote["policy_price_per_raw_cny"]))):
                issues.append({"episode_id": episode["episode_id"], "trade_date": day.date().isoformat(), "reason": "HELD_MARK_UNPROVEN"})
    return issues


def _power(values):
    values = np.asarray(values, dtype=float)
    random, width = np.random.default_rng(20261002), min(5, len(values))
    means = []
    for _ in range(2000):
        starts = random.integers(0, len(values)-width+1, size=int(np.ceil(len(values)/width)))
        means.append(np.concatenate([values[start:start+width] for start in starts])[:len(values)].mean())
    error = float(np.std(means, ddof=1))
    return {"status": "DESCRIPTIVE_PROXY" if len(values) >= 10 and error > 0 else "UNPROVEN_SHORT_OR_DEGENERATE",
        "mde_bps": 2.801585218112967*error if len(values) >= 10 and error > 0 else None,
        "interpretation": "POST_EVALUATION_VARIANCE_PROXY_NOT_ACHIEVED_POWER", "activation_supported": False}


def evaluate_value_anchor_v1(*, plan_path, output_root):
    plan, root, registered, source = load_value_anchor_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _ = source
    prepared = read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    trained = read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    _ledger(plan, root, "TRAINED", root/"trained/manifest.json")
    if (root/"evaluated").exists():
        read_stage(root/"evaluated", stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        _record(plan, root, parent, "EVALUATED", root/"evaluated/manifest.json", generated=1, evaluated=1)
        return root/"evaluated"
    fitted = load_value_anchor_fit_v1(plan_path=plan_path, output_root=output_root)
    rankings = pd.read_parquet(frozen/"frozen_rankings.parquet")
    rankings = rankings.loc[rankings[KEY[0]].ge(pd.Timestamp(parent.configuration.test_start))]
    candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)
        & rankings[KEY[0]].le(pd.Timestamp(parent.configuration.test_end))]
    prices, references = _value_prices(frozen, parent.configuration), pd.read_parquet(frozen/"references.parquet")
    inputs = pd.read_parquet(root/"prepared/rows.parquet", columns=[*KEY, *D_FEATURES, "feature_visible_through"])
    decisions = {arm: value_anchor_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm) for arm in ("constant", "model")}
    baseline = decisions["constant"].copy()
    baseline["model_action"] = "TAKE"
    all_actions = {"baseline": baseline, **decisions}
    market, cash, suspend, calendar = value_anchor_shadow_inputs_v1(prices=prices,
        calendar=json.loads((frozen/"calendar.json").read_text(encoding="utf-8")))
    targets = pd.DatetimeIndex(rankings[KEY[1]].unique()).sort_values()
    dates = sorted(candidates[KEY[0]].unique())
    portfolios, audits, artifacts = {}, {}, {}
    for arm, action in all_actions.items():
        priorities = matched_entry_priorities(action.loc[action.market_admissible], arm="model")
        result = replay_shadow_portfolio(rankings=rankings, daily=market, benchmark_daily=cash, suspend_rows=suspend,
            trading_calendar=calendar, policy=value_anchor_policy_v1(), policy_sha256=value_anchor_policy_sha256_v1(), cost_policy=COST,
            request_id=plan.experiment_id+"_"+arm, candidate_decision_dates=dates, entry_priorities=priorities)
        portfolios[arm] = result
        audits[arm] = {"endpoints": shadow_endpoint_execution_audit_v3(result.episodes, prices), "holding": _holding_audit(result.episodes, prices, targets)}
        artifacts[arm+"_daily.parquet"], artifacts[arm+"_episodes.parquet"] = _parquet_bytes(result.daily), _parquet_bytes(result.episodes)
    blocked = any(audit["endpoints"]["restricted_episode_count"] or audit["holding"] for audit in audits.values())
    report = {"plan_sha256": plan.plan_sha256, "value_policy_sha256": value_anchor_policy_sha256_v1(), "audits": audits,
        "decision_use": "NAVIGATION_ONLY", "deployable": False, "economic_effectiveness": "NOT_CONFIRMED",
        "source_evidence": identity.source_evidence, "evidence_limitations": identity.evidence_limitations,
        "sealed_accessed": False, "real_fill_proven": False, "test_decision_days": len(dates), "candidate_rows": len(candidates),
        "fitted_head_count": 2, "model_configuration_count": 1, "economic_candidate_count": 1,
        "regime_support": "UNPROVEN_NO_PREREGISTERED_REGIME", "metrics": None, "increment": None,
        "navigation": "BLOCKED_EXECUTION_OR_MARK_UNPROVEN"}
    if not blocked:
        series, metrics = {}, {}
        for arm, result in portfolios.items():
            daily = result.daily.set_index("target_trade_date").net_return_bps
            values = daily.reindex(targets)
            absent = values.isna()
            if result.metrics["active_episode_count"] or (absent & (values.index <= daily.index.max())).any():
                raise ValueError("value replay has unfinished or missing valuations")
            values = values.fillna(0.)  # Only proven terminal idle-cash dates.
            wealth = pd.Series(np.r_[1., (1+values/10000).cumprod().to_numpy()])
            metrics[arm] = {**result.metrics, "common_day_count": len(values), "common_horizon_return": float(wealth.iloc[-1]-1),
                "common_horizon_max_drawdown": float((wealth/wealth.cummax()-1).min()), "benchmark": "ZERO_RETURN_CASH_NOT_INDEX"}
            series[arm] = values
        matched = pd.DataFrame(series)
        increment = {"model_minus_constant": fixed_block_interval_v3(matched.model-matched.constant),
                     "model_minus_baseline": fixed_block_interval_v3(matched.model-matched.baseline)}
        support = shadow_portfolio_intervention_support_v3(portfolios["constant"].episodes, portfolios["model"].episodes, targets)
        positive = all(value["mean_bps"] > 0 for value in increment.values()) and support["entry_action_difference_day_count"] > 0
        report.update(metrics=metrics, increment=increment, actual_interventions=support,
            attribution={arm: aligned_intervention_attribution_v3(decisions[arm], portfolios["baseline"].episodes, portfolios[arm].episodes) for arm in decisions},
            power={"model_minus_constant": _power(matched.model-matched.constant), "model_minus_baseline": _power(matched.model-matched.baseline)},
            navigation="CONSIDER_FURTHER_EVALUATION_NOT_CONFIRMATION" if positive else "STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION")
        artifacts["matched_daily.parquet"] = _parquet_bytes(matched.reset_index())
    artifacts.update({arm+"_decisions.parquet": _parquet_bytes(action) for arm, action in decisions.items()})
    artifacts["evaluation.json"] = _json_bytes(report)
    target = publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"], artifacts=artifacts)
    _record(plan, root, parent, "EVALUATED", target/"manifest.json", generated=1, evaluated=1)
    return target
