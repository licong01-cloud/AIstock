"""Navigation-only actual-open queries and matched fixed-slot portfolio replay.

This is not evidence that a complete D-frozen daily price grid was deployed.
It never chooses a new model, policy, threshold or replacement candidate.
"""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_inference import build_economic_price_advice, select_frozen_opening_advice
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame, _positive
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _check_registered_stage, _json_bytes, _parquet_bytes, _register, _prepared,
    file_sha256, load_economic_study, load_fitted_economic_study, publish_stage, read_stage,
)
from backend.services.advisory_model_first.economic_entry_training import EconomicEntryTrainingResult
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import transition_policy_from_payload
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def query_actual_open_conditions(
    *, fitted: EconomicEntryTrainingResult, identity: EconomicEntryInputIdentityV1,
    bundle_sha256: str, candidates: pd.DataFrame, features: pd.DataFrame,
    references: pd.DataFrame, prices: pd.DataFrame,
) -> pd.DataFrame:
    """D fields + T raw open only; no maturity, return, later OHLC or future path."""
    identity = EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    if identity.identity_sha256 != fitted.request.input_identity_sha256:
        _fail("economic query identity differs from the frozen model")
    roster = _frame(candidates, KEY, set(KEY) | {"selection_effective_rank"})
    d_names = [name for name in fitted.request.feature_names if name != "query_gap_bps"]
    fields = set(KEY + d_names + ["feature_visible_through", "feature_source_sha256"])
    feature_map = _frame(features.loc[:, sorted(fields)], KEY, fields).set_index(KEY).to_dict("index")
    if not features["feature_source_sha256"].eq(fitted.request.feature_source_sha256).all():
        _fail("evaluation feature source differs from the frozen fitted request")
    if (pd.to_datetime(features["feature_visible_through"]) > pd.to_datetime(features[KEY[0]])).any():
        _fail("economic evaluation feature clock exceeds D")
    refs = _frame(references, KEY, set(KEY) | {"target_reference_raw_cny", "reference_visible_through"}).set_index(KEY).to_dict("index")
    market = _frame(prices, ["trade_date", "instrument"], {"trade_date", "instrument", "raw_open_cny", "suspended"}).set_index(["trade_date", "instrument"]).to_dict("index")
    for frame, column, expected in ((prices, "source_sha256", identity.price_source_sha256),
                                    (prices, "price_coordinate_sha256", identity.price_coordinate_sha256),
                                    (references, "source_sha256", identity.reference_source_sha256)):
        if column not in frame or not frame[column].eq(expected).all():
            _fail("economic evaluation source identity mismatch")
    records = []
    for row in roster.to_dict("records"):
        decision, target, symbol = (row[name] for name in KEY)
        key = decision, target, symbol
        feature, reference, observed = feature_map.get(key), refs.get(key), market.get((target, symbol))
        record = {**row, "model_action": "UNAVAILABLE", "rule_action": "UNAVAILABLE",
                  "reason_code": "SOURCE_UNAVAILABLE", "actual_gap_bps": None,
                  "expected_net_return_bps": None, "daily_mark_drawdown_q90_bps": None}
        if feature is None or reference is None or observed is None:
            records.append(record)
            continue
        if pd.Timestamp(reference["reference_visible_through"]) != decision:
            _fail("economic evaluation reference clock is not frozen at D")
        if observed.get("tradability_unknown", False):
            record["reason_code"] = "OPEN_TRADABILITY_UNKNOWN"
            records.append(record)
            continue
        if observed["suspended"]:
            record.update(model_action="NOT_APPLICABLE", rule_action="NOT_APPLICABLE", reason_code="SUSPENDED")
            records.append(record)
            continue
        actual, anchor = _positive(observed["raw_open_cny"]), _positive(reference["target_reference_raw_cny"])
        if actual is None or anchor is None:
            records.append(record)
            continue
        up_limit, down_limit = _positive(observed.get("up_limit")), _positive(observed.get("down_limit"))
        if up_limit is None or down_limit is None or actual >= up_limit or actual < down_limit:
            record["reason_code"] = "OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"
            records.append(record)
            continue
        if Decimal(str(actual)) % Decimal(".01"):
            record["reason_code"] = "OBSERVED_OPEN_OFF_EQUITY_TICK"
            records.append(record)
            continue
        gap = (actual / anchor - 1) * 10000
        record.update(actual_gap_bps=gap, rule_action="TAKE" if -300 <= gap <= 300 else "SKIP")
        # Singleton is a conditional query at the real observed price, not a
        # hindsight claim that this price was the D-frozen grid or best entry.
        advice = build_economic_price_advice(
            fitted=fitted, model_bundle_sha256=bundle_sha256, input_identity=identity,
            instrument=symbol, decision_date=decision.date(), target_date=target.date(),
            feature_visible_through=pd.Timestamp(feature["feature_visible_through"]).date(),
            decision_features={name: feature[name] for name in d_names}, reference_raw_cny=anchor,
            legal_price_min_cny=actual, legal_price_max_cny=actual,
            query_prices_cny=[actual], grid_step_cny=.01, tick_size_cny=.01,
        )
        selected = select_frozen_opening_advice(
            advice=advice, expected_advice_sha256=advice["advice_sha256"], observation_date=target.date(),
            actual_open_raw_cny=actual, trading_status="EXECUTABLE",
        )
        record.update(model_action=selected["action"], reason_code=selected["reason_code"],
                      query_advice_sha256=advice["advice_sha256"])
        node = selected.get("selected_node")
        if node:
            record.update(expected_net_return_bps=node["expected_net_return_bps"],
                          daily_mark_drawdown_q90_bps=node["daily_mark_drawdown_q90_bps"])
        records.append(record)
    return pd.DataFrame(records)


def matched_entry_priorities(decisions: pd.DataFrame, *, arm: str) -> pd.DataFrame:
    """Unknown keeps baseline for research control, never counts as model TAKE.

    Only original Top5 can enter. Active holdings still use original exit ranks
    inside the existing simulator. Skipped ranks are never renumbered or filled.
    """
    column = {"model": "model_action", "rule": "rule_action"}[arm]
    if not decisions[column].isin({"TAKE", "SKIP", "UNAVAILABLE", "NOT_APPLICABLE"}).all():
        _fail("economic matched evaluation contains unknown action states")
    selected = decisions.loc[decisions["selection_effective_rank"].le(5) & ~decisions[column].eq("SKIP")]
    return selected.loc[:, [KEY[0], "instrument", "selection_effective_rank"]].rename(columns={"selection_effective_rank": "entry_priority_rank"})


def economic_intervention_attribution(*, decisions: pd.DataFrame, baseline_episodes: pd.DataFrame,
                                      model_episodes: pd.DataFrame) -> dict:
    top5 = decisions.loc[decisions["selection_effective_rank"].le(5)].copy()
    keys = [KEY[0], "instrument"]
    if top5.duplicated(keys).any():
        _fail("economic intervention attribution has duplicate candidate keys")
    actions = top5.loc[:, keys + ["model_action"]].rename(columns={KEY[0]: "entry_signal_date"})
    entered = (actions.iloc[:0].copy() if model_episodes.empty else
               model_episodes.merge(actions, on=["entry_signal_date", "instrument"], how="left", validate="many_to_one"))
    if not entered["model_action"].isin({"TAKE", "UNAVAILABLE", "NOT_APPLICABLE"}).all():
        _fail("model portfolio entered an unbound or skipped candidate")
    baseline = (actions.iloc[:0].assign(net_return_bps=pd.Series(dtype=float)) if baseline_episodes.empty else
                baseline_episodes.merge(actions, on=["entry_signal_date", "instrument"], how="left", validate="many_to_one"))
    skipped = baseline.loc[baseline["model_action"].eq("SKIP")]
    if "net_return_bps" not in skipped:
        _fail("economic attribution requires explicit episode outcomes")
    supported = top5.loc[top5["expected_net_return_bps"].notna()]
    return {
        "actual_model_take_episodes": int(entered["model_action"].eq("TAKE").sum()),
        "research_unknown_baseline_control_episodes": int(entered["model_action"].isin({"UNAVAILABLE", "NOT_APPLICABLE"}).sum()),
        "baseline_entered_episode_count": len(baseline), "baseline_entries_skipped_by_model": len(skipped),
        "skipped_baseline_profitable_episodes": int(skipped["net_return_bps"].gt(0).sum()),
        "skipped_baseline_loss_episodes": int(skipped["net_return_bps"].lt(0).sum()),
        "supported_top5_condition_count": len(supported),
        "positive_expected_value_conditions": int(supported["expected_net_return_bps"].gt(0).sum()),
        "risk_budget_passing_conditions": int(supported["daily_mark_drawdown_q90_bps"].le(800).sum()),
        "model_take_value_evidence": "NO_MODEL_TAKE" if not entered["model_action"].eq("TAKE").any() else "EXPLORATORY_ONLY",
        "episode_analysis_is_not_portfolio_return": True,
        "risk_metric": "whole_policy_episode_peak_to_trough_daily_mark_drawdown_q90",
        "budget_reference": "800bps_entry_stop_not_proof_of_same_risk_semantics",
        "post_result_threshold_change": False,
    }


def audit_published_economic_evaluation(*, plan_path: str | Path, output_root: str | Path) -> Path:
    """Read existing results without rewriting/retraining/re-evaluating them."""
    plan, root = load_economic_study(plan_path, output_root=output_root)
    _check_registered_stage(plan, root, "EVALUATED")
    prepared = _prepared(root, plan)
    trained = read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    parent = read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
    attribution = economic_intervention_attribution(
        decisions=pd.read_parquet(root / "evaluated" / "conditional_decisions.parquet"),
        baseline_episodes=pd.read_parquet(root / "evaluated" / "baseline_episodes.parquet"),
        model_episodes=pd.read_parquet(root / "evaluated" / "model_episodes.parquet"),
    )
    attribution.update({"evaluation_stage_sha256": parent["stage_sha256"], "audit_implementation_sha256": file_sha256(__file__),
                        "decision_use": "NAVIGATION_ONLY", "no_new_model_trial": True,
                        "original_evaluation_unchanged": True, "economic_effectiveness": "NOT_CONFIRMED",
                        "original_query_open_limit_filter": "NOT_EXPLICITLY_CHECKED_NO_MODEL_TAKE",
                        "consumer_open_limit_filter_fixed_for_future_studies": True,
                        "original_study_not_rerun_after_review": True})
    return publish_stage(study_root=root / f"evaluation_review_{file_sha256(__file__)[:12]}", stage="evaluated", plan_sha256=plan.plan_sha256,
                         parent_sha256=parent["stage_sha256"], artifacts={"attribution.json": _json_bytes(attribution)})


def evaluate_economic_study(*, plan_path: str | Path, output_root: str | Path) -> Path:
    plan, root = load_economic_study(plan_path, output_root=output_root)
    prepared = read_stage(root / "prepared", stage="prepared", plan_sha256=plan.plan_sha256,
                          parent_sha256=read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)["stage_sha256"])
    trained = read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    _check_registered_stage(plan, root, "PREPARED")
    _check_registered_stage(plan, root, "TRAINED")
    # The implementation of this diagnostic is bound BEFORE any test query or
    # test outcome analysis; training configuration and original inputs stay fixed.
    eval_contract = {
        "schema_version": "economic_entry_actual_open_navigation_v1", "plan_sha256": plan.plan_sha256,
        "model_stage_sha256": trained["stage_sha256"], "evaluation_implementation_sha256": file_sha256(__file__),
        "shadow_simulator_sha256": file_sha256(Path(__file__).with_name("shadow_portfolio_policy.py")),
        "arms": ["baseline", "rule_gap_minus300_plus300", "model_expected_positive_risk_le800"],
        "unknown_treatment": "research_baseline_control_not_model_take", "no_refill": True,
        "starting_state": "all_arms_empty_at_first_test_decision", "comparison_horizon": "common_frozen_rank_context_through_label_cutoff",
        "evidence_level": "HISTORICAL_REPLAY", "decision_use": "NAVIGATION_ONLY", "deployable": False,
        "price_grid_delivery_verified": False, "cash_benchmark_only": True,
    }
    registration = publish_stage(study_root=root / "evaluation_registration", stage="preregistered", plan_sha256=plan.plan_sha256,
                                 parent_sha256=trained["stage_sha256"], artifacts={"evaluation_contract.json": _json_bytes(eval_contract)})
    _register(plan, root, "EVALUATION_REGISTERED", registration / "manifest.json", generated=1)
    eval_parent = read_stage(registration, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
    if (root / "evaluated").exists():
        read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        _register(plan, root, "EVALUATED", root / "evaluated" / "manifest.json", generated=1, evaluated=1)
        return root / "evaluated"
    fitted = load_fitted_economic_study(plan_path=plan_path, output_root=output_root)
    identity = EconomicEntryInputIdentityV1.model_validate_json((root / "prepared" / "identity.json").read_text(encoding="utf-8"))
    rankings = pd.read_parquet(root / "prepared" / "frozen_rankings.parquet")
    candidates = rankings.loc[rankings["is_candidate_decision"].eq(True) & rankings["selection_effective_rank"].le(20)].copy()
    candidates = candidates.loc[pd.to_datetime(candidates[KEY[0]]).between(pd.Timestamp(plan.configuration.test_start), pd.Timestamp(plan.configuration.test_end))]
    prices = pd.read_parquet(root / "prepared" / "prices.parquet")
    decisions = query_actual_open_conditions(
        fitted=fitted, identity=identity, bundle_sha256=trained["stage_sha256"], candidates=candidates,
        features=pd.read_parquet(root / "prepared" / "features.parquet"),
        references=pd.read_parquet(root / "prepared" / "references.parquet"), prices=prices,
    )
    market = prices.rename(columns={"trade_date": "datetime"}).copy()
    for field in ("open", "high", "low", "close"):
        market[field] = market[f"raw_{field}_cny"] * market["policy_price_per_raw_cny"]
    market["factor"], market["limit_up"], market["limit_down"] = market["policy_price_per_raw_cny"], 1., 1.
    market["up_limit_price"], market["down_limit_price"] = market["up_limit"], market["down_limit"]
    market = market.set_index(["datetime", "instrument"]).sort_index()
    calendar = pd.DatetimeIndex(json.loads((root / "prepared" / "calendar.json").read_text(encoding="utf-8")))
    # Cash 0 is an explicit comparator, NOT a fabricated CSI300 observation.
    cash = pd.DataFrame({"datetime": calendar, "open": 1.}).set_index("datetime")
    suspend = prices.loc[prices["suspended"], ["trade_date", "instrument"]]
    portfolio_rankings = rankings.loc[pd.to_datetime(rankings[KEY[0]]).ge(pd.Timestamp(plan.configuration.test_start))]
    all_dates = sorted(pd.to_datetime(portfolio_rankings[KEY[0]]).unique())
    common_targets = pd.DatetimeIndex(portfolio_rankings[KEY[1]].unique()).sort_values()
    candidate_dates = sorted(pd.to_datetime(candidates[KEY[0]]).unique())
    artifacts, metrics, daily_by_arm = {"conditional_decisions.parquet": _parquet_bytes(decisions)}, {}, {}
    for arm in ("baseline", "rule", "model"):
        replay = replay_shadow_portfolio(
            rankings=portfolio_rankings, daily=market, benchmark_daily=cash, suspend_rows=suspend,
            trading_calendar=calendar, policy=transition_policy_from_payload(identity.shadow_policy),
            policy_sha256=identity.shadow_policy_sha256, cost_policy=identity.cost_policy,
            request_id=f"{plan.experiment_id}_{arm}", candidate_decision_dates=candidate_dates,
            entry_priorities=None if arm == "baseline" else matched_entry_priorities(decisions, arm=arm),
        )
        # The simulator stops when empty; pad ONLY known zero-cash trailing days
        # to a common horizon, never drop unknown or missing market days.
        observed = replay.daily.set_index("target_trade_date")
        if not observed.index.is_unique:
            _fail("matched portfolio produced duplicate valuation clocks")
        series = observed["net_return_bps"].reindex(common_targets)
        absent = series.isna()
        if absent.any() and (replay.metrics["active_episode_count"] or (absent & (series.index <= observed.index.max())).any()):
            _fail("matched portfolio cannot pad missing active valuations")
        series = series.fillna(0.)
        nav = (1 + series / 10000).cumprod()
        metrics[arm] = {**replay.metrics, "common_day_count": len(series), "common_horizon_return": float(nav.iloc[-1] - 1),
                        "benchmark": "ZERO_RETURN_CASH_NOT_MARKET_INDEX", "remaining_active_positions": replay.metrics["active_episode_count"]}
        daily_by_arm[arm] = series
        artifacts[f"{arm}_daily.parquet"] = _parquet_bytes(replay.daily)
        artifacts[f"{arm}_episodes.parquet"] = _parquet_bytes(replay.episodes)
    matched = pd.DataFrame(daily_by_arm)
    matched["model_minus_baseline_bps"] = matched["model"] - matched["baseline"]
    top5 = decisions.loc[decisions["selection_effective_rank"].le(5)]
    report = {"contract": eval_contract, "evaluation_registration_sha256": eval_parent["stage_sha256"], "metrics": metrics, "test_decision_day_count": len(candidate_dates),
              "rank_context_day_count": len(all_dates), "top20_rows": len(decisions), "top5_rows": len(top5),
              "model_action_counts": top5["model_action"].value_counts().astype(int).to_dict(),
              "rule_action_counts": top5["rule_action"].value_counts().astype(int).to_dict(),
              "model_skip_day_count": int(top5.loc[top5["model_action"].eq("SKIP"), KEY[0]].nunique()),
              "mean_daily_model_minus_baseline_bps": float(matched["model_minus_baseline_bps"].mean()),
              "economic_effectiveness": "EXPLORATORY_NOT_CONFIRMED", "source_evidence": identity.source_evidence,
              "evidence_limitations": list(identity.evidence_limitations), "activation": "NOT_AUTHORIZED"}
    artifacts.update({"matched_daily.parquet": _parquet_bytes(matched.reset_index()), "evaluation.json": _json_bytes(report)})
    path = publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256,
                         parent_sha256=trained["stage_sha256"], artifacts=artifacts)
    _register(plan, root, "EVALUATED", path / "manifest.json", generated=1, evaluated=1)
    return path


def main():
    parser = argparse.ArgumentParser(description="Advisory已消费窗口实际开盘条件价值导航；不生产激活")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--audit-only", action="store_true", help="只读既存评价、追加归因；不重跑或覆盖")
    args = parser.parse_args()
    try:
        operation = audit_published_economic_evaluation if args.audit_only else evaluate_economic_study
        print(json.dumps({"path": str(operation(plan_path=args.plan, output_root=args.output_root)), "stage": "EVALUATION_REVIEW" if args.audit_only else "EVALUATED", "deployable": False}))
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
