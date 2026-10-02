"""Actual-open conditional navigation; never reads future labels for an action."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_aligned_inference import predict_aligned_entry_nodes_v3
from backend.services.advisory_model_first.economic_entry_aligned_pipeline import (
    _sources, load_aligned_study_v3, load_fitted_aligned_study_v3, register_aligned_stage, verify_aligned_stage_ledger,
)
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, POLICY_PRICE_RELATIVE_TOLERANCE
from backend.services.advisory_model_first.economic_entry_evaluation import matched_entry_priorities
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame, _positive
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.policy_contracts import transition_policy_from_payload
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def query_aligned_actual_opens_v3(*, fitted, identity, candidates, features, references, prices):
    identity = EconomicEntryInputIdentityV1.model_validate(identity.model_dump())
    source = fitted.request.source_request
    if source.input_identity_sha256 != identity.identity_sha256:
        _fail("aligned historical source identity differs from model lineage")
    if max(len(candidates), len(features), len(references)) > source.resource_max_rows or len(prices) > 500000:
        _fail("aligned actual query exceeds registered source row budgets")
    roster = _frame(candidates, KEY, set(KEY) | {"selection_effective_rank"}).loc[:, KEY + ["selection_effective_rank"]]
    ranks = pd.to_numeric(roster.selection_effective_rank, errors="coerce")
    if (not ranks.between(1, 20).all() or not ranks.mod(1).eq(0).all()
            or roster.duplicated([KEY[0], "selection_effective_rank"]).any()):
        _fail("aligned actual query has invalid or duplicate Top20 ranks")
    roster["selection_effective_rank"] = ranks.astype(int)
    d_names = [name for name in source.feature_names if name != "query_gap_bps"]
    fields = set(KEY + d_names + ["feature_visible_through", "feature_source_sha256"])
    frozen = _frame(features.loc[:, sorted(fields)], KEY, fields)
    if not frozen.feature_source_sha256.eq(source.feature_source_sha256).all():
        _fail("aligned historical features have a different source")
    if (pd.to_datetime(frozen.feature_visible_through) > frozen[KEY[0]]).any():
        _fail("aligned historical feature clock exceeds D")
    feature_map = frozen.set_index(KEY).to_dict("index")
    refs = _frame(references, KEY, set(KEY) | {"target_reference_raw_cny", "reference_visible_through", "source_sha256"})
    if not refs.source_sha256.eq(identity.reference_source_sha256).all():
        _fail("aligned historical reference source differs")
    reference_map = refs.set_index(KEY).to_dict("index")
    quote_fields = {"trade_date", "instrument", "raw_open_cny", "suspended", "tradability_unknown", "up_limit", "down_limit",
                    "source_sha256", "price_coordinate_sha256"}
    observed = _frame(prices.loc[:, sorted(quote_fields)], ["trade_date", "instrument"], quote_fields)
    if (not observed.source_sha256.eq(identity.price_source_sha256).all()
            or not observed.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()):
        _fail("aligned actual-open source/coordinate identity differs")
    for field in ("suspended", "tradability_unknown"):
        if not observed[field].map(lambda value: isinstance(value, bool)).all():
            _fail("aligned open tradability must be explicit boolean")
    market = observed.set_index(["trade_date", "instrument"]).to_dict("index")
    rows, matrices, positions = [], [], []
    for candidate in roster.to_dict("records"):
        decision, target, symbol = (candidate[name] for name in KEY)
        if not source.test_start <= decision.date() <= source.test_end or not decision < target:
            _fail("aligned actual query lies outside registered test decisions")
        key = decision, target, symbol
        reference, feature, quote = reference_map.get(key), feature_map.get(key), market.get((target, symbol))
        row = {**candidate, "model_action": "UNAVAILABLE", "rule_action": "UNAVAILABLE", "reason_code": "SOURCE_UNAVAILABLE",
               "actual_gap_bps": None, "expected_net_return_bps": None, "entry_net_max_loss_q90_bps": None}
        if reference is None or quote is None:
            rows.append(row)
            continue
        if pd.Timestamp(reference["reference_visible_through"]) != decision:
            _fail("aligned reference is not exactly D-visible")
        if quote["tradability_unknown"]:
            row["reason_code"] = "OPEN_TRADABILITY_UNKNOWN"
            rows.append(row)
            continue
        if quote["suspended"]:
            row.update(model_action="NOT_APPLICABLE", rule_action="NOT_APPLICABLE", reason_code="SUSPENDED")
            rows.append(row)
            continue
        actual, anchor = _positive(quote["raw_open_cny"]), _positive(reference["target_reference_raw_cny"])
        high, low = _positive(quote["up_limit"]), _positive(quote["down_limit"])
        if actual is None or anchor is None or high is None or low is None or low > high or actual >= high - 1e-9 or actual < low - 1e-9:
            row["reason_code"] = "OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"
            rows.append(row)
            continue
        if Decimal(str(actual)) % Decimal(".01"):
            row["reason_code"] = "OBSERVED_OPEN_OFF_EQUITY_TICK"
            rows.append(row)
            continue
        gap = (actual / anchor - 1) * 10000
        row.update(actual_gap_bps=gap, rule_action="TAKE" if -300 <= gap <= 300 else "SKIP")
        if feature is not None:
            positions.append(len(rows))
            matrices.append({**{name: feature[name] for name in d_names}, "query_gap_bps": gap})
        rows.append(row)
    if matrices:
        predicted = predict_aligned_entry_nodes_v3(fitted=fitted, matrix=pd.DataFrame(matrices))
        for position, values in zip(positions, predicted.to_dict("records"), strict=True):
            rows[position].update(values)
    return pd.DataFrame(rows)


def fixed_block_interval_v3(values, *, block_days=5, repetitions=2000, seed=20261002):
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all():
        _fail("aligned matched inference needs a nonempty finite full-day series")
    if (block_days, repetitions, seed) != (5, 2000, 20261002):
        _fail("aligned first candidate has one fixed block inference specification")
    random = np.random.default_rng(seed)
    width = min(block_days, len(values))
    sample_means = np.empty(repetitions)
    for index in range(repetitions):
        starts = random.integers(0, len(values) - width + 1, size=int(np.ceil(len(values) / width)))
        selected = np.concatenate([values[start:start + width] for start in starts])[:len(values)]
        sample_means[index] = selected.mean()
    return {"mean_bps": float(values.mean()), "ci95_bps": [float(value) for value in np.quantile(sample_means, [.025, .975])],
            "block_days": block_days, "repetitions": repetitions, "seed": seed, "interpretation": "EXPLORATORY_NOT_INDEPENDENT_CONFIRMATION"}


def aligned_intervention_attribution_v3(decisions, baseline_episodes, model_episodes):
    if max(len(decisions), len(baseline_episodes), len(model_episodes)) > 100000:
        _fail("aligned attribution exceeds its row budget")
    top5 = decisions.loc[decisions.selection_effective_rank.le(5)]
    keys = [KEY[0], "instrument"]
    if top5.duplicated(keys).any():
        _fail("aligned intervention candidates are not unique")
    actions = top5.loc[:, keys + ["model_action"]].rename(columns={KEY[0]: "entry_signal_date"})
    def bound_episodes(frame):
        if frame.empty:
            return actions.iloc[:0].assign(net_return_bps=pd.Series(dtype=float))
        # At most 100k episodes x 3 narrow action columns; unique D/symbol on
        # the right plus many_to_one prevents Cartesian growth (O(rows)).
        return frame.merge(actions, on=["entry_signal_date", "instrument"], how="left", validate="many_to_one")
    entered, baseline = bound_episodes(model_episodes), bound_episodes(baseline_episodes)
    if not entered.model_action.isin(["TAKE", "UNAVAILABLE", "NOT_APPLICABLE"]).all():
        _fail("aligned model portfolio has an unbound/skipped entry")
    skipped = baseline.loc[baseline.model_action.eq("SKIP")]
    return {"actual_model_take_episodes": int(entered.model_action.eq("TAKE").sum()),
            "research_unknown_control_episodes": int(entered.model_action.isin(["UNAVAILABLE", "NOT_APPLICABLE"]).sum()),
            "baseline_entries_skipped": len(skipped), "skipped_baseline_profitable": int(skipped.net_return_bps.gt(0).sum()),
            "skipped_baseline_losses": int(skipped.net_return_bps.lt(0).sum()),
            "candidate_action_counts": top5.model_action.value_counts().astype(int).to_dict(),
            "model_skip_days": int(top5.loc[top5.model_action.eq("SKIP"), KEY[0]].nunique()),
            "model_take_days": int(top5.loc[top5.model_action.eq("TAKE"), KEY[0]].nunique()),
            "unknown_days": int(top5.loc[top5.model_action.eq("UNAVAILABLE"), KEY[0]].nunique()),
            "episode_analysis_is_not_portfolio_return": True}


def shadow_portfolio_intervention_support_v3(baseline, model, targets):
    """Compare simulated actions/holdings, not merely proposed candidate SKIPs."""
    def state(frame, day):
        if frame.empty:
            return set(), set(), set()
        entered = pd.to_datetime(frame.entry_trade_date)
        exited = pd.to_datetime(frame.exit_trade_date)
        return (set(frame.loc[entered.eq(day), "instrument"]), set(frame.loc[exited.eq(day), "instrument"]),
                set(frame.loc[entered.le(day) & (exited.isna() | exited.gt(day)), "instrument"]))
    entry_days, exit_days, holding_days = [], [], []
    for day in targets:
        original, changed = state(baseline, day), state(model, day)
        for index, collection in enumerate((entry_days, exit_days, holding_days)):
            if original[index] != changed[index]:
                collection.append(pd.Timestamp(day).date().isoformat())
    return {"entry_action_difference_days": entry_days, "exit_action_difference_days": exit_days,
            "holding_composition_difference_days": holding_days, "common_day_count": len(targets),
            "entry_action_difference_day_count": len(entry_days), "holding_difference_day_count": len(holding_days),
            "semantics": "nominal_frozen_shadow_actions_not_real_fill_proof"}


def shadow_endpoint_execution_audit_v3(episodes, prices):
    """Settlement-only restrictions, never an input to a prediction or replay."""
    market = prices.set_index(["trade_date", "instrument"])
    if not market.index.is_unique:
        _fail("aligned settlement price endpoints are not unique")
    limitations = []
    for episode in episodes.to_dict("records"):
        reasons = []
        for side, field in (("ENTRY", "entry"), ("EXIT", "exit")):
            day = episode[f"{field}_trade_date"]
            if pd.isna(day):
                reasons.append(f"{side}_NOT_SETTLED")
                continue
            key = pd.Timestamp(day), episode["instrument"]
            if key not in market.index:
                reasons.append(f"{side}_QUOTE_MISSING")
                continue
            quote = market.loc[key]
            if quote.tradability_unknown or quote.suspended:
                reasons.append(f"{side}_TRADING_STATE_UNPROVEN")
                continue
            raw, factor = _positive(quote.raw_open_cny), _positive(quote.policy_price_per_raw_cny)
            upper, lower, recorded = _positive(quote.up_limit), _positive(quote.down_limit), _positive(episode[f"{field}_price"])
            if None in (raw, factor, upper, lower, recorded):
                reasons.append(f"{side}_PRICE_OR_LIMIT_UNKNOWN")
                continue
            if lower > upper or raw < lower - 1e-9 or raw > upper + 1e-9:
                _fail("aligned settlement endpoint contradicts raw daily limits")
            if not math.isclose(raw * factor, recorded, rel_tol=POLICY_PRICE_RELATIVE_TOLERANCE, abs_tol=1e-8):
                _fail("aligned settlement raw-to-policy endpoint parity failed")
            if Decimal(str(raw)) % Decimal(".01"):
                reasons.append(f"{side}_OPEN_OFF_EQUITY_TICK")
            if (side == "ENTRY" and raw >= upper - 1e-9) or (side == "EXIT" and raw <= lower + 1e-9):
                reasons.append(f"{side}_OPEN_LIMIT_EXECUTION_UNPROVEN")
        if reasons:
            limitations.append({"episode_id": episode["episode_id"], "instrument": episode["instrument"], "reason_codes": reasons})
    return {"episode_count": len(episodes), "restricted_episode_count": len(limitations), "limitations": limitations,
            "semantics": "daily_endpoint_check_not_fill_proof", "nominal_returns_unchanged": True}


def evaluate_aligned_study_v3(*, plan_path: str | Path, output_root: str | Path) -> Path:
    plan, root, registered = load_aligned_study_v3(plan_path=plan_path, output_root=output_root)
    source_v2, parent_plan, parent_root, _, risk_labels, _, _, _ = _sources(plan.v2_plan_ref, plan.v2_prepared_manifest_ref)
    trained = read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    verify_aligned_stage_ledger(plan, root, "TRAINED", root / "trained/manifest.json")
    if (root / "evaluated").exists():
        read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        register_aligned_stage(plan, parent_plan, root, "EVALUATED", root / "evaluated/manifest.json", generated=1, evaluated=1)
        return root / "evaluated"
    fitted = load_fitted_aligned_study_v3(plan_path=plan_path, output_root=output_root)
    identity = EconomicEntryInputIdentityV1.model_validate_json((parent_root / "prepared/identity.json").read_text(encoding="utf-8"))
    rankings = pd.read_parquet(parent_root / "prepared/frozen_rankings.parquet")
    source = plan.training_request.source_request
    candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20) &
                              pd.to_datetime(rankings[KEY[0]]).between(pd.Timestamp(source.test_start), pd.Timestamp(source.test_end))].copy()
    prices = pd.read_parquet(parent_root / "prepared/prices.parquet")
    decisions = query_aligned_actual_opens_v3(fitted=fitted, identity=identity, candidates=candidates,
                                            features=pd.read_parquet(parent_root / "prepared/features.parquet"),
                                            references=pd.read_parquet(parent_root / "prepared/references.parquet"), prices=prices)
    market = prices.rename(columns={"trade_date": "datetime"}).copy()
    for field in ("open", "high", "low", "close"):
        market[field] = market[f"raw_{field}_cny"] * market.policy_price_per_raw_cny
    market["factor"], market["limit_up"], market["limit_down"] = market.policy_price_per_raw_cny, 1., 1.
    market["up_limit_price"], market["down_limit_price"] = market.up_limit, market.down_limit
    market = market.set_index(["datetime", "instrument"]).sort_index()
    calendar = pd.DatetimeIndex(json.loads((parent_root / "prepared/calendar.json").read_text(encoding="utf-8")))
    cash = pd.DataFrame({"datetime": calendar, "open": 1.}).set_index("datetime")
    suspend = prices.loc[prices.suspended, ["trade_date", "instrument"]]
    rankings = rankings.loc[pd.to_datetime(rankings[KEY[0]]).ge(pd.Timestamp(source.test_start))]
    targets = pd.DatetimeIndex(rankings[KEY[1]].unique()).sort_values()
    candidate_dates = sorted(pd.to_datetime(candidates[KEY[0]]).unique())
    artifacts, metrics, daily, episodes = {"conditional_decisions.parquet": _parquet_bytes(decisions)}, {}, {}, {}
    for arm in ("baseline", "rule", "model"):
        replay = replay_shadow_portfolio(rankings=rankings, daily=market, benchmark_daily=cash, suspend_rows=suspend,
                                         trading_calendar=calendar, policy=transition_policy_from_payload(identity.shadow_policy),
                                         policy_sha256=identity.shadow_policy_sha256, cost_policy=identity.cost_policy,
                                         request_id=f"{plan.experiment_id}_{arm}", candidate_decision_dates=candidate_dates,
                                         entry_priorities=None if arm == "baseline" else matched_entry_priorities(decisions, arm=arm))
        observed = replay.daily.set_index("target_trade_date")
        if not observed.index.is_unique:
            _fail("aligned matched portfolio has duplicate valuation days")
        series = observed.net_return_bps.reindex(targets)
        absent = series.isna()
        if absent.any() and (replay.metrics["active_episode_count"] or (absent & (series.index <= observed.index.max())).any()):
            _fail("aligned portfolio cannot pad absent active valuations")
        series = series.fillna(0.)
        wealth = pd.Series(np.r_[1., (1 + series / 10000).cumprod().to_numpy()])
        metrics[arm] = {**replay.metrics, "common_day_count": len(series), "common_horizon_return": float(wealth.iloc[-1] - 1),
                        "common_horizon_max_drawdown": float((wealth / wealth.cummax() - 1).min()),
                        "benchmark": "ZERO_RETURN_CASH_NOT_MARKET_INDEX"}
        daily[arm], episodes[arm] = series, replay.episodes
        artifacts[f"{arm}_daily.parquet"], artifacts[f"{arm}_episodes.parquet"] = _parquet_bytes(replay.daily), _parquet_bytes(replay.episodes)
    matched = pd.DataFrame(daily)
    matched["model_minus_baseline_bps"] = matched.model - matched.baseline
    attribution = aligned_intervention_attribution_v3(decisions, episodes["baseline"], episodes["model"])
    unavailable = [value for value in risk_labels if value.original.decision_date >= source.test_start and value.status == "UNAVAILABLE"]
    report = {"plan_sha256": plan.plan_sha256, "training_stage_sha256": trained["stage_sha256"], "metrics": metrics,
              "test_decision_days": len(candidate_dates), "top20_rows": len(decisions), "top5_rows": int(decisions.selection_effective_rank.le(5).sum()),
              "attribution": attribution, "paired_daily_lift": fixed_block_interval_v3(matched.model_minus_baseline_bps),
              "actual_shadow_intervention_support": shadow_portfolio_intervention_support_v3(episodes["baseline"], episodes["model"], targets),
              "portfolio_endpoint_execution_audits": {arm: shadow_endpoint_execution_audit_v3(frame, prices) for arm, frame in episodes.items()},
              "test_original_episode_execution_unknown": len(unavailable), "shadow_returns_are_not_execution_proof": True,
              "source_evidence": identity.source_evidence, "evidence_limitations": list(identity.evidence_limitations),
              "original_v2_study": source_v2.experiment_id, "daily_grid_delivery_verified": False,
              "economic_effectiveness": "EXPLORATORY_NOT_CONFIRMED", "decision_use": "NAVIGATION_ONLY", "deployable": False}
    artifacts.update({"matched_daily.parquet": _parquet_bytes(matched.reset_index()), "evaluation.json": _json_bytes(report),
                      "execution_limitations.json": _json_bytes([value.model_dump(mode="json") for value in unavailable])})
    published = publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"], artifacts=artifacts)
    register_aligned_stage(plan, parent_plan, root, "EVALUATED", published / "manifest.json", generated=1, evaluated=1)
    return published
